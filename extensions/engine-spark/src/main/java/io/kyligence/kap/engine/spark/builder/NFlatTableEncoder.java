/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.builder;

import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.cube.model.NDataSegment;
import scala.Tuple2;

@SuppressWarnings("serial")
public class NFlatTableEncoder implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(NFlatTableEncoder.class);

    private Dataset<Row> dataset;
    private NDataSegment seg;
    private KylinConfig config;
    private SparkSession ss;

    public NFlatTableEncoder(Dataset<Row> dataset, NDataSegment seg, KylinConfig config, SparkSession ss) {
        this.dataset = dataset;
        this.seg = seg;
        this.config = config;
        this.ss = ss;
    }

    public Dataset<Row> encode() {
        final Set<TblColRef> globalDictCols = extractGlobalDictColumns();
        // process global dictionary
        if (globalDictCols.size() > 0) {
            int partitions = config.getAppendDictHashPartitions();
            final NCubeJoinedFlatTableDesc flatTableDesc = new NCubeJoinedFlatTableDesc(seg.getCubePlan(),
                    seg.getSegRange());
            JavaRDD<Row> globalDictRdd = dataset.toJavaRDD();
            for (final TblColRef ref : globalDictCols) {
                String dictBuildClz = seg.getCubePlan().getDictionaryBuilderClass(ref);

                if (NDictionaryBuilder.isUsingGlobalDict2(dictBuildClz) == false)
                    continue;

                final int columnIndex = flatTableDesc.getColumnIndex(ref);
                globalDictRdd = globalDictRdd.mapToPair(new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(columnIndex), row);
                    }
                }).partitionBy(new NHashPartitioner(partitions)).values().map(new Function<Row, Row>() {
                    private RowEncoder encoder;
                    private volatile transient boolean initialized = false;

                    @Override
                    public Row call(Row row) throws Exception {
                        if (initialized == false) {
                            synchronized (EncodeFunction.class) {
                                KylinConfig.setKylinConfigThreadLocal(config);
                                encoder = new RowEncoder(seg);
                                initialized = true;
                            }
                        }
                        String original = row.getString(columnIndex);
                        int id = encoder.getGlobalDictId(ref, original);
                        Object[] objects = new Object[row.size()];
                        for (int i = 0; i < row.size(); i++) {
                            if (i == columnIndex)
                                objects[i] = id == -1 ? null : String.valueOf(id);
                            else
                                objects[i] = row.get(i);
                        }
                        return RowFactory.create(objects);
                    }
                });
            }
            dataset = ss.createDataFrame(globalDictRdd, dataset.schema());
        }

        Set<Integer> dimIndexes = seg.getCubePlan().getEffectiveDimCols().keySet();
        Set<Integer> meaIndexes = seg.getCubePlan().getEffectiveMeasures().keySet();
        StructType schema = new StructType();
        for (Integer index : dimIndexes) {
            schema = schema.add(String.valueOf(index), DataTypes.BinaryType, false);
        }
        for (Integer index : meaIndexes) {
            schema = schema.add(String.valueOf(index), DataTypes.BinaryType, false);
        }
        return dataset.map(new EncodeFunction(seg, config),
                org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(schema));
    }

    private Set<TblColRef> extractGlobalDictColumns() {
        Set<TblColRef> globalDictColumns = new HashSet<>();
        for (MeasureDesc measure : seg.getDataflow().getMeasures()) {
            TblColRef ref = NDictionaryBuilder.needGlobalDictionary(measure);
            if (ref != null)
                globalDictColumns.add(ref);
        }
        return globalDictColumns;
    }

    static public class EncodeFunction implements MapFunction<Row, Row> {
        private NDataSegment seg;
        private RowEncoder encoder;
        private KylinConfig config;
        private volatile transient boolean initialized = false;

        public EncodeFunction(NDataSegment seg, KylinConfig config) {
            this.seg = seg;
            this.config = config;
        }

        @Override
        public Row call(Row row) throws Exception {
            if (initialized == false) {
                synchronized (EncodeFunction.class) {
                    KylinConfig.setKylinConfigThreadLocal(config);
                    encoder = new RowEncoder(seg);
                    initialized = true;
                }
            }
            encoder.resetAggrs();
            return encoder.encode(row);
        }
    }

    public static class RowEncoder {

        private static final Logger logger = LoggerFactory.getLogger(RowEncoder.class);
        private static final int DEFAULT_BUFFER_SIZE = 256;
        private NDataSegment nDataSegment;
        private RowKeyColumnIO colIO;
        private MeasureCodec measureCodec;
        private MeasureIngester<?>[] aggrIngesters;
        private Map<TblColRef, Dictionary<String>> dictionaryMap;
        private NCubeJoinedFlatTableDesc flatTableDesc;
        private final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
        private BitmapCounter bitmapCounter = factory.newBitmap();

        public RowEncoder(NDataSegment nDataSegment) {
            this.nDataSegment = nDataSegment;
            this.measureCodec = new MeasureCodec(nDataSegment.getDataflow().getMeasures());
            this.flatTableDesc = new NCubeJoinedFlatTableDesc(nDataSegment.getCubePlan(), nDataSegment.getSegRange());
            IDimensionEncodingMap dimEncoding = new NCubeDimEncMap(nDataSegment);
            for (TblColRef colRef : nDataSegment.getCubePlan().getEffectiveDimCols().values()) {
                dimEncoding.get(colRef);
            }
            this.colIO = new RowKeyColumnIO(dimEncoding);
            aggrIngesters = MeasureIngester.create(nDataSegment.getDataflow().getMeasures());
            dictionaryMap = nDataSegment.buildDictionaryMap();
        }

        public int getGlobalDictId(TblColRef col, String value) {
            return dictionaryMap.get(col).getIdFromValue(value);
        }

        private byte[] encodeMeasure(int index, Object obj) {
            int estimateSize = DEFAULT_BUFFER_SIZE;
            while (true) {
                try {
                    ByteBuffer buf = ByteBuffer.allocate(estimateSize);
                    buf.clear();
                    measureCodec.encode(index, obj, buf);
                    byte[] ret = new byte[buf.position()];
                    System.arraycopy(buf.array(), 0, ret, 0, buf.position());
                    return ret;
                } catch (BufferOverflowException e) {
                    logger.info("Buffer size {} cannot hold the filter, resizing to 2 times", estimateSize);
                    if (estimateSize == (1 << 30))
                        throw e;
                    estimateSize = estimateSize << 1;
                }
            }
        }

        public Row encode(Row row) {
            int measuresSize = nDataSegment.getDataflow().getMeasures().size();
            List<Object> output = new ArrayList<>();

            for (TblColRef tblColRef : nDataSegment.getCubePlan().getEffectiveDimCols().values()) {
                int len = colIO.getColumnLength(tblColRef);
                byte[] col = new byte[len];
                colIO.writeColumn(tblColRef, row.get(flatTableDesc.getColumnIndex(tblColRef)).toString(), 0,
                        DimensionEncoding.NULL, col, 0);
                output.add(col);
            }

            for (int i = 0; i < measuresSize; i++) {
                output.add(encodeMeasure(i, buildValueOf(i, row)));
            }
            return RowFactory.create(output.toArray(new Object[0]));
        }

        private Object buildValueOf(int idxOfMeasure, Row row) {
            MeasureDesc measure = nDataSegment.getDataflow().getMeasures().get(idxOfMeasure);
            FunctionDesc function = measure.getFunction();
            int paramCount = function.getParameterCount();
            String[] inputToMeasure = new String[paramCount];
            // pick up parameter values
            ParameterDesc param = function.getParameter();
            for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
                String value;
                if (function.isCount()) {
                    value = "1";
                } else if (param.isColumnType()) {
                    int index = flatTableDesc.getColumnIndex(param.getColRef());
                    value = row.get(index).toString();
                } else {
                    value = param.getValue();
                }
                inputToMeasure[i] = value;
            }
            TblColRef tblColRef = NDictionaryBuilder.needGlobalDictionary(measure);
            if (tblColRef != null) {
                String dictBuildClz = nDataSegment.getCubePlan().getDictionaryBuilderClass(tblColRef);

                if (NDictionaryBuilder.isUsingGlobalDict2(dictBuildClz)) {
                    bitmapCounter.clear();
                    if (inputToMeasure[0] == null)
                        return bitmapCounter;
                    bitmapCounter.add(Integer.parseInt(inputToMeasure[0]));
                    return bitmapCounter;
                }
            }
            return aggrIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
        }

        public void resetAggrs() {
            for (int i = 0; i < nDataSegment.getDataflow().getMeasures().size(); i++) {
                aggrIngesters[i].reset();
            }
        }
    }
}
