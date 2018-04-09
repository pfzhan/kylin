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

package io.kyligence.kap.engine.spark.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.storage.format.NParquetCuboidInputFormat;
import io.kyligence.kap.engine.spark.storage.format.NParquetCuboidOutputFormat;
import io.kyligence.kap.metadata.model.NDataModel;
import scala.Serializable;
import scala.Tuple2;

@SuppressWarnings("serial")
public class NParquetStorage implements NSparkCubingEngine.NSparkCubingStorage, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(NParquetStorage.class);

    private String getCuboidStoragePath(NDataCuboid cuboidInstance) {
        return NSparkCubingUtil.getStoragePath(cuboidInstance);
    }

    @Override
    public Dataset<Row> getCuboidData(NDataCuboid cuboid, SparkSession ss) {
        final NCuboidLayout layout = cuboid.getCuboidLayout();
        final ImmutableBiMap<Integer, TblColRef> orderedDimensions = layout.getOrderedDimensions();
        final ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures = layout.getOrderedMeasures();
        final IDimensionEncodingMap dimEncMap = new NCubeDimEncMap(cuboid.getSegDetails().getDataSegment());
        final RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(dimEncMap);
        final MeasureCodec measureCodec = new MeasureCodec(orderedMeasures.values().toArray(new MeasureDesc[0]));

        StructType schema = new StructType();
        for (Map.Entry<Integer, TblColRef> dimEntry : orderedDimensions.entrySet()) {
            schema = schema.add(String.valueOf(dimEntry.getKey()), DataTypes.BinaryType);
        }

        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : orderedMeasures.entrySet()) {
            schema = schema.add(String.valueOf(measureEntry.getKey()), DataTypes.BinaryType);
        }

        final int[] rowKeyColumnLens = new int[orderedDimensions.size()];
        int i = 0;
        for (TblColRef tblColRef : orderedDimensions.values()) {
            int length = rowKeyColumnIO.getColumnLength(tblColRef);
            rowKeyColumnLens[i] = length;
            i++;
        }

        String path = getCuboidStoragePath(cuboid);
        logger.debug("Get Cuboid Dataset from path: {}", path);

        @SuppressWarnings("resource")
        JavaSparkContext ctx = new JavaSparkContext(ss.sparkContext());

        Configuration jobConf = new Configuration(ctx.hadoopConfiguration());
        jobConf.set(BatchConstants.CFG_CUBE_NAME, cuboid.getSegDetails().getDataflowName());
        jobConf.set(BatchConstants.CFG_CUBE_SEGMENT_ID, Integer.toString(cuboid.getSegDetails().getSegmentId()));
        jobConf.set(BatchConstants.CFG_PROJECT_NAME, cuboid.getSegDetails().getDataflow().getProject());
        JavaPairRDD<Text, Text> batchRDD = ctx.newAPIHadoopFile(path, NParquetCuboidInputFormat.class, Text.class,
                Text.class, jobConf);

        //TODO: serialize RowKeyColumnIO will serialize NCubeDimEncMap -> NCubePlan -> NCuboidDesc -> NRuleBasedCuboidsDesc -> NCuboidScheduler, too many?
        JavaRDD<Row> rows = batchRDD.map(new org.apache.spark.api.java.function.Function<Tuple2<Text, Text>, Row>() {
            private volatile transient boolean initialized = false;

            @Override
            public Row call(Tuple2<Text, Text> tuple) throws Exception {
                Object[] cells = new Object[orderedDimensions.size() + orderedMeasures.size()];

                byte[] values = tuple._1.getBytes();

                int i = 0;
                int offset = 0;
                for (int l : rowKeyColumnLens) {
                    cells[i++] = ByteArray.copyOf(values, offset, l).array();
                    offset += l;
                }

                ByteBuffer measureBuf = ByteBuffer.wrap(values);
                measureBuf.position(offset);
                int[] lens = measureCodec.getPeekLength(measureBuf);
                for (int l : lens) {
                    cells[i++] = ByteArray.copyOf(values, offset, l).array();
                    offset += l;
                }
                return RowFactory.create(cells);
            }
        });
        return ss.createDataFrame(rows, schema);
    }

    @Override
    public void saveCuboidData(NDataCuboid cuboid, Dataset<Row> data, SparkSession ss) {
        final NCuboidLayout layout = cuboid.getCuboidLayout();
        final NCuboidDesc cuboidDesc = layout.getCuboidDesc();

        final int columns = data.schema().length();
        boolean expression = columns == cuboidDesc.getEffectiveDimCols().size()
                + cuboidDesc.getOrderedMeasures().size();
        Preconditions.checkArgument(expression, "Dataset schema not match with cuboid layout.");

        final Map<Integer, TblColRef> dimensions = Maps.newLinkedHashMap(layout.getOrderedDimensions());
        final Map<Integer, NDataModel.Measure> measures = Maps.newLinkedHashMap(cuboidDesc.getOrderedMeasures());
        final IDimensionEncodingMap dimEncMap = new NCubeDimEncMap(cuboid.getSegDetails().getDataSegment());
        final RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(dimEncMap);
        final MeasureCodec measureCodec = new MeasureCodec(measures.values().toArray(new MeasureDesc[0]));

        int dimLen = 0;
        for (TblColRef dimColRef : dimensions.values()) {
            dimLen += rowKeyColumnIO.getColumnLength(dimColRef);
        }
        int measureMaxLen = 0;
        int[] measureMaxLens = measureCodec.getMaxLength();
        for (int m : measureMaxLens) {
            measureMaxLen += m;
        }

        final int dimBufSize = dimLen;
        final int measureBufSize = measureMaxLen;

        Configuration jobConf = new Configuration(ss.sparkContext().hadoopConfiguration());
        String path = getCuboidStoragePath(cuboid);
        try {
            HadoopUtil.deletePath(jobConf, new Path(path));
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't delete hdfs path: " + path);
        }
        logger.debug("Write Cuboid Dataset to path: {}", path);
        jobConf.set(BatchConstants.CFG_CUBE_NAME, cuboid.getSegDetails().getDataflowName());
        jobConf.set(BatchConstants.CFG_CUBE_SEGMENT_ID, Integer.toString(cuboid.getSegDetails().getSegmentId()));
        jobConf.set(BatchConstants.KYLIN_CUBOID_LAYOUT_ID, Long.toString(cuboid.getCuboidLayoutId()));
        jobConf.set(BatchConstants.CFG_PROJECT_NAME, cuboid.getSegDetails().getDataflow().getProject());
        jobConf.set(NBatchConstants.P_DIST_META_URL, cuboid.getConfig().getMetadataUrl().toString());

        data.javaRDD().mapToPair(new PairFunction<Row, Text, Text>() {
            byte[] buffer = new byte[dimBufSize + measureBufSize];

            @Override
            public Tuple2<Text, Text> call(Row row) throws Exception {
                int offset = 0;
                int i = 0;
                for (; i < dimensions.size(); i++) {
                    offset = addBytes(buffer, (byte[]) row.get(i), offset);
                }

                for (; i < measures.size() + dimensions.size(); i++) {
                    offset = addBytes(buffer, (byte[]) row.get(i), offset);
                }

                byte[] actualBuf = new byte[offset];
                System.arraycopy(buffer, 0, actualBuf, 0, offset);
                return Tuple2.apply(new Text(actualBuf), new Text());
            }

            private int addBytes(byte[] source, byte[] target, int offset) {
                int remain = source.length - offset;
                if (remain < target.length)
                    throw new IllegalStateException(
                            "Overflow exception: available bytes: " + remain + ", expect bytes: " + target.length);
                for (int i = 0; i < target.length; i++) {
                    source[i + offset] = target[i];
                }
                return offset + target.length;
            }

        }).saveAsNewAPIHadoopFile(path, Text.class, Text.class, NParquetCuboidOutputFormat.class, jobConf);
    }
}
