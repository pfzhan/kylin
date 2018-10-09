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

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;

public class NCuboidAggregator {
    protected static final Logger logger = LoggerFactory.getLogger(NCuboidAggregator.class);
    private SparkSession ss;
    private Dataset<Row> dataSet;
    private Set<Integer> dimensions;
    private Map<Integer, NDataModel.Measure> measures;

    public NCuboidAggregator(SparkSession ss, Dataset<Row> dataSet, Set<Integer> dimensions,
            Map<Integer, NDataModel.Measure> measures) {
        this.ss = ss;
        this.dataSet = dataSet;
        this.dimensions = dimensions;
        this.measures = measures;
    }

    public Dataset<Row> aggregate() {
        CuboidAggregateUdf udf = new CuboidAggregateUdf(measures.values());
        //TODO: when concurrent, udf conflict?
        ss.udf().register(NBatchConstants.P_CUBOID_AGG_UDF, udf);
        Dataset<Row> afterAgg = dataSet.groupBy(NSparkCubingUtil.getColumns(dimensions))
                .agg(callUDF(NBatchConstants.P_CUBOID_AGG_UDF, NSparkCubingUtil.getColumns(measures.keySet())));

        return new Mapper().doMap(afterAgg, dimensions, measures.keySet());
    }

    public static class Mapper implements Serializable {
        public Dataset<Row> doMap(Dataset<Row> input, final Set<Integer> dims, final Set<Integer> meas) {
            final int dimSize = dims.size();
            final int meaSize = meas.size();
            StructType schema = new StructType();
            for (Integer index : dims) {
                schema = schema.add(String.valueOf(index), DataTypes.BinaryType, false);
            }
            for (Integer index : meas) {
                schema = schema.add(String.valueOf(index), DataTypes.BinaryType, false);
            }

            Dataset<Row> afterMap = input.map(new MapFunction<Row, Row>() {
                @Override
                public Row call(Row value) throws Exception {
                    Object[] ret = new Object[dimSize + meaSize];
                    int i = 0;
                    for (; i < dimSize; i++) {
                        ret[i] = value.get(i);
                    }
                    if (value.get(i) instanceof GenericRowWithSchema) {
                        Row meaRow = ((GenericRowWithSchema) value.get(i));
                        for (int j = 0; j < meaSize; i++, j++) {
                            ret[i] = meaRow.get(j);
                        }
                    } else {
                        for (; i < meaSize; i++)
                            ret[i] = value.get(i);
                    }
                    return RowFactory.create(ret);
                }
            }, RowEncoder.apply(schema));
            return afterMap;
        }
    }

    public static class CuboidAggregateUdf extends UserDefinedAggregateFunction {

        //TODO: what is the optimal default size?
        private static final int DEFAULT_BUFFER_SIZE = 1024;
        private int measureNum;
        private MeasureAggregators aggregators;
        private boolean[] needAggs;
        private MeasureCodec measureCodec;

        private StructType inputSchema;
        private StructType bufferSchema;
        private DataType returnDataType;

        public CuboidAggregateUdf(Collection<NDataModel.Measure> measureDescSet) {
            init(measureDescSet);
        }

        private void init(Collection<NDataModel.Measure> measureDescSet) {
            List<MeasureDesc> measures = Lists.newArrayListWithExpectedSize(measureDescSet.size());
            measures.addAll(measureDescSet);
            aggregators = new MeasureAggregators(measures);
            measureNum = measureDescSet.size();
            needAggs = new boolean[measureNum];
            measureCodec = new MeasureCodec(measures);

            for (int i = 0; i < measureNum; i++) {
                needAggs[i] = true;
            }
            List<StructField> inputFields = new ArrayList<>();
            List<StructField> bufferFields = new ArrayList<>();
            List<StructField> returnFields = new ArrayList<>();
            for (MeasureDesc md : measureDescSet) {
                StructField inputStructField = DataTypes.createStructField("input_" + md.getName(),
                        DataTypes.BinaryType, true);
                StructField bufferStructField = DataTypes.createStructField("buffer_" + md.getName(),
                        DataTypes.BinaryType, true);
                StructField returnedStructField = DataTypes.createStructField("return_" + md.getName(),
                        DataTypes.BinaryType, true);
                inputFields.add(inputStructField);
                bufferFields.add(bufferStructField);
                returnFields.add(returnedStructField);
            }
            inputSchema = DataTypes.createStructType(inputFields);
            bufferSchema = DataTypes.createStructType(bufferFields);
            returnDataType = DataTypes.createStructType(returnFields);
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
                    if (estimateSize >= (1 << 20))
                        logger.info("Buffer size {} cannot hold the filter, resizing to 2 times", estimateSize);

                    if (estimateSize == (1 << 30))
                        throw e;
                    estimateSize = estimateSize << 1;
                }
            }
        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        @Override
        public DataType dataType() {
            return returnDataType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer mutableAggregationBuffer) {
            for (int i = 0; i < measureNum; i++) {
                mutableAggregationBuffer.update(i, null);
            }
        }

        @Override
        public void update(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
            doAggregate(mutableAggregationBuffer, row);
        }

        @Override
        public void merge(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
            doAggregate(mutableAggregationBuffer, row);
        }

        @Override
        public Object evaluate(Row row) {
            return row;
        }

        private void doAggregate(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
            Object[] curObj = new Object[measureNum];
            Object[] lastObj = new Object[measureNum];
            Object[] outObj = new Object[measureNum];
            int first = 0;
            for (int i = 0; i < measureNum; i++) {
                if (mutableAggregationBuffer.get(i) == null) {
                    mutableAggregationBuffer.update(i, row.get(i));
                    first++;
                } else {
                    ByteBuffer buffer = ByteBuffer.wrap((byte[]) row.get(i));
                    curObj[i] = measureCodec.decode(i, buffer);
                    buffer = ByteBuffer.wrap((byte[]) mutableAggregationBuffer.get(i));
                    lastObj[i] = measureCodec.decode(i, buffer);
                }
            }
            if (first == measureNum)
                return;
            aggregators.aggregate(curObj, lastObj, outObj);
            for (int i = 0; i < measureNum; i++) {
                mutableAggregationBuffer.update(i, encodeMeasure(i, outObj[i]));
            }
        }
    }
}
