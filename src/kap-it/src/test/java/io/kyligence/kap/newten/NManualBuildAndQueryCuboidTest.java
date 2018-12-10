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
package io.kyligence.kap.newten;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.CuboidAggregator;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.common.SparderQueryTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NManualBuildAndQueryCuboidTest extends NManualBuildAndQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    private static final String DEFAULT_PROJECT = "default";

    private static StructType OUT_SCHEMA = null;

    @Before
    public void setup() throws Exception {
        super.init();
        System.setProperty("spark.local", "true");
        System.setProperty("noBuild", "false");
        System.setProperty("isDeveloperMode", "false");
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();

        System.clearProperty("noBuild");
        System.clearProperty("isDeveloperMode");
        System.clearProperty("spark.local");
    }

    @Override
    public String getProject() {
        return DEFAULT_PROJECT;
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        buildCubes();
        compareCuboidParquetWithSparkSql("ncube_basic");
        compareCuboidParquetWithSparkSql("ncube_basic_inner");
    }

    private void compareCuboidParquetWithSparkSql(String dfName) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        List<NDataCuboid> nDataCuboids = Lists.newArrayList();
        NDataflow df = dsMgr.getDataflow(dfName);
        for (NDataSegment segment : df.getSegments()) {
            nDataCuboids.addAll(segment.getSegDetails().getCuboids());
        }
        for (NDataCuboid cuboid : nDataCuboids) {
            Set<Integer> rowKeys = cuboid.getCuboidLayout().getOrderedDimensions().keySet();

            Dataset<Row> layoutDataset = StorageFactory.createEngineAdapter(cuboid.getCuboidLayout(), NSparkCubingEngine.NSparkCubingStorage.class).getFrom(NSparkCubingUtil.getStoragePath(cuboid), ss);
            layoutDataset = layoutDataset.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid))).sort(NSparkCubingUtil.getColumns(rowKeys));
            System.out.println("Query cuboid ------------ " + cuboid.getCuboidLayoutId());
            layoutDataset = dsConvertToOriginal(layoutDataset, cuboid.getCuboidLayout());
            layoutDataset.show(10);

            NDataSegment segment = cuboid.getSegDetails().getDataSegment();
            Dataset<Row> ds = initFlatTable(dfName, new SegmentRange.TimePartitionedSegmentRange(segment.getTSRange().getStart(), segment.getTSRange().getEnd()));

            if (cuboid.getCuboidLayout().getCuboidDesc().getId() < NCuboidDesc.TABLE_INDEX_START_ID) {
                ds = queryCuboidLayout(cuboid.getCuboidLayout(), ds);
            }

            Dataset<Row> exceptDs = ds.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid))).sort(NSparkCubingUtil.getColumns(rowKeys));

            System.out.println("Spark sql ------------ ");
            exceptDs.show(10);

            Assert.assertEquals(layoutDataset.count(), exceptDs.count());
            String msg = SparderQueryTest.checkAnswer(layoutDataset, exceptDs);
            Assert.assertNull(msg);
        }
    }

    private Set<Integer> chooseMeas(NDataCuboid cuboid) {
        Set<Integer> meaSet = Sets.newHashSet();
        for (Map.Entry<Integer, NDataModel.Measure> entry : cuboid.getCuboidLayout().getOrderedMeasures().entrySet()) {
            String funName = entry.getValue().getFunction().getReturnDataType().getName();
            if (funName.equals("hllc") || funName.equals("topn") || funName.equals("percentile")) {
                continue;
            }
            meaSet.add(entry.getKey());
        }
        return meaSet;
    }

    private Dataset<Row> queryCuboidLayout(NCuboidLayout layout, Dataset<Row> ds) {
        NCubeJoinedFlatTableDesc flatTableDesc = new NCubeJoinedFlatTableDesc(layout.getCuboidDesc().getCubePlan());
        return CuboidAggregator.agg(ss, ds, layout.getCuboidDesc().getEffectiveDimCols().keySet(), layout.getCuboidDesc().getCubePlan().getEffectiveMeasures(), flatTableDesc, true);
    }

    private Dataset<Row> dsConvertToOriginal(Dataset<Row> layoutDs, NCuboidLayout layout) {
        ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures = layout.getOrderedMeasures();

        for (final Map.Entry<Integer, NDataModel.Measure> entry : orderedMeasures.entrySet()) {
            MeasureDesc measureDesc = entry.getValue();
            if (measureDesc != null) {
                final String[] columns = layoutDs.columns();
                String function = measureDesc.getFunction().getReturnDataType().getName();

                if ("bitmap".equals(function)) {
                    final int finalIndex = convertOutSchema(layoutDs, entry.getKey().toString(), DataTypes.LongType);
                    layoutDs = layoutDs.map((MapFunction<Row, Row>) value -> {
                        Object[] ret = new Object[value.size()];
                        for (int i = 0; i < columns.length; i++) {
                            if (i == finalIndex) {
                                BitmapSerializer serializer = new BitmapSerializer(DataType.ANY);
                                byte[] bytes = (byte[]) value.get(i);
                                ByteBuffer buf = ByteBuffer.wrap(bytes);
                                BitmapCounter bitmapCounter = serializer.deserialize(buf);
                                ret[i] = bitmapCounter.getCount();
                            } else {
                                ret[i] = value.get(i);
                            }
                        }
                        return RowFactory.create(ret);
                    }, RowEncoder.apply(OUT_SCHEMA));
                }
            }
        }
        return layoutDs;
    }

    private Integer convertOutSchema(Dataset<Row> layoutDs, String fieldName, org.apache.spark.sql.types.DataType dataType) {
        StructField[] structFieldList = layoutDs.schema().fields();
        String[] columns = layoutDs.columns();

        int index = 0;
        StructField[] outStructFieldList = new StructField[structFieldList.length];
        for (int i = 0; i < structFieldList.length; i++) {
            if (columns[i].equalsIgnoreCase(fieldName)) {
                index = i;
                StructField structField = structFieldList[i];
                outStructFieldList[i] = new StructField(structField.name(), dataType, false, structField.metadata());
            } else {
                outStructFieldList[i] = structFieldList[i];
            }
        }

        OUT_SCHEMA = new StructType(outStructFieldList);

        return index;
    }

    private Dataset<Row> initFlatTable(String dfName, SegmentRange segmentRange) {
        System.out.println(getTestConfig().getMetadataUrl());
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataModel model = df.getModel();

        NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(df.getCubePlan(), segmentRange);
        Dataset<Row> ds = NJoinedFlatTable.generateDataset(flatTable, ss);

        StructType schema = ds.schema();
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(model.getColumnNameByColumnId(Integer.valueOf(field.name()))));
        }
        return ds;
    }
}