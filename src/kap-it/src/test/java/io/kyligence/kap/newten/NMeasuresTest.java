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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COLLECT_SET;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT_DISTINCT;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MAX;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MIN;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_PERCENTILE;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_SUM;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_TOP_N;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import io.kyligence.kap.util.ExecAndComp;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.measure.percentile.PercentileSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.engine.spark.job.UdfManager;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import scala.collection.mutable.WrappedArray;

public class NMeasuresTest extends NLocalWithSparkSessionTest {

    private static final String DF_NAME = "cb596712-3a09-46f8-aea1-988b43fe9b6c";

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testTopnMeasure() throws Exception {
        //validate Cube Data by decode
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        buildCuboid(generateTopnMeaLists());

        //build is done, validate Layout data

        populateSSWithCSVData(config, "default", SparderEnv.getSparkSession());
        SchemaProcessor.checkSchema(ss, DF_NAME, getProject());

        NDataSegment seg = NDataflowManager.getInstance(config, getProject()).getDataflow(DF_NAME)
                .getLatestReadySegment();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), 1);
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(seg, dataCuboid.getLayoutId()), ss);
        for (Row row : ret.collectAsList()) {
            if (row.apply(0).toString().equals("10000000157")) {
                WrappedArray topnArray = (WrappedArray) row.apply(6);
                Assert.assertEquals("[1.0000000157E10,['ATOM']]", topnArray.apply(0).toString());// TOP_N(ID1)
                Assert.assertEquals("[1.0000000157E10,['FT']]", topnArray.apply(1).toString());// TOP_N(ID1)
                topnArray = (WrappedArray) row.apply(7);
                Assert.assertEquals("[1.32342342E8,['ATOM']]", topnArray.apply(0).toString());// TOP_N(ID2)
                Assert.assertEquals("[1.32322342E8,['FT']]", topnArray.apply(1).toString());// TOP_N(ID2)
                topnArray = (WrappedArray) row.apply(8);
                Assert.assertEquals("[124123.0,['ATOM']]", topnArray.apply(0).toString());// TOP_N(ID3)
                Assert.assertEquals("[14123.0,['FT']]", topnArray.apply(1).toString());// TOP_N(ID3)
                topnArray = (WrappedArray) row.apply(9);
                Assert.assertEquals("[3123.0,['ATOM']]", topnArray.apply(0).toString());// TOP_N(ID4)
                Assert.assertEquals("[313.0,['FT']]", topnArray.apply(1).toString());// TOP_N(ID4)
                topnArray = (WrappedArray) row.apply(10);
                Assert.assertEquals("[2.0,['FT']]", topnArray.apply(0).toString());// TOP_N(PRICE5)
                Assert.assertEquals("[1.0,['ATOM']]", topnArray.apply(1).toString());// TOP_N(PRICE5)
                topnArray = (WrappedArray) row.apply(11);
                Assert.assertEquals("[7.0,['FT']]", topnArray.apply(0).toString());// TOP_N(PRICE6)
                Assert.assertEquals("[1.0,['ATOM']]", topnArray.apply(1).toString());// TOP_N(PRICE6)
                topnArray = (WrappedArray) row.apply(12);
                Assert.assertEquals("[1.0,['ATOM']]", topnArray.apply(0).toString());// TOP_N(PRICE7)
                Assert.assertEquals("[1.0,['FT']]", topnArray.apply(1).toString());// TOP_N(PRICE7)
                topnArray = (WrappedArray) row.apply(13);
                Assert.assertEquals("[12.0,['ATOM']]", topnArray.apply(0).toString());// TOP_N(NAME4)
                Assert.assertEquals("[2.0,['FT']]", topnArray.apply(1).toString());// TOP_N(NAME4)
            }
            if (row.apply(0).toString().equals("10000000158")) {
                WrappedArray topnArray = (WrappedArray) row.apply(6);
                Assert.assertEquals("[1.0000000158E10,['中国']]", topnArray.apply(0).toString());// TOP_N(ID1)
                Assert.assertEquals("[1.0000000158E10,[null]]", topnArray.apply(1).toString());// TOP_N(ID1)
                topnArray = (WrappedArray) row.apply(7);
                Assert.assertEquals("[3.32342342E8,['中国']]", topnArray.apply(0).toString());// TOP_N(ID2)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(ID2)
                topnArray = (WrappedArray) row.apply(8);
                Assert.assertEquals("[1241.0,['中国']]", topnArray.apply(0).toString());// TOP_N(ID3)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(ID3)
                topnArray = (WrappedArray) row.apply(9);
                Assert.assertEquals("[31233.0,['中国']]", topnArray.apply(0).toString());// TOP_N(ID4)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(ID4)
                topnArray = (WrappedArray) row.apply(10);
                Assert.assertEquals("[5.0,['中国']]", topnArray.apply(0).toString());// TOP_N(PRICE5)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(PRICE5)
                topnArray = (WrappedArray) row.apply(11);
                Assert.assertEquals("[11.0,['中国']]", topnArray.apply(0).toString());// TOP_N(PRICE6)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(PRICE6)
                topnArray = (WrappedArray) row.apply(12);
                Assert.assertEquals("[3.0,['中国']]", topnArray.apply(0).toString());// TOP_N(PRICE7)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(PRICE7)
                topnArray = (WrappedArray) row.apply(13);
                Assert.assertEquals("[12.0,['中国']]", topnArray.apply(0).toString());// TOP_N(NAME4)
                Assert.assertEquals("[null,[null]]", topnArray.apply(1).toString());// TOP_N(NAME4)
            }
            // verify the all null value aggregate
            if (row.apply(0).toString().equals("10000000159")) {
                WrappedArray topnArray = (WrappedArray) row.apply(6);
                Assert.assertEquals("[1.0000000159E10,[null]]", topnArray.apply(0).toString());// TOP_N(ID1)
                topnArray = (WrappedArray) row.apply(7);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(ID2)
                topnArray = (WrappedArray) row.apply(8);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(ID3)
                topnArray = (WrappedArray) row.apply(9);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(ID4)
                topnArray = (WrappedArray) row.apply(10);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(PRICE5)
                topnArray = (WrappedArray) row.apply(11);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(PRICE6)
                topnArray = (WrappedArray) row.apply(12);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(PRICE7)
                topnArray = (WrappedArray) row.apply(13);
                Assert.assertEquals("[null,[null]]", topnArray.apply(0).toString());// TOP_N(NAME4)
            }
        }

        // Validate results between sparksql and cube
        ExecAndComp.execAndCompare(fetchTopNQuerySql(), getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    // Need to fill in support for top-n and percentile #8848
    public void testAllMeasures() throws Exception {
        UdfManager.create(ss);

        //validate Cube Data by decode
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        buildCuboid(generateMeasList());
        //build is done, start to test query
        String querySql = fetchQuerySql();

        populateSSWithCSVData(config, "default", SparderEnv.getSparkSession());
        NDataSegment seg = NDataflowManager.getInstance(config, getProject()).getDataflow(DF_NAME)
                .getLatestReadySegment();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), 1);
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(seg, dataCuboid.getLayoutId()), ss);

        double delta = 0.0001;
        for (Row row : ret.collectAsList()) {
            if (row.apply(0).toString().equals("10000000157")) {
                Assert.assertEquals(132322344, BigDecimal.valueOf(decodePercentileCol(row, 140)).doubleValue(), delta);// percentile(ID2)
                Assert.assertEquals(14123, decodePercentileCol(row, 141), delta);// percenti(ID3)
                Assert.assertEquals(313, decodePercentileCol(row, 142), delta);// percentile(ID4)
                Assert.assertEquals(1, decodePercentileCol(row, 143), delta);// percentile(PRICE5)
                Assert.assertEquals(1, decodePercentileCol(row, 144), delta);// percentile(PRICE6)
                Assert.assertEquals(1, decodePercentileCol(row, 145), delta);// percentile(PRICE7)
                Assert.assertEquals(2, decodePercentileCol(row, 146), delta);// percentile(NAME4)
            }
            if (row.apply(0).toString().equals("10000000158")) {
                Assert.assertEquals(332342336, BigDecimal.valueOf(decodePercentileCol(row, 140)).doubleValue(), delta);// percentile(ID2)
                Assert.assertEquals(1241, decodePercentileCol(row, 141), delta);// percentile(ID3)
                Assert.assertEquals(31233, decodePercentileCol(row, 142), delta);// percentile(ID4)
                Assert.assertEquals(5, decodePercentileCol(row, 143), delta);// percentile(PRICE5)
                Assert.assertEquals(11, decodePercentileCol(row, 144), delta);// percentile(PRICE6)
                Assert.assertEquals(3, decodePercentileCol(row, 145), delta);// percentile(PRICE7)
                Assert.assertEquals(12, decodePercentileCol(row, 146), delta);// percentile(NAME4)
            }
            // verify the all null value aggregate
            if (row.apply(0).toString().equals("10000000160")) {
                Assert.assertNull(decodePercentileCol(row, 140));// percentile(ID2)
                Assert.assertNull(decodePercentileCol(row, 141));// percentile(ID3)
                Assert.assertNull(decodePercentileCol(row, 142));// percentile(ID4)
                Assert.assertNull(decodePercentileCol(row, 143));// percentile(PRICE5)
                Assert.assertNull(decodePercentileCol(row, 144));// percentile(PRICE6)
                Assert.assertNull(decodePercentileCol(row, 145));// percentile(PRICE7)
                Assert.assertNull(decodePercentileCol(row, 146));// percentile(NAME4)
            }
        }
        Pair<String, String> pair = new Pair<>("sql", querySql);

        ExecAndComp.execAndCompare(newArrayList(pair), getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testCountOneReplaceCountColumn() throws Exception {
        overwriteSystemProp("kylin.query.replace-count-column-with-count-star", "true");
        NDataModel.Measure countOneMeasure = new NDataModel.Measure();
        countOneMeasure.setName("COUNT_ONE");
        countOneMeasure.setFunction(
                FunctionDesc.newInstance(FUNC_COUNT, newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        countOneMeasure.setId(200001);

        NDataModel.Measure sumColumnMeasure = new NDataModel.Measure();
        sumColumnMeasure.setName("SUM_COLUMN");
        sumColumnMeasure.setFunction(generateMeasList().get(4).getFunction());
        sumColumnMeasure.setId(200002);

        NDataModel.Measure maxColumnMeasure = new NDataModel.Measure();
        maxColumnMeasure.setName("MAX_COLUMN");
        maxColumnMeasure.setFunction(generateMeasList().get(26).getFunction());
        maxColumnMeasure.setId(200003);

        List<NDataModel.Measure> needMeasures = Lists.newArrayList();
        needMeasures.add(countOneMeasure);
        needMeasures.add(sumColumnMeasure);
        needMeasures.add(maxColumnMeasure);

        buildCuboid(needMeasures);
        UdfManager.create(ss);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, "default", SparderEnv.getSparkSession());
        Pair<String, String> sqlForKap1 = new Pair<>("queryForKap",
                "select max(ID1),Round(avg(ID1),2) from TEST_MEASURE");
        Pair<String, String> sqlForSpark1 = new Pair<>("queryForSpark",
                "select max(ID1),Round(sum(ID1)/count(1),2) from TEST_MEASURE");
        Pair<String, String> sqlForKap2 = new Pair<>("queryForKap", "select count(ID1),count(1) from TEST_MEASURE");
        Pair<String, String> sqlForSpark2 = new Pair<>("queryForSpark", "select count(1),count(1) from TEST_MEASURE");
        Pair<String, String> sqlForKap3 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE)");
        Pair<String, String> sqlForSpark3 = new Pair<>("queryForSpark",
                "select count(1) from (select count(1) from TEST_MEASURE)");
        Pair<String, String> sqlForKap4 = new Pair<>("queryForKap",
                "select count(ID1) from (select count(ID1) ID1 from TEST_MEASURE)");
        Pair<String, String> sqlForSpark4 = new Pair<>("queryForSpark",
                "select count(ID1) from (select count(1) ID1 from TEST_MEASURE)");
        Pair<String, String> sqlForKap5 = new Pair<>("queryForKap",
                "select count(1) from (select count(1) from TEST_MEASURE)");
        Pair<String, String> sqlForSpark5 = new Pair<>("queryForSpark",
                "select count(1) from (select count(1) from TEST_MEASURE)");
        Pair<String, String> sqlForKap6 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForSpark6 = new Pair<>("queryForSpark",
                "select count(1) from (select count(1) from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForKap7 = new Pair<>("queryForKap",
                "select count(ID1) from (select count(ID1) ID1 from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForSpark7 = new Pair<>("queryForSpark",
                "select count(ID1) from (select count(1) ID1 from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForKap8 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE where ID1 > 0)");
        Pair<String, String> sqlForSpark8 = new Pair<>("queryForSpark",
                "select count(1) from (select count(1) from TEST_MEASURE where ID1 > 0)");
        Pair<String, String> sqlForKap9 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE order by count(ID1))");
        Pair<String, String> sqlForSpark9 = new Pair<>("queryForSpark",
                "select count(1) from (select count(ID1) from TEST_MEASURE order by count(ID1))");
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap1, sqlForSpark1, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap2, sqlForSpark2, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap3, sqlForSpark3, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap4, sqlForSpark4, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap5, sqlForSpark5, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap6, sqlForSpark6, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap7, sqlForSpark7, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap8, sqlForSpark8, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap9, sqlForSpark9, "left", getProject(), null));
    }

    @Test
    public void testCountOneNotReplaceCountColumn() throws Exception {
        overwriteSystemProp("kylin.query.replace-count-column-with-count-star", "true");
        NDataModel.Measure countOneMeasure = new NDataModel.Measure();
        countOneMeasure.setName("COUNT_ONE");
        countOneMeasure.setFunction(
                FunctionDesc.newInstance(FUNC_COUNT, newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        countOneMeasure.setId(200001);

        NDataModel.Measure countColumnMeasure = new NDataModel.Measure();
        countColumnMeasure.setName("COUNT_COLUMN");
        countColumnMeasure.setFunction(generateMeasList().get(82).getFunction());
        countColumnMeasure.setId(200002);

        NDataModel.Measure sumColumnMeasure = new NDataModel.Measure();
        sumColumnMeasure.setName("SUM_COLUMN");
        sumColumnMeasure.setFunction(generateMeasList().get(4).getFunction());
        sumColumnMeasure.setId(200003);

        NDataModel.Measure maxColumnMeasure = new NDataModel.Measure();
        maxColumnMeasure.setName("MAX_COLUMN");
        maxColumnMeasure.setFunction(generateMeasList().get(26).getFunction());
        maxColumnMeasure.setId(200004);

        List<NDataModel.Measure> needMeasures = Lists.newArrayList();
        needMeasures.add(countOneMeasure);
        needMeasures.add(countColumnMeasure);
        needMeasures.add(sumColumnMeasure);
        needMeasures.add(maxColumnMeasure);

        buildCuboid(needMeasures);
        UdfManager.create(ss);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, "default", SparderEnv.getSparkSession());
        Pair<String, String> sqlForKap1 = new Pair<>("queryForKap",
                "select max(ID1),Round(avg(ID1),4) from TEST_MEASURE");
        Pair<String, String> sqlForSpark1 = new Pair<>("queryForSpark",
                "select max(ID1),Round(avg(ID1),4) from TEST_MEASURE");
        Pair<String, String> sqlForKap2 = new Pair<>("queryForKap", "select count(ID1),count(1) from TEST_MEASURE");
        Pair<String, String> sqlForSpark2 = new Pair<>("queryForSpark", "select count(ID1),count(1) from TEST_MEASURE");
        Pair<String, String> sqlForKap3 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE)");
        Pair<String, String> sqlForSpark3 = new Pair<>("queryForSpark",
                "select count(1) from (select count(ID1) from TEST_MEASURE)");
        Pair<String, String> sqlForKap4 = new Pair<>("queryForKap",
                "select count(ID1) from (select count(ID1) ID1 from TEST_MEASURE)");
        Pair<String, String> sqlForSpark4 = new Pair<>("queryForSpark",
                "select count(ID1) from (select count(1) ID1 from TEST_MEASURE)");
        Pair<String, String> sqlForKap5 = new Pair<>("queryForKap",
                "select count(1) from (select count(1) from TEST_MEASURE)");
        Pair<String, String> sqlForSpark5 = new Pair<>("queryForSpark",
                "select count(1) from (select count(1) from TEST_MEASURE)");
        Pair<String, String> sqlForKap6 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForSpark6 = new Pair<>("queryForSpark",
                "select count(1) from (select count(ID1) from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForKap7 = new Pair<>("queryForKap",
                "select count(ID1) from (select count(ID1) ID1 from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForSpark7 = new Pair<>("queryForSpark",
                "select count(ID1) from (select count(ID1) ID1 from TEST_MEASURE group by ID1)");
        Pair<String, String> sqlForKap8 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE where ID1 > 0)");
        Pair<String, String> sqlForSpark8 = new Pair<>("queryForSpark",
                "select count(1) from (select count(ID1) from TEST_MEASURE where ID1 > 0)");
        Pair<String, String> sqlForKap9 = new Pair<>("queryForKap",
                "select count(1) from (select count(ID1) from TEST_MEASURE order by count(ID1))");
        Pair<String, String> sqlForSpark9 = new Pair<>("queryForSpark",
                "select count(1) from (select count(ID1) from TEST_MEASURE order by count(ID1))");
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap1, sqlForSpark1, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap2, sqlForSpark2, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap3, sqlForSpark3, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap4, sqlForSpark4, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap5, sqlForSpark5, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap6, sqlForSpark6, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap7, sqlForSpark7, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap8, sqlForSpark8, "left", getProject(), null));
        Assert.assertTrue(ExecAndComp.execAndCompareQueryResult(sqlForKap9, sqlForSpark9, "left", getProject(), null));
    }

    @Test
    public void testCollectSetMeasure() throws Exception {
        UdfManager.create(ss);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        buildCuboid(generateCollectSetMeaLists());
        populateSSWithCSVData(config, "default", SparderEnv.getSparkSession());

        NDataSegment seg = NDataflowManager.getInstance(config, getProject()).getDataflow(DF_NAME)
                .getLatestReadySegment();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), 1);
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(seg, dataCuboid.getLayoutId()), ss);

        for (Row row : ret.collectAsList()) {
            if (row.apply(0).toString().equals("10000000157")) {
                WrappedArray collectSetArray1 = (WrappedArray) row.apply(6);
                Assert.assertEquals("132322342", collectSetArray1.apply(0).toString()); // collect_set(ID2)
                Assert.assertEquals("132342342", collectSetArray1.apply(1).toString());
                WrappedArray collectSetArray2 = (WrappedArray) row.apply(7);
                Assert.assertEquals("124123", collectSetArray2.apply(0).toString()); // collect_set(ID3)
                Assert.assertEquals("14123", collectSetArray2.apply(1).toString());
                WrappedArray collectSetArray3 = (WrappedArray) row.apply(8);
                Assert.assertEquals("3123", collectSetArray3.apply(0).toString()); // collect_set(ID4)
                Assert.assertEquals("313", collectSetArray3.apply(1).toString());
                WrappedArray collectSetArray4 = (WrappedArray) row.apply(9);
                Assert.assertEquals("22.334", collectSetArray4.apply(0).toString()); // collect_set(price1)
                Assert.assertEquals("12.34", collectSetArray4.apply(1).toString());
                WrappedArray collectSetArray5 = (WrappedArray) row.apply(10);
                Assert.assertEquals("124.44", collectSetArray5.apply(0).toString()); // collect_set(price2)
                Assert.assertEquals("1234.244", collectSetArray5.apply(1).toString());
                WrappedArray collectSetArray6 = (WrappedArray) row.apply(11);
                Assert.assertEquals("14.2423", collectSetArray6.apply(0).toString()); // collect_set(price3)
                Assert.assertEquals("1434.2423", collectSetArray6.apply(1).toString());
                WrappedArray collectSetArray7 = (WrappedArray) row.apply(12);
                Assert.assertEquals("1", collectSetArray7.apply(0).toString()); // collect_set(price5)
                Assert.assertEquals("2", collectSetArray7.apply(1).toString());
                WrappedArray collectSetArray8 = (WrappedArray) row.apply(13);
                Assert.assertEquals("1", collectSetArray8.apply(0).toString()); // collect_set(price6)
                Assert.assertEquals("7", collectSetArray8.apply(1).toString());
                WrappedArray collectSetArray9 = (WrappedArray) row.apply(14);
                Assert.assertEquals("1", collectSetArray9.apply(0).toString()); // collect_set(price7)
                WrappedArray collectSetArray11 = (WrappedArray) row.apply(15);
                Assert.assertEquals("'FT'", collectSetArray11.apply(0).toString()); // collect_set(name1)
                WrappedArray collectSetArray12 = (WrappedArray) row.apply(16);
                Assert.assertEquals("'FT'", collectSetArray12.apply(0).toString()); // collect_set(name2)
                Assert.assertEquals("'ATOM'", collectSetArray12.apply(1).toString());
                WrappedArray collectSetArray13 = (WrappedArray) row.apply(17);
                Assert.assertEquals("'FT'", collectSetArray13.apply(0).toString()); // collect_set(name3)
                Assert.assertEquals("'ATOM'", collectSetArray13.apply(1).toString());
                WrappedArray collectSetArray14 = (WrappedArray) row.apply(18);
                Assert.assertEquals("12", collectSetArray14.apply(0).toString()); // collect_set(name4)
                Assert.assertEquals("2", collectSetArray14.apply(1).toString());
                WrappedArray collectSetArray15 = (WrappedArray) row.apply(19);
                Assert.assertEquals("2013-03-31", collectSetArray15.apply(0).toString()); // collect_set(time1)
                Assert.assertEquals("2014-03-31", collectSetArray15.apply(1).toString());
                WrappedArray collectSetArray16 = (WrappedArray) row.apply(20);
                Assert.assertEquals("2012-03-21 00:00:00.0", collectSetArray16.apply(0).toString()); // collect_set(time2)
                Assert.assertEquals("2013-03-31 00:00:00.0", collectSetArray16.apply(1).toString());
                WrappedArray collectSetArray17 = (WrappedArray) row.apply(21);
                Assert.assertEquals("true", collectSetArray17.apply(0).toString()); // collect_set(flag)
            }
        }
    }

    @Test
    public void testCollectSetSegmentMerge() throws Exception {
        // prepare and clean up all segments
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        // build 2 segment
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName,
                new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                        SegmentRange.dateToLong("2012-06-01")),
                Sets.newLinkedHashSet(layouts), true);
        indexDataConstructor.buildIndex(dfName,
                new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-06-01"),
                        SegmentRange.dateToLong("2013-01-01")),
                Sets.newLinkedHashSet(layouts), true);

        // merge segment
        df = dsMgr.getDataflow(dfName);
        NDataSegment mergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob mergeJob = NSparkMergingJob.merge(mergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(mergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(mergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(mergeJob.getSparkMergingStep());

        // validate merge parquet result
        NDataSegment segment = dsMgr.getDataflow(dfName).getSegments().get(0);
        NDataLayout dataCuboid = NDataLayout.newDataLayout(segment.getDataflow(), segment.getId(), 1000001);
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(segment, dataCuboid.getLayoutId()), ss);
        List<Row> rows = ret.collectAsList();
        Assert.assertEquals("WrappedArray(2012-01-01)", rows.get(0).apply(49).toString());
        Assert.assertEquals("WrappedArray(2012-01-02)", rows.get(1).apply(49).toString());
        Assert.assertEquals("WrappedArray(2012-01-04)", rows.get(2).apply(49).toString());
        Assert.assertEquals("WrappedArray(2012-01-04)", rows.get(3).apply(49).toString());
        Assert.assertEquals("WrappedArray(2012-01-05)", rows.get(4).apply(49).toString());

        // query merge result
        List<Row> rows1 = ExecAndComp.queryModel(getProject(),
                "select SELLER_ID,collect_set(CAL_DT) from test_kylin_fact group by SELLER_ID").collectAsList();
        Assert.assertEquals("[10000000,WrappedArray(2012-10-24, 2012-06-09)]", rows1.get(0).toString());
        Assert.assertEquals("[10000001,WrappedArray(2012-04-22, 2012-07-11, 2012-09-17, 2012-04-03, 2012-02-20)]",
                rows1.get(1).toString());
        Assert.assertEquals("[10000002,WrappedArray(2012-12-01, 2012-09-18, 2012-01-26, 2012-06-09)]",
                rows1.get(2).toString());
        Assert.assertEquals("[10000003,WrappedArray(2012-05-15, 2012-07-09, 2012-07-10, 2012-01-14, 2012-08-20, "
                + "2012-06-24, 2012-04-18, 2012-03-01)]", rows1.get(3).toString());
        Assert.assertEquals("[10000004,WrappedArray(2012-09-12, 2012-09-13, 2012-04-12, 2012-12-12, 2012-07-25,"
                + " 2012-03-18, 2012-12-13, 2012-08-20, 2012-09-10, 2012-10-10)]", rows1.get(4).toString());
    }

    private Double decodePercentileCol(Row row, int index) {
        PercentileSerializer ps = new PercentileSerializer(DataType.ANY);
        ByteBuffer buffer = ByteBuffer.wrap((byte[]) row.get(index));
        PercentileCounter counter1 = new PercentileCounter(100, buffer.getDouble(1));
        counter1.merge(ps.deserialize(buffer));
        return counter1.getResultEstimate();
    }

    private List<Pair<String, String>> fetchTopNQuerySql() {
        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        IndexPlan indexPlan = indePlanManager.getIndexPlan(DF_NAME);

        List<Pair<String, String>> sqlPair = Lists.newArrayList();
        int index = 0;
        for (NDataModel.Measure mea : indexPlan.getEffectiveMeasures().values()) {
            String exp = mea.getFunction().getExpression();
            if (!FunctionDesc.FUNC_TOP_N.equalsIgnoreCase(exp)) {
                continue;
            }

            ParameterDesc parmeter = mea.getFunction().getParameters().get(0);
            ParameterDesc nextParmeter = mea.getFunction().getParameters().get(1);
            if (parmeter.isColumnType()) {
                StringBuilder sqlBuilder = new StringBuilder("select ");
                StringBuilder topnStr = new StringBuilder(nextParmeter.getValue());
                topnStr.append(" ,sum(").append(parmeter.getColRef().getExpressionInSourceDB()).append(") ")
                        .append(" from TEST_MEASURE ").append(" group by ")
                        .append(nextParmeter.getColRef().getExpressionInSourceDB()).append(" order by ").append(" sum(")
                        .append(parmeter.getColRef().getExpressionInSourceDB()).append(") desc limit 15");
                sqlBuilder.append(topnStr);
                sqlPair.add(new Pair<>(index + "_topn_sql", sqlBuilder.toString()));
            }
        }

        return sqlPair;
    }

    private String fetchQuerySql() {
        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        IndexPlan indexPlan = indePlanManager.getIndexPlan(DF_NAME);
        StringBuilder sqlBuilder;
        sqlBuilder = new StringBuilder("select ");
        for (TblColRef col : indexPlan.getEffectiveDimCols().values())
            sqlBuilder.append(col.getIdentity()).append(",");

        for (NDataModel.Measure mea : indexPlan.getEffectiveMeasures().values()) {
            if (mea.getFunction().getColRefs().size() <= 1)
                continue;
            String exp = mea.getFunction().getExpression();
            ParameterDesc parmeter = mea.getFunction().getParameters().get(0);
            if (parmeter.isColumnType()) {
                // issue: #10137
                if (exp.equalsIgnoreCase(FUNC_PERCENTILE)) {
                    continue;
                }
                if (exp.equalsIgnoreCase(FUNC_COUNT_DISTINCT)) {
                    StringBuilder distinctStr = new StringBuilder("distinct");
                    for (TblColRef col : mea.getFunction().getColRefs()) {
                        distinctStr.append(",").append(col.getExpressionInSourceDB());
                    }
                    distinctStr = new StringBuilder(distinctStr.toString().replaceFirst(",", " "));
                    sqlBuilder.append(String.format(Locale.ROOT, "%s(%s)", "count", distinctStr.toString()))
                            .append(",");
                } else {
                    sqlBuilder.append(
                            String.format(Locale.ROOT, "%s(%s)", exp, parmeter.getColRef().getExpressionInSourceDB()))
                            .append(",");
                }
            } else {
                sqlBuilder.append(String.format(Locale.ROOT, "%s(%s)", exp, parmeter.getValue())).append(",");
            }

        }
        sqlBuilder.append(" 1 from TEST_MEASURE LEFT join TEST_MEASURE1 on TEST_MEASURE.ID1=TEST_MEASURE1.ID1 ");

        sqlBuilder.append(" group by ");
        for (TblColRef col : indexPlan.getEffectiveDimCols().values()) {
            sqlBuilder.append(col.getIdentity()).append(",");
        }
        return StringUtils.removeEnd(sqlBuilder.toString(), ",");
    }

    private List<NDataModel.Measure> generateMeasList() {
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        NDataModel model = modelMgr.getDataModelDesc(DF_NAME);
        List<String> funList = newArrayList(FUNC_SUM, FUNC_MAX, FUNC_MIN, FUNC_COUNT, FUNC_COUNT_DISTINCT,
                FUNC_PERCENTILE);
        List<String> cdReturnTypeList = newArrayList("hllc(10)", "bitmap");
        List<TblColRef> columnList = model.getEffectiveCols().entrySet().stream().filter(entry -> entry.getKey() < 29)
                .map(Map.Entry::getValue).collect(Collectors.toList());

        int meaStart = 110000;
        List<NDataModel.Measure> measureList = model.getAllMeasures();
        for (String fun : funList) {
            for (TblColRef col : columnList) {
                // cannot support sum(date) sum(string) sum(boolean)
                if (fun.equalsIgnoreCase(FUNC_SUM) && (col.getType().isDateTimeFamily()
                        || col.getType().isStringFamily() || col.getType().isBoolean())) {
                    continue;
                }

                String returnType = col.getDatatype();
                if (fun.equalsIgnoreCase(FUNC_COUNT_DISTINCT)) {
                    returnType = cdReturnTypeList.get(RandomUtils.nextInt(2));
                }

                if (fun.equalsIgnoreCase(FUNC_PERCENTILE)) {
                    if (!col.getType().isNumberFamily() || !col.getType().isIntegerFamily()) {
                        continue;
                    } else {
                        returnType = "percentile(100)";
                    }
                }

                NDataModel.Measure measure = CubeUtils.newMeasure(
                        FunctionDesc.newInstance(fun, Lists.newArrayList(ParameterDesc.newInstance(col)), returnType),
                        meaStart + "_" + fun, meaStart++);
                measureList.add(measure);
            }
        }

        List<ParameterDesc> parameters = Lists.newArrayList();
        columnList.stream().forEach(columnRef -> parameters.add(ParameterDesc.newInstance(columnRef)));
        NDataModel.Measure measure = CubeUtils.newMeasure(
                FunctionDesc.newInstance(FUNC_COUNT_DISTINCT, parameters, "hllc(10)"),
                meaStart + "_" + FUNC_COUNT_DISTINCT, meaStart++);
        measureList.add(measure);

        return measureList;
    }

    private List<NDataModel.Measure> generateTopnMeaLists() {
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        NDataModel model = modelMgr.getDataModelDesc(DF_NAME);
        List<TblColRef> columnList = model.getEffectiveCols().entrySet().stream().filter(entry -> entry.getKey() < 29)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        List<TblColRef> groupByCols = Lists.newArrayList(columnList.get(12), columnList.get(14), columnList.get(15),
                columnList.get(16), columnList.get(3));
        int meaStart = 120000;
        List<NDataModel.Measure> measureList = model.getAllMeasures();

        boolean isFirstNumCol = false;
        for (TblColRef groupByCol : groupByCols) {
            for (TblColRef col : columnList) {
                if (col.getExpressionInSourceDB().contains("TEST_MEASURE1"))
                    continue;
                // cannot support topn(date) topn(string) topn(boolean)
                if (!col.getType().isNumberFamily() || !col.getType().isIntegerFamily() || groupByCol.equals(col)) {
                    continue;
                }
                if (!isFirstNumCol) {
                    NDataModel.Measure sumMeasure = CubeUtils.newMeasure(
                            FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
                                    Lists.newArrayList(ParameterDesc.newInstance(col)), null),
                            meaStart + "_" + FunctionDesc.FUNC_SUM, meaStart++);
                    measureList.add(sumMeasure);
                    isFirstNumCol = true;
                }

                List<ParameterDesc> parameters = Lists.newArrayList(ParameterDesc.newInstance(col),
                        ParameterDesc.newInstance(groupByCol));
                NDataModel.Measure measure = CubeUtils.newMeasure(
                        FunctionDesc.newInstance(FUNC_TOP_N, parameters, "topn(10000, 4)"), meaStart + "_" + FUNC_TOP_N,
                        meaStart++);
                measureList.add(measure);
            }
        }

        return measureList;
    }

    private List<NDataModel.Measure> generateCollectSetMeaLists() {
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        NDataModel model = modelMgr.getDataModelDesc(DF_NAME);
        List<TblColRef> columnList = model.getEffectiveCols().values().asList();
        int meaStart = 300000;
        List<NDataModel.Measure> measureList = model.getAllMeasures();

        for (TblColRef col : columnList) {
            List<ParameterDesc> parameters = Lists.newArrayList(ParameterDesc.newInstance(col),
                    ParameterDesc.newInstance(col));
            NDataModel.Measure measure = CubeUtils.newMeasure(
                    FunctionDesc.newInstance(FUNC_COLLECT_SET, parameters, "array"), meaStart + "_" + FUNC_COLLECT_SET,
                    meaStart++);
            measureList.add(measure);
        }
        return measureList;
    }

    private void prepareMeasModel(List<NDataModel.Measure> measureList) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NDataModelManager modelMgr = NDataModelManager.getInstance(config, getProject());
        NDataModel model = modelMgr.getDataModelDesc(DF_NAME);
        model.setAllMeasures(measureList);

        NDataModel modelUpdate = modelMgr.copyForWrite(model);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelMgr.updateDataModelDesc(modelUpdate);
        modelMgr = NDataModelManager.getInstance(config, getProject());
        model = modelMgr.getDataModelDesc(DF_NAME);

        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(config, getProject());
        NDataModel finalModel = model;
        indePlanManager.updateIndexPlan(DF_NAME, copyForWrite -> {
            IndexEntity indexEntity = copyForWrite.getAllIndexes().get(0);
            indexEntity.setMeasures(finalModel.getEffectiveMeasures().inverse().values().asList());
            LayoutEntity layout = indexEntity.getLayouts().get(0);
            List<Integer> colList = newArrayList(indexEntity.getDimensions());
            colList.addAll(indexEntity.getMeasures());
            layout.setColOrder(colList);

            // add another simple layout
            LayoutEntity layout1 = new LayoutEntity();
            layout1.setAuto(true);
            layout1.setId(layout.getId() + 1);
            List<Integer> col1List = newArrayList(indexEntity.getDimensions());
            List<Integer> meaList = Lists.newArrayList(finalModel.getEffectiveMeasures().inverse().values().asList());
            Collections.reverse(meaList);
            col1List.addAll(meaList);
            layout1.setColOrder(col1List);

            indexEntity.setLayouts(Lists.newArrayList(layout, layout1));
            copyForWrite.setIndexes(newArrayList(indexEntity));
        });
    }

    private void buildCuboid(List<NDataModel.Measure> meaList) throws Exception {
        prepareMeasModel(meaList);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(DF_NAME);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> toBuildLayouts = df.getIndexPlan().getAllLayouts().subList(0, 2);

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(toBuildLayouts),
                "ADMIN", null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(job));

        val buildStore = ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep());
        AfterBuildResourceMerger merger = new AfterBuildResourceMerger(config, getProject());
        val layoutIds = toBuildLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);
    }
}
