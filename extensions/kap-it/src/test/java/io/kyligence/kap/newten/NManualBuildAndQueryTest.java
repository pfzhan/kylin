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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.engine.spark.storage.NParquetStorage;
import io.kyligence.kap.job.execution.NExecutableManager;
import io.kyligence.kap.job.impl.threadpool.NDefaultScheduler;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.spark.KapSparkSession;

@Ignore
public class NManualBuildAndQueryTest extends NLocalWithSparkSessionTest {

    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void test_ncube_basic_inner() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        if (true) {
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
            NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

            Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
            // ready dataflow, segment, cuboid layout
            NDataflow df = dsMgr.getDataflow("ncube_basic_inner");

            // cleanup all segments first
            NDataflowUpdate update = new NDataflowUpdate(df.getName());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            dsMgr.updateDataflow(update);
            df = dsMgr.getDataflow("ncube_basic_inner");

            NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
            List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
            List<NCuboidLayout> round1 = Lists.newArrayList(layouts);

            NSpanningTree nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round1, df.getName());
            for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
                NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                        rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
                Assert.assertEquals(null, layout);
            }

            // Build new segment
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1),
                    "ADMIN");
            NSparkCubingStep sparkStep = job.getSparkCubingStep();
            StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
            Assert.assertEquals("hdfs", distMetaUrl.getScheme());
            Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

            // launch the job
            execMgr.addJob(job);

            // wait job done
            ExecutableState status = wait(job);
            Assert.assertEquals(ExecutableState.SUCCEED, status);

        }
        validate("ncube_basic_inner", 1000001, "inner");

        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        // Validate results between sparksql and cube
        populateSSWithCSVData(config, DEFAULT_PROJECT, kapSparkSession);

        List<Pair<String, String>> queries;
        String joinType = "inner";

        //ITKapKylinQueryTest.testLookupQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_lookup");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testCaseWhen
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_casewhen");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testLikeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_like");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testCachedQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_cache");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDerivedColumnQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_derived");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDateTimeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_datetime");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testSubQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_tableau");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME_ROWCOUNT, joinType);

        //ITKapKylinQueryTest.testDimDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct_dim");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testTimeStampAdd
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_timestamp");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testMultiModelQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_multi_model");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testOrderByQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_orderby");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testSnowflakeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_snowflake");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testTopNQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_topn");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testJoinCastQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_join");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testUnionQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_union");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //                //fail, it's ignore ever since
        //                //ITKapKylinQueryTest.testTableauProbing
        //                queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "tableau_probing");
        //                NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);
        //ITKapKylinQueryTest.testWindowQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_window");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testH2Uncapable
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_h2_uncapable");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testGroupingQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_grouping");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testHiveQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_hive"); //gg
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //in lastest KAP, this is ignore only with a confusing comment: "skip, has conflict with raw table, and Kylin CI has covered"
        //ITKapKylinQueryTest.testIntersectCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_intersect_count");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testPercentileQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_percentile");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //need to check it again after global dict
        //ITKapKylinQueryTest.testPreciselyDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct_precisely");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);
        //kap folders

        //ITKapKylinQueryTest.testPowerBiQuery
        queries = NExecAndComp.fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_powerbi");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //execLimitAndValidate
        //ITKapKylinQueryTest.testLimitCorrectness
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
        NExecAndComp.execLimitAndValidate(queries, kapSparkSession, joinType);

        //ITKapKylinQueryTest.testCommonQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

    }

    @Test
    @SuppressWarnings("MethodLength")
    public void test_ncube_basic() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        if (true) {

            if (Boolean.valueOf(System.getProperty("isDeveloperMode"))) {
                fullBuildBasic();
            } else {
                buildAndMergeCube();
            }
        }

        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        // Validate results between sparksql and cube
        populateSSWithCSVData(config, DEFAULT_PROJECT, kapSparkSession);

        List<Pair<String, String>> queries;
        String joinType = "left";

        //ITKapKylinQueryTest.testCommonQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testLookupQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_lookup");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testCaseWhen
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_casewhen");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testLikeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_like");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testCachedQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_cache");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDerivedColumnQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_derived");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDateTimeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_datetime");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testSubQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_tableau");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME_ROWCOUNT, joinType);

        //ITKapKylinQueryTest.testDimDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct_dim");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testTimeStampAdd
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_timestamp");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testOrderByQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_orderby");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testSnowflakeQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_snowflake");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testTopNQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_topn");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testJoinCastQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_join");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testUnionQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_union");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //                //fail, it's ignore ever since
        //                //ITKapKylinQueryTest.testTableauProbing
        //                queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "tableau_probing");
        //                NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testWindowQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_window");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testH2Uncapable
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_h2_uncapable");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testGroupingQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_grouping");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testHiveQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_hive");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //in lastest KAP, this is ignore only with a confusing comment: "skip, has conflict with raw table, and Kylin CI has covered"
        //ITKapKylinQueryTest.testIntersectCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_intersect_count"); //left
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //ITKapKylinQueryTest.testPercentileQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_percentile");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //need to check it again after global dict
        //ITKapKylinQueryTest.testPreciselyDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct_precisely"); //left
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //ITKapKylinQueryTest.testDistinctCountQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_distinct"); //left
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.NONE, joinType);

        //kap folders

        //ITKapKylinQueryTest.testPowerBiQuery
        queries = NExecAndComp.fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_powerbi");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, joinType);

        //execLimitAndValidate
        //ITKapKylinQueryTest.testLimitCorrectness
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
        NExecAndComp.execLimitAndValidate(queries, kapSparkSession, joinType);
    }

    private void validate(String dataflow, int id, String joinType) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow(dataflow);
        NDataSegment seg = df.getSegment(0);
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, id);
        NCuboidLayout layout = dataCuboid.getCuboidLayout();

        // check row count in NDataSegDetails
        System.out.println(seg.getCuboid(1).getRows());
        System.out.println(seg.getCuboid(2).getRows());
        System.out.println(seg.getCuboid(1001).getRows());
        System.out.println(seg.getCuboid(1002).getRows());
        if ("inner".equals(joinType)) {
            Assert.assertEquals(9896, seg.getCuboid(1).getRows());
            Assert.assertEquals(9896, seg.getCuboid(2).getRows());
            Assert.assertEquals(9896, seg.getCuboid(1001).getRows());
            Assert.assertEquals(9896, seg.getCuboid(1002).getRows());
        }
        if ("left".equals(joinType)) {
            Assert.assertEquals(10000, seg.getCuboid(1).getRows());
            Assert.assertEquals(10000, seg.getCuboid(2).getRows());
            Assert.assertEquals(10000, seg.getCuboid(1001).getRows());
            Assert.assertEquals(10000, seg.getCuboid(1002).getRows());
        }

        NParquetStorage storage = new NParquetStorage();
        Dataset<Row> ret = storage.getCuboidData(dataCuboid, ss);
        IDimensionEncodingMap dimEncoding = new NCubeDimEncMap(seg);

        for (TblColRef colRef : seg.getCubePlan().getEffectiveDimCols().values()) {
            dimEncoding.get(colRef);
        }

        RowKeyColumnIO colIO = new RowKeyColumnIO(dimEncoding);
        MeasureCodec measureCodec = new MeasureCodec(layout.getOrderedMeasures().values().toArray(new MeasureDesc[0]));
        int ms = layout.getOrderedMeasures().size();

        List<String[]> resultFromLayout = new ArrayList<>();
        for (Row row : ret.collectAsList()) {
            List<String> l = new ArrayList<>();
            int i = 0;
            for (TblColRef rowkey : layout.getOrderedDimensions().values()) {
                byte[] bytes = (byte[]) row.get(i);
                String value = colIO.readColumnString(rowkey, bytes, 0, bytes.length);
                l.add(value);
                i++;
            }
            for (int j = 0; j < ms; j++, i++) {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) row.get(i));
                String m = measureCodec.decode(j, buffer).toString();
                l.add(m);
            }
            resultFromLayout.add(l.toArray(new String[0]));
        }
        //        long actualCount = 0L;
        //        long actualMax = 0L;
        //        for (String[] row : resultFromLayout) {
        //            if (row[0].equals("402")) {
        //                actualCount = Long.parseLong(row[2]);
        //                actualMax = Long.parseLong(row[3]);
        //            }
        //        }
        //        Assert.assertEquals(402, resultFromLayout.size());
        //        Assert.assertEquals(402, resultFromLayout.size());
        //        Assert.assertEquals(1, actualCount);
        //        Assert.assertEquals(1, actualMax);
    }

    public void buildAndMergeCube() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        /**
         * Round1. Build 4 segment
         */
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment secondMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2013-01-01"), SegmentRange.dateToLong("2015-06-01")), false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, Sets.newLinkedHashSet(layouts),
                "ADMIN");
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob));

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow("ncube_basic").getSegment(4);
        NDataSegment secondSegment = dsMgr.getDataflow("ncube_basic").getSegment(5);
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2015-01-01")), secondSegment.getSegRange());
        Assert.assertEquals(19, firstSegment.getDictionaries().size());
        Assert.assertEquals(19, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());

        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
    }

    private void fullBuildBasic() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("ncube_basic");

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("ncube_basic");
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = Lists.newArrayList(layouts);
        builCuboid("ncube_basic", SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.<NCuboidLayout> newLinkedHashSet(round1));
        //        validate("ncube_basic", 1);
        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
    }
}
