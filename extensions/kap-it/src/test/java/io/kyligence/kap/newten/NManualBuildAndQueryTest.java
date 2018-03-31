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
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
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
    public void test_ncube_basic() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        System.setProperty("noBuild", "false");
        //System.setProperty("isDeveloperMode", "true");
        if (Boolean.valueOf(System.getProperty("noBuild"))) {
            System.out.println("Direct query");
        } else if (Boolean.valueOf(System.getProperty("isDeveloperMode"))) {
            fullBuildBasic("ncube_basic");
            fullBuildBasic("ncube_basic_inner");
        } else {
            buildAndMergeCube("ncube_basic");
            buildAndMergeCube("ncube_basic_inner");
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
        String[] joinTypes = new String[] { "left", "inner" };

        for (String joinType : joinTypes) {
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

            // test table index
            queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_raw");
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, joinType);

            queries = NExecAndComp.fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_rawtable");
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, joinType);
        }

        //ITKapKylinQueryTest.testMultiModelQuery
        queries = NExecAndComp.fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql_multi_model");
        NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "left");
    }

    public void buildAndMergeCube(String dfName) throws Exception {
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
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        df = dsMgr.getDataflow(dfName);
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
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegment(4);
        NDataSegment secondSegment = dsMgr.getDataflow(dfName).getSegment(5);
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

    private void fullBuildBasic(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = Lists.newArrayList(layouts);
        builCuboid(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.<NCuboidLayout> newLinkedHashSet(round1));
        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
    }
}
