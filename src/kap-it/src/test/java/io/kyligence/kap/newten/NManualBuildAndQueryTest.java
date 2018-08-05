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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockedDistributedLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.spark.KapSparkSession;

@SuppressWarnings("serial")
public class NManualBuildAndQueryTest extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setup() throws Exception {
        super.init();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl", MockedDistributedLock.MockedFactory.class.getName());
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        
        buildCubes();

        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();

        final KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        populateSSWithCSVData(config, DEFAULT_PROJECT, kapSparkSession);

        String[] joinTypes = new String[] { "left", "inner" };
        String[] sameLevelSql = new String[] { "sql", "sql_lookup", "sql_casewhen", "sql_like", "sql_cache",
                "sql_derived", "sql_datetime", "sql_subquery", "sql_distinct_dim", "sql_timestamp", "sql_orderby",
                "sql_snowflake", "sql_topn", "sql_join", "sql_union", "sql_hive", "sql_distinct_precisely", "sql_raw",
                "sql_rawtable" };
        String[] rowcountLevelSql = new String[] { "sql_tableau" };
        String[] noneLevelSql = new String[] { "sql_window", "sql_h2_uncapable", "sql_grouping", "sql_intersect_count",
                "sql_percentile", "sql_distinct", "sql_powerbi" };
        String[] limitedSql = new String[] { "sql" };
        String[] multiSql = new String[] { "sql_multi_model" };
        
        //String[] ignoreSql = new String[] { "tableau_probing" };

        Map<CompareLevel, String[]> allJoinType = new HashMap<CompareLevel, String[]>();
        allJoinType.put(CompareLevel.SAME, sameLevelSql);
        allJoinType.put(CompareLevel.SAME_ROWCOUNT, rowcountLevelSql);
        allJoinType.put(CompareLevel.NONE, noneLevelSql);
        allJoinType.put(CompareLevel.SUBSET, limitedSql);

        final CustomThreadPoolExecutor service = new CustomThreadPoolExecutor();
        int countNum = (rowcountLevelSql.length + noneLevelSql.length + sameLevelSql.length + limitedSql.length)
                * joinTypes.length + multiSql.length;
        final CountDownLatch latch = new CountDownLatch(countNum);
        
        for (final String joinType : joinTypes) {
            for (Map.Entry<CompareLevel, String[]> entry : allJoinType.entrySet()) {
                CompareLevel compareLevel = entry.getKey();
                String[] values = entry.getValue();
                for (String value : values) {
                    Runnable runnable = new QueryRunnbale(kapSparkSession, latch, compareLevel, joinType, value);
                    service.submit(runnable);
                }
            }
        }
        
        QueryRunnbale runnbale = new QueryRunnbale(kapSparkSession, latch, CompareLevel.SAME, "default", multiSql[0]);
        service.submit(runnbale);
        
        try {
            latch.await();
        } finally {
            service.shutdown();
        }
        
        if (!service.reportError())
            Assert.fail();
    }

    private void buildCubes() throws Exception {
        if (Boolean.valueOf(System.getProperty("noBuild", "false"))) {
            System.out.println("Direct query");
        } else if (Boolean.valueOf(System.getProperty("isDeveloperMode", "false"))) {
            fullBuildBasic("ncube_basic");
            fullBuildBasic("ncube_basic_inner");
        } else {
            buildAndMergeCube("ncube_basic");
            buildAndMergeCube("ncube_basic_inner");
        }
    }

    class QueryRunnbale implements Runnable {

        private KapSparkSession kapSparkSession;
        private CountDownLatch latch;
        private NExecAndComp.CompareLevel compareLevel;
        private String joinType;
        private String sqlSuffix;

        public QueryRunnbale(KapSparkSession kapSparkSession, CountDownLatch latch,
                             NExecAndComp.CompareLevel compareLevel, String joinType, String sqlSuffix) {
            this.kapSparkSession = kapSparkSession;
            this.latch = latch;
            this.compareLevel = compareLevel;
            this.joinType = joinType;
            this.sqlSuffix = sqlSuffix;
        }

        @Override
        public void run() {
            try {
                if (NExecAndComp.CompareLevel.SUBSET.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
                    NExecAndComp.execLimitAndValidate(queries, kapSparkSession, joinType);
                } else {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + sqlSuffix);
                    NExecAndComp.execAndCompare(queries, kapSparkSession, compareLevel, joinType);
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "query error: " + sqlSuffix + ", joinType: " + joinType + ", compareLevel: " + compareLevel, e);
            } finally {
                latch.countDown();
            }
        }
    }

    class CustomThreadPoolExecutor extends ThreadPoolExecutor {
        private List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());

        public CustomThreadPoolExecutor() {
            super(9, 9, 1, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(100));
        }

        public boolean reportError() {
            if (exceptions.isEmpty())
                return true;
            
            logger.error("There were exceptions in CustomThreadPoolExecutor");
            for (Throwable ex : exceptions) {
                logger.error("", ex);
            }
            return false;
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            
            if (t != null) {
                exceptions.add(t);
            }
        }
    }

    public void buildAndMergeCube(String dfName) throws Exception {
        if (dfName.equals("ncube_basic")) {
            buildFourSegementAndMerge(dfName);
        }
        if (dfName.equals("ncube_basic_inner")) {
            buildTwoSegementAndMerge(dfName);
        }
    }

    private void buildTwoSegementAndMerge(String dfName) throws Exception{
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow(dfName);
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
        long end = SegmentRange.dateToLong("2013-01-01");
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2015-01-01");
        builCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2015-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegment(2);
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2015-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(21, firstSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
    }

    private void buildFourSegementAndMerge(String dfName) throws Exception{
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow(dfName);
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
        Assert.assertEquals(21, firstSegment.getDictionaries().size());
        Assert.assertEquals(21, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());
    }

    private void fullBuildBasic(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
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
                Sets.<NCuboidLayout>newLinkedHashSet(round1));
    }
}
