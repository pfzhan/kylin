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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockedDistributedLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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


@Ignore("see io.kyligence.kap.ut.TestQueryAndBuild")
@SuppressWarnings("serial")
public class NManualBuildAndQueryTest extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    private boolean succeed = true;

    @Before
    public void setup() throws Exception {
        super.init();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        //super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("noBuild");
        System.clearProperty("isDeveloperMode");
    }

    @Test
    @Ignore("for developing")
    public void testTmp() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl", MockedDistributedLock.MockedFactory.class.getName());
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        System.setProperty("noBuild", "true");
        System.setProperty("isDeveloperMode", "true");
        buildCubes();
        KapSparkSession kapSparkSession = prepareSS(config);
        List<Pair<String, Throwable>> results = execAndGetResults(
                Lists.newArrayList(new QueryCallable(kapSparkSession, CompareLevel.SAME, "left", "temp"))); //
        report(results);
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl", MockedDistributedLock.MockedFactory.class.getName());
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        buildCubes();

        // build is done, start to test query
        List<QueryCallable> tasks = prepareAndGenQueryTasks(config);
        List<Pair<String, Throwable>> results = execAndGetResults(tasks);
        Assert.assertEquals(results.size(), tasks.size());
        report(results);
    }

    private List<Pair<String, Throwable>> execAndGetResults(List<QueryCallable> tasks)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(9//
                , 9 //
                , 1 //
                , TimeUnit.DAYS //
                , new LinkedBlockingQueue<Runnable>(100));
        CompletionService<Pair<String, Throwable>> service = new ExecutorCompletionService<>(executor);
        for (QueryCallable task : tasks) {
            service.submit(task);
        }

        List<Pair<String, Throwable>> results = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Pair<String, Throwable> r = service.take().get();
            failFastIfNeeded(r);
            results.add(r);
        }
        executor.shutdown();
        return results;
    }

    private void report(List<Pair<String, Throwable>> results) {
        for (Pair<String, Throwable> result : results) {
            if (result.getSecond() != null) {
                succeed = false;
                logger.error("CI failed on:" + result.getFirst(), result.getSecond());
            }
        }
        if (!succeed) {
            Assert.fail();
        }
    }

    private void failFastIfNeeded(Pair<String, Throwable> result) {
        if (Boolean.valueOf(System.getProperty("failFast", "false")) && result.getSecond() != null) {
            logger.error("CI failed on:" + result.getFirst());
            Assert.fail();
        }
    }

    private List<QueryCallable> prepareAndGenQueryTasks(KylinConfig config) throws Exception {
        KapSparkSession kapSparkSession = prepareSS(config);

        String[] joinTypes = new String[] { "left", "inner" };
        List<QueryCallable> tasks = new ArrayList<>();
        for (String joinType : joinTypes) {
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_lookup"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_casewhen"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_like"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_cache"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_derived"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_datetime"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_subquery"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_distinct_dim"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_timestamp"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_orderby"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_snowflake"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_topn"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_join"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_union"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_hive"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_distinct_precisely"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_powerbi"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_raw"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_value"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_magine"));
            //            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, joinType, "sql_cross_join"));

            // same row count
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME_ROWCOUNT, joinType, "sql_tableau"));

            // none
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_window"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_h2_uncapable"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_grouping"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_intersect_count"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_percentile"));
            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.NONE, joinType, "sql_distinct"));

            //execLimitAndValidate
            //            tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SUBSET, joinType, "sql"));
        }
        
        // cc tests
        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_common"));
        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_leftjoin"));

        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, "inner", "sql_magine_inner"));
        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, "inner", "sql_magine_window"));
        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, "default", "sql_rawtable"));
        tasks.add(new QueryCallable(kapSparkSession, CompareLevel.SAME, "default", "sql_multi_model"));
        logger.info("Total {} tasks.", tasks.size());
        return tasks;
    }

    private KapSparkSession prepareSS(KylinConfig config) throws Exception {
        // build is done, start to test query
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(getProject());
        populateSSWithCSVData(config, getProject(), kapSparkSession);
        kapSparkSession.sparkContext().setLogLevel("ERROR");
        return kapSparkSession;
    }

    public void buildCubes() throws Exception {
        if (Boolean.valueOf(System.getProperty("noBuild", "false"))) {
            System.out.println("Direct query");
        } else if (Boolean.valueOf(System.getProperty("isDeveloperMode", "false"))) {
            fullBuildCube("ncube_basic", getProject());
            fullBuildCube("ncube_basic_inner", getProject());
        } else {
            buildAndMergeCube("ncube_basic");
            buildAndMergeCube("ncube_basic_inner");
        }
    }

    static class QueryCallable implements Callable<Pair<String, Throwable>> {

        private KapSparkSession kapSparkSession;
        private NExecAndComp.CompareLevel compareLevel;
        private String joinType;
        private String sqlFolder;

        QueryCallable(KapSparkSession kapSparkSession, NExecAndComp.CompareLevel compareLevel, String joinType,
                      String sqlFolder) {
            this.kapSparkSession = kapSparkSession;
            this.compareLevel = compareLevel;
            this.joinType = joinType;
            this.sqlFolder = sqlFolder;
        }

        @Override
        public Pair<String, Throwable> call() {
            String identity = "sqlFolder:" + sqlFolder + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
            try {
                if (NExecAndComp.CompareLevel.SUBSET.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql");
                    NExecAndComp.execLimitAndValidate(queries, kapSparkSession, joinType);
                } else if (NExecAndComp.CompareLevel.SAME_SQL_COMPARE.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execCompareQueryAndCompare(queries, kapSparkSession, joinType);
                    
                } else {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execAndCompare(queries, kapSparkSession, compareLevel, joinType);
                }
            } catch (Throwable th) {
                logger.error("Query fail on:", identity);
                return Pair.newPair(identity, th);
            }
            logger.info("Query succeed on:", identity);
            return Pair.newPair(identity, null);
        }
    }

    private void buildAndMergeCube(String dfName) throws Exception {
        if (dfName.equals("ncube_basic")) {
            buildFourSegementAndMerge(dfName);
        }
        if (dfName.equals("ncube_basic_inner")) {
            buildTwoSegementAndMerge(dfName);
        }
    }

    private void buildTwoSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

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
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2015-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
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
        //Assert.assertEquals(27, firstSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
    }

    private void buildFourSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

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
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        validateTableExt(df.getModel().getRootFactTableName(), 2054, 1, 11);

        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        validateTableExt(df.getModel().getRootFactTableName(), 4903, 2, 11);

        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        validateTableExt(df.getModel().getRootFactTableName(), 7075, 3, 11);

        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        validateTableExt(df.getModel().getRootFactTableName(), 10000, 4, 11);

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
        //Assert.assertEquals(31, firstSegment.getDictionaries().size());
        //Assert.assertEquals(31, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());
    }

    private void validateTableExt(String tableName, long rows, int segSize, int colStats) {
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        final TableExtDesc tableExt = tableMetadataManager.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableExt);
        Assert.assertEquals(rows, tableExt.getTotalRows());
        Assert.assertEquals(colStats, tableExt.getColumnStats().size());
        Assert.assertEquals(segSize, tableExt.getLoadingRange().size());
    }
}
