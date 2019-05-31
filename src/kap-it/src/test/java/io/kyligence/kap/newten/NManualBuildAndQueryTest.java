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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import lombok.val;

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
        System.setProperty("noBuild", "true");
        System.setProperty("isDeveloperMode", "true");
        buildCubes();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        List<Pair<String, Throwable>> results = execAndGetResults(
                Lists.newArrayList(new QueryCallable(CompareLevel.SAME, "left", "temp"))); //
        report(results);
    }

    @Test
    @Ignore
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();

        buildCubes();

        // build is done, start to test query
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
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
        String[] joinTypes = new String[] { "left", "inner" };
        List<QueryCallable> tasks = new ArrayList<>();
        for (String joinType : joinTypes) {
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_lookup"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_casewhen"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_like"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_cache"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_derived"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_datetime"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_subquery"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_dim"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_timestamp"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_orderby"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_snowflake"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_topn"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_join"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_union"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_hive"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_precisely"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_powerbi"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_raw"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_value"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_magine"));
            //            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_cross_join"));

            // same row count
            tasks.add(new QueryCallable(CompareLevel.SAME_ROWCOUNT, joinType, "sql_tableau"));

            // none
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_window"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_h2_uncapable"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_grouping"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_intersect_count"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_percentile"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_distinct"));

            //execLimitAndValidate
            //            tasks.add(new QueryCallable(CompareLevel.SUBSET, joinType, "sql"));
        }

        // cc tests
        tasks.add(new QueryCallable(CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_common"));
        tasks.add(new QueryCallable(CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_leftjoin"));

        tasks.add(new QueryCallable(CompareLevel.SAME, "inner", "sql_magine_inner"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "inner", "sql_magine_window"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "default", "sql_rawtable"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "default", "sql_multi_model"));
        logger.info("Total {} tasks.", tasks.size());
        return tasks;
    }

    public void buildCubes() throws Exception {
        if (Boolean.valueOf(System.getProperty("noBuild", "false"))) {
            System.out.println("Direct query");
        } else if (Boolean.valueOf(System.getProperty("isDeveloperMode", "false"))) {
            fullBuildCube("89af4ee2-2cdb-4b07-b39e-4c29856309aa", getProject());
            fullBuildCube("741ca86a-1f13-46da-a59f-95fb68615e3a", getProject());
        } else {
            buildAndMergeCube("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            buildAndMergeCube("741ca86a-1f13-46da-a59f-95fb68615e3a");
        }
    }

    class QueryCallable implements Callable<Pair<String, Throwable>> {

        private NExecAndComp.CompareLevel compareLevel;
        private String joinType;
        private String sqlFolder;

        QueryCallable(NExecAndComp.CompareLevel compareLevel, String joinType, String sqlFolder) {
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
                    NExecAndComp.execLimitAndValidate(queries, getProject(), joinType);
                } else if (NExecAndComp.CompareLevel.SAME_SQL_COMPARE.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execCompareQueryAndCompare(queries, getProject(), joinType);

                } else {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
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
        if (dfName.equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa")) {
            buildFourSegementAndMerge(dfName);
        }
        if (dfName.equals("741ca86a-1f13-46da-a59f-95fb68615e3a")) {
            buildTwoSegementAndMerge(dfName);
        }
    }

    private void buildTwoSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow(dfName);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        /**
         * Round1. Build 4 segment
         */
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2013-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2015-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2015-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                UUID.randomUUID().toString());
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(firstMergeJob.getSparkMergingStep());

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0);

        if (getProject().equals("default") && dfName.equals("741ca86a-1f13-46da-a59f-95fb68615e3a")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10002L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(30001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1010001L, new Long[] { 731L, 9163L });
            compareTuples1.put(1020001L, new Long[] { 302L, 6649L });
            compareTuples1.put(1030001L, new Long[] { 44L, 210L });
            compareTuples1.put(1040001L, new Long[] { 9163L, 9884L });
            compareTuples1.put(1050001L, new Long[] { 105L, 276L });
            compareTuples1.put(1060001L, new Long[] { 138L, 286L });
            compareTuples1.put(1070001L, new Long[] { 9880L, 9896L });
            compareTuples1.put(1080001L, new Long[] { 9833L, 9896L });
            compareTuples1.put(1090001L, new Long[] { 9421L, 9884L });
            compareTuples1.put(1100001L, new Long[] { 143L, 6649L });
            compareTuples1.put(1110001L, new Long[] { 4714L, 9884L });
            compareTuples1.put(1120001L, new Long[] { 9884L, 9896L });
            compareTuples1.put(20000000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000010001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000020001L, new Long[] { 9896L, 9896L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);
        }

        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2015-01-01")), firstSegment.getSegRange());
        //Assert.assertEquals(27, firstSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
    }

    private void buildFourSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow(dfName);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        /**
         * Round1. Build 4 segment
         */
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        validateTableExt(df.getModel().getRootFactTableName(), 2054, 1, 12);

        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        validateTableExt(df.getModel().getRootFactTableName(), 4903, 2, 12);

        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        validateTableExt(df.getModel().getRootFactTableName(), 7075, 3, 12);

        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        validateTableExt(df.getModel().getRootFactTableName(), 10000, 4, 12);

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                UUID.randomUUID().toString());
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(firstMergeJob.getSparkMergingStep());

        df = dsMgr.getDataflow(dfName);

        NDataSegment secondMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2013-01-01"), SegmentRange.dateToLong("2015-06-01")), false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, Sets.newLinkedHashSet(layouts),
                "ADMIN", UUID.randomUUID().toString());
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob));
        merger.merge(secondMergeJob.getSparkMergingStep());

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0);
        NDataSegment secondSegment = dsMgr.getDataflow(dfName).getSegments().get(1);

        if (getProject().equals("default") && dfName.equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10002L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(30001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(1000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000010001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000020001L, new Long[] { 4903L, 4903L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);

            Map<Long, NDataLayout> cuboidsMap2 = secondSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples2 = Maps.newHashMap();
            compareTuples2.put(1L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10002L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(30001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(1000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000010001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000020001L, new Long[] { 5097L, 5097L });
            verifyCuboidMetrics(cuboidsMap2, compareTuples2);
        }

        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2015-01-01")), secondSegment.getSegRange());
        //Assert.assertEquals(31, firstSegment.getDictionaries().size());
        //Assert.assertEquals(31, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());
    }

    private void verifyCuboidMetrics(Map<Long, NDataLayout> cuboidsMap, Map<Long, Long[]> compareTuples) {
        compareTuples.forEach((key, value) -> {
            Assert.assertEquals(value[0], (Long) cuboidsMap.get(key).getRows());
            Assert.assertEquals(value[1], (Long) cuboidsMap.get(key).getSourceRows());
        });
    }

    private void validateTableExt(String tableName, long rows, int segSize, int colStatsSize) {
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(),
                getProject());
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        final TableExtDesc tableExt = tableMetadataManager.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableExt);
        Assert.assertEquals(rows, tableExt.getTotalRows());
        Assert.assertEquals(colStatsSize, tableExt.getAllColumnStats().size());
    }
}
