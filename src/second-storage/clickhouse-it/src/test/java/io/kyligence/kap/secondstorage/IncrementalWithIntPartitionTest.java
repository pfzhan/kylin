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

package io.kyligence.kap.secondstorage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_LOCKING;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class IncrementalWithIntPartitionTest implements JobWaiter {
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";

    public static String getProject() {
        return project;
    }

    static private final String project = "table_index_incremental_with_int_date";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );
    private final SparkSession sparkSession = sharedSpark.getSpark();
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1,
            project, Collections.singletonList(modelId), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Before
    public void setUp() {
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        NLocalWithSparkSessionTest.buildCuboid(dfName, timeRange, indexes, project, true);
    }


    private void mergeSegments(List<String> segIds) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(modelId);
        val jobManager = JobManager.getInstance(config, getProject());
        long start = Long.MAX_VALUE;
        long end = -1;
        for (String id : segIds) {
            val segment = df.getSegment(id);
            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();
            if (segmentStart < start)
                start = segmentStart;
            if (segmentEnd > end)
                end = segmentEnd;
        }

        val mergeSeg = dfMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(start, end), true);
        val jobParam = new JobParam(mergeSeg, modelId, enableTestUser.getUser());
        val jobId = jobManager.mergeSegmentJob(jobParam);
        waitJobFinish(getProject(), jobId);
    }

    @Test
    public void testMergeSegmentWhenSegmentNotInSecondStorage() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        secondStorageEndpoint.cleanStorage(request, segs.subList(0, 1));

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val jobs = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs.forEach(job -> waitJobFinish(getProject(), job.getId()));

        mergeSegments(segs);
        Assert.assertEquals(1, dataflowManager.getDataflow(modelId).getQueryableSegments().size());
        checkSizeInNode();
        secondStorageService.sizeInNode(project);
        checkSizeInNode();
    }

    private void checkSizeInNode() {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val expectSegments = dataflowManager.getDataflow(modelId).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertTrue(tableData.containSegments(expectSegments));
        Long wholeSize = tableData.getPartitions().get(0).getSizeInNode().values().stream().reduce(Long::sum).orElse(0L);
        Assert.assertTrue(wholeSize > 0);
    }

    @Test
    public void testRemoveSegmentFromSecondStorage() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val jobs = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs.forEach(job -> waitJobFinish(getProject(), job.getId()));
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(1, tableData.getPartitions().size());
        val count = getModelRowCount(project, modelId);
        Assert.assertTrue(count > 0);
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        secondStorageEndpoint.cleanStorage(request, segs.subList(0, 1));

        val jobs2 = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs2.forEach(job -> waitJobFinish(getProject(), job.getId()));

        val tableData2 = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(0, tableData2.getPartitions().size());
        val newCount = getModelRowCount(project, modelId);
        Assert.assertTrue(newCount == 0);
    }

    public static int getModelRowCount(String project, String modelId) throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, 20000000001L);
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbcUrl = SecondStorageNodeHelper.resolve(node);
        try (ClickHouse clickHouse = new ClickHouse(jdbcUrl)) {
            val count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`", rs -> {
                try {
                    return rs.getInt(1);
                } catch (SQLException e) {
                    return ExceptionUtils.rethrow(e);
                }
            });
            Assert.assertFalse(count.isEmpty());
            return count.get(0);
        }
    }

    @Test
    public void testRefreshSegmentWhenLocked() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val seg = dataflowManager.getDataflow(modelId).getSegments().getFirstSegment();
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            refreshSegment(seg.getId());
        } catch (Exception e) {
            KylinException cause = (KylinException) e.getCause();
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), cause.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    private void refreshSegment(String segId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(modelId);
        val jobManager = JobManager.getInstance(config, getProject());
        NDataSegment newSeg = dfMgr.refreshSegment(df, df.getSegment(segId).getSegRange());
        val jobParam = new JobParam(newSeg, df.getModel().getId(), enableTestUser.getUser());
        val jobId = jobManager.refreshSegmentJob(jobParam);
        waitJobFinish(project, jobId);
    }

    @Test
    public void testMergeSegmentsWhenLocked() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            mergeSegments(segs);
        } catch (Exception e) {
            KylinException cause = (KylinException) e.getCause();
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), cause.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCheckLock() {
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            LockTypeEnum.checkLock(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(project));
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), e.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCleanSegmentWhenDatabaseNotExists() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP DATABASE " + database);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val jobId = secondStorageService.triggerSegmentsClean(project, modelId, Sets.newHashSet(segs));
        waitJobFinish(project, jobId);
    }

    @Test
    public void testCleanSegmentWhenModelNotExists() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val table = NameUtil.getTable(modelId, 20000000001L);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP TABLE " + database + "." + table);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val jobId = secondStorageService.triggerSegmentsClean(project, modelId, Sets.newHashSet(segs));
        waitJobFinish(project, jobId);
    }

    @Test
    public void testCleanProjectSegments() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val jobId = secondStorageService.projectClean(Arrays.asList(project, "default"));
        waitJobFinish(project, jobId.get(project).get(modelId));

        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(0, tableData.getPartitions().size());
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, modelId));
    }
}
