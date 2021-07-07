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
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
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

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class IncrementalTest implements JobWaiter {
    private static final String modelName = "test_table_index";
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";

    public static String getProject() {
        return project;
    }

    static private final String project = "table_index_incremental";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );
    private final SparkSession sparkSession = sharedSpark.getSpark();
    @Rule
    public EnableTestUser enableTestUser = new EnableTestUser();
    @Rule
    public EnableClickHouseJob test = new EnableClickHouseJob(1, 1, project, modelId, "src/test/resources/ut_meta");
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Before
    public void setUp() throws Exception {
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
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val expectSegments = dataflowManager.getDataflow(modelId).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
        Assert.assertTrue(tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0).containSegments(expectSegments));
    }
}