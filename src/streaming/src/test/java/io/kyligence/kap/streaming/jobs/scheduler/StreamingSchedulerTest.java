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
package io.kyligence.kap.streaming.jobs.scheduler;

import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;

public class StreamingSchedulerTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private static String PROJECT = "streaming_test";
    private static String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private static String dataflowId = modelId;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        val map = (Map<String, StreamingScheduler>)ReflectionUtils.getField(StreamingScheduler.class, "INSTANCE_MAP");
        map.clear();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testInit() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        streamingScheduler.init();
        Assert.assertEquals(true, streamingScheduler.getInitialized().get());
        Assert.assertEquals(true, streamingScheduler.getHasStarted().get());
    }

    @Test
    public void testNoneJobNode() {
        val testConfig = getTestConfig();
        testConfig.setProperty("kylin.server.mode", "query");
        val streamingScheduler = new StreamingScheduler(PROJECT);
        Assert.assertEquals(false, streamingScheduler.getInitialized().get());
        Assert.assertEquals(false, streamingScheduler.getHasStarted().get());
    }

    @Test
    public void testProjectExists() {
        val streamingScheduler = StreamingScheduler.getInstance(PROJECT);
        try {
            val streamingScheduler1 = new StreamingScheduler(PROJECT);
        } catch (Exception e) {
            Assert.assertEquals(true, e instanceof IllegalStateException);
        }
        streamingScheduler.forceShutdown();
    }

    @Test
    public void testSubmitJobCheckEpochFail() {
        val streamingScheduler = Mockito.spy(new StreamingScheduler(PROJECT));
        streamingScheduler.init();
        Assert.assertEquals(true, streamingScheduler.getInitialized().get());
        Assert.assertEquals(true, streamingScheduler.getHasStarted().get());
        Mockito.when(streamingScheduler.checkEpochIdFailed(PROJECT)).thenReturn(true);
        thrown.expect(KylinException.class);
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        val testConfig = getTestConfig();
        val mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
    }

    @Test
    public void testSubmitJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val testConfig = getTestConfig();
        val mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.toString());
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testSubmitBuildJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
        jobMeta.setProcessId("9876");
        Assert.assertNotNull(jobMeta.getProcessId());

        var dfMgr = NDataflowManager.getInstance(testConfig, PROJECT);
        var df = dfMgr.getDataflow(dataflowId);
        Assert.assertEquals(1, df.getSegments(SegmentStatusEnum.NEW).size());
        Assert.assertTrue(df.getSegments(SegmentStatusEnum.NEW).get(0).getAdditionalInfo().isEmpty());
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        dfMgr = NDataflowManager.getInstance(testConfig, PROJECT);
        df = dfMgr.getDataflow(dataflowId);
        Assert.assertEquals(0, df.getSegments(SegmentStatusEnum.NEW).size());

    }

    @Test
    public void testSubmitMergeJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());

        var dfMgr = NDataflowManager.getInstance(testConfig, PROJECT);
        var df = dfMgr.getDataflow(dataflowId).copy();
        var seg = df.getSegments(SegmentStatusEnum.NEW).get(0);
        seg.getAdditionalInfo().put("file_layer", "1");
        val update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg);
        dfMgr.updateDataflow(update);
        Assert.assertEquals(1, df.getSegments(SegmentStatusEnum.NEW).size());
        Assert.assertEquals("1", seg.getAdditionalInfo().get("file_layer"));

        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        dfMgr = NDataflowManager.getInstance(testConfig, PROJECT);
        df = dfMgr.getDataflow(dataflowId);
        Assert.assertEquals(0, df.getSegments(SegmentStatusEnum.NEW).size());
    }

    @Test
    public void testStopJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        streamingScheduler.submitJob(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val testConfig = getTestConfig();
        val mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.toString());
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());

        streamingScheduler.stopJob(modelId, JobTypeEnum.STREAMING_BUILD);
        streamingScheduler.stopJob(modelId, JobTypeEnum.STREAMING_MERGE);

        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());

    }

    @Test
    public void testStopBuildJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        val jobType = JobTypeEnum.STREAMING_BUILD;
        val jobId = StreamingUtils.getJobId(modelId, jobType.toString());
        streamingScheduler.submitJob(PROJECT, modelId, jobType);
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());

        streamingScheduler.stopJob(modelId, jobType);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStopMergeJob() {
        val streamingScheduler = new StreamingScheduler(PROJECT);
        val jobType = JobTypeEnum.STREAMING_MERGE;
        val jobId = StreamingUtils.getJobId(modelId, jobType.toString());
        streamingScheduler.submitJob(PROJECT, modelId, jobType);
        val testConfig = getTestConfig();
        val mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());

        streamingScheduler.stopJob(modelId, jobType);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStopYarnJob() {
        val streamingScheduler = Mockito.spy(new StreamingScheduler(PROJECT));
        val jobType = JobTypeEnum.STREAMING_MERGE;
        val jobId = StreamingUtils.getJobId(modelId, jobType.toString());
        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        Mockito.when(streamingScheduler.applicationExisted(jobId)).thenReturn(true);

        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());

        streamingScheduler.stopJob(modelId, jobType);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPING, jobMeta.getCurrentStatus());
    }

    @Test
    public void testRetryJob() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.getParams().put(StreamingConstants.STREAMING_RETRY_ENABLE, "true");
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.getParams().put(StreamingConstants.STREAMING_RETRY_ENABLE, "true");
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        val streamingScheduler = new StreamingScheduler(PROJECT);
        streamingScheduler.retryJob();
        val retryMap = (Map<String, String>) ReflectionUtils.getField(streamingScheduler, "retryMap");
        Assert.assertTrue(retryMap.containsKey(buildJobId));
        Assert.assertTrue(retryMap.containsKey(mergeJobId));

        for (int i = 0; i < 5; i++) {
            streamingScheduler.retryJob();
        }
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testResumeJobOfStartingStatus() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STARTING);
            copyForWrite.setSkipListener(true);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STARTING);
        });
        val instance = new StreamingScheduler(PROJECT);
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        Assert.assertFalse(buildJobMeta.isSkipListener());
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillJobOfStoppingStatus() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);
        });
        val instance = new StreamingScheduler(PROJECT);
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillStreamingJob() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        val instance = new StreamingScheduler(PROJECT);
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        instance.killJob(modelId, JobTypeEnum.STREAMING_MERGE, JobStatusEnum.ERROR);
        instance.killJob(modelId, JobTypeEnum.STREAMING_BUILD, JobStatusEnum.ERROR);
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillYarnApplication() {
        val streamingScheduler = Mockito.spy(new StreamingScheduler(PROJECT));
        val jobType = JobTypeEnum.STREAMING_MERGE;
        val jobId = StreamingUtils.getJobId(modelId, jobType.toString());
        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        Mockito.when(streamingScheduler.applicationExisted(jobId)).thenReturn(true);
        thrown.expect(KylinException.class);
        streamingScheduler.killYarnApplication(jobId, modelId);
    }

    @Test
    public void testForceStopStreamingJob() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        val instance = new StreamingScheduler(PROJECT);
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        instance.killJob(modelId, JobTypeEnum.STREAMING_MERGE, JobStatusEnum.STOPPED);
        instance.killJob(modelId, JobTypeEnum.STREAMING_BUILD, JobStatusEnum.STOPPED);
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testSkipJobListener() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val config = getTestConfig();
        val instance = new StreamingScheduler(PROJECT);
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertFalse(buildJobMeta.isSkipListener());

        instance.skipJobListener(PROJECT, buildJobId, true);
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertTrue(buildJobMeta.isSkipListener());

        instance.skipJobListener(PROJECT, buildJobId, false);
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertFalse(buildJobMeta.isSkipListener());
    }

    @Test
    public void testGetInstanceByProject() {
        val instance = StreamingScheduler.getInstanceByProject(PROJECT + "_not_exist");
        Assert.assertNull(instance);
    }

    @Test
    public void testShutdownByProject() {
        StreamingScheduler.getInstance(PROJECT);
        StreamingScheduler.shutdownByProject(PROJECT);
        val map = (Map<String, StreamingScheduler>)ReflectionUtils.getField(StreamingScheduler.class, "INSTANCE_MAP");
        Assert.assertTrue(map.isEmpty());
    }
}