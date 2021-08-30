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
package io.kyligence.kap.streaming.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;
import lombok.var;

public class MetaInfoUpdaterTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testUpdate() {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236633";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertEquals(17, seg.getLayoutSize());
        val layout = NDataLayout.newDataLayout(df, seg.getId(), 10002L);
        MetaInfoUpdater.update(PROJECT, seg, layout);
        NDataflowManager mgr1 = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df1 = mgr1.getDataflow(dataflowId);
        val seg1 = df1.getSegment(segId);
        Assert.assertEquals(18, seg1.getLayoutSize());
    }

    @Test
    public void testUpdateJobState() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastUpdateTime());
        Assert.assertNull(jobMeta.getLastEndTime());
        Assert.assertNull(jobMeta.getLastStartTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.ERROR);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastEndTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.RUNNING);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastStartTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.STOPPED);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastEndTime());
        Assert.assertNotNull(jobMeta.getLastUpdateTime());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setYarnAppId("application_1626786933603_1752");
            copyForWrite
                    .setYarnAppUrl("http://sandbox.hortonworks.com:8088/cluster/app/application_1626786933603_1752");
        });
        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.STARTING);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(StringUtils.EMPTY, jobMeta.getYarnAppId());
        Assert.assertEquals(StringUtils.EMPTY, jobMeta.getYarnAppUrl());
    }

    @Test
    public void testMarkGracefulShutdown() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        MetaInfoUpdater.markGracefulShutdown(PROJECT, jobId);
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN, jobMeta.getAction());
    }
}
