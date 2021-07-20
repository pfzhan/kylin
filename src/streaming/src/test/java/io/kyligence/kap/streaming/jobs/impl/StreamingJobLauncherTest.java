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
package io.kyligence.kap.streaming.jobs.impl;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.ReflectionUtils;

import lombok.val;

public class StreamingJobLauncherTest extends NLocalFileMetadataTestCase {

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
    public void testBuildJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        Assert.assertTrue(!launcher.isInitialized());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        Assert.assertTrue(launcher.isInitialized());
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(4, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_DURATION_DEFAULT, appArgs[2]);
        Assert.assertEquals("", appArgs[3]);
    }

    @Test
    public void testMergeJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_MERGE_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(4, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT, appArgs[2]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT, appArgs[3]);
    }

    @Test
    public void testStop() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);

        launcher.stop();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val uuid = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        val meta = mgr.getStreamingJobByUuid(uuid);
        Assert.assertEquals(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN, meta.getAction());
    }

    @Test
    public void testStartYarnMergeJob() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        try {
            launcher.startYarnJob();
        } catch (Exception e) {
        }
    }
}