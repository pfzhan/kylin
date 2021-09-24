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
package io.kyligence.kap.streaming.jobs.thread;

import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.jobs.impl.StreamingJobLauncher;
import io.kyligence.kap.streaming.util.AwaitUtils;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;

public class StreamingJobRunnerTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private static String PROJECT = "streaming_test";
    private StreamingJobRunner runner;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testStop() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val app = new StreamingMergeEntry();
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, buildJobId));
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, mergeJobId));

        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        runner.stop();
        Assert.assertTrue(app.isGracefulShutdown(PROJECT, buildJobId));
        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        runner.init();
        runner.stop();
        Assert.assertTrue(app.isGracefulShutdown(PROJECT, mergeJobId));
    }

    @Test
    public void testStopWithNoInitial() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.stop();
        val app = new StreamingMergeEntry();
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, buildJobId));
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, mergeJobId));
    }

    /**
     * test StreamingJobRunner class 's  run method
     */
    @Test
    public void testBuildJobRunner_run() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        AwaitUtils.await(() -> runner.run(), 10000, () -> {});
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testBuildJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {});
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testMergeJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {});
    }
}
