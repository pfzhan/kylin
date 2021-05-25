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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;

public class StreamingJobStatusWatcherTest extends StreamingTestCase {

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
    public void testExecute() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        var jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.isEmpty());
        watcher.execute(runningJobs);
        jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.containsKey(mergeJobId));
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillBuildJob() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        runningJobs.add(mergeJobId);
        val watcher = new StreamingJobStatusWatcher();
        for (int i = 0; i < 5; i++) {
            watcher.execute(runningJobs);
        }
        val mergeJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillMergeJob() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);

        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        for (int i = 0; i < 8; i++) {
            watcher.execute(runningJobs);
        }
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }
}
