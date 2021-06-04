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
package io.kyligence.kap.streaming.jobs;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.streaming.event.StreamingJobDropEvent;
import io.kyligence.kap.streaming.event.StreamingJobKillEvent;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;

public class StreamingJobListenerTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        EventBusFactory.getInstance().register(eventListener, true);
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().restart();
        this.cleanupTestMetadata();
    }

    @Test
    public void testStateChangedToRunning() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setSkipListener(true);
        });
        listener.stateChanged(mockRunningState());
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        Assert.assertFalse(jobMeta.isSkipListener());
    }

    @Test
    public void testStateChangedToFailure() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockFailedState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);
            copyForWrite.setSkipListener(true);
        });
        listener.stateChanged(mockFailedState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPING, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 2 * 60 * 1000);
            copyForWrite.setLastUpdateTime(simpleFormat.format(cal.getTime()));
        });
        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPING, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedToKilled() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        listener.stateChanged(mockKilledState());
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
            copyForWrite.setSkipListener(true);
        });
        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 3 * 60 * 1000);
            copyForWrite.setLastUpdateTime(simpleFormat.format(cal.getTime()));
        });
        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedToFinish() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockRunningState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        listener.stateChanged(mockFinishedState());
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
    }

    @Test
    public void testOnStreamingJobKill() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        String project = "streaming_test";
        val config = getTestConfig();
        var mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
        EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, modelId));
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testOnStreamingJobDrop() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        String project = "streaming_test";
        val config = getTestConfig();
        var mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNotNull(buildJobMeta);
        Assert.assertNotNull(mergeJobMeta);
        EventBusFactory.getInstance().postSync(new StreamingJobDropEvent(project, modelId));
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNull(buildJobMeta);
        Assert.assertNull(mergeJobMeta);
    }

    private SparkAppHandle mockRunningState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.RUNNING;
            }
        };
    }

    private SparkAppHandle mockFailedState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.FAILED;
            }
        };
    }

    private SparkAppHandle mockKilledState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.KILLED;
            }
        };
    }

    private SparkAppHandle mockFinishedState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.FINISHED;
            }
        };
    }

    public static abstract class AbstractSparkAppHandle implements SparkAppHandle {
        @Override
        public void addListener(Listener listener) {

        }

        @Override
        public String getAppId() {
            return "local-" + UUID.randomUUID();
        }

        @Override
        public void stop() {

        }

        @Override
        public void kill() {

        }

        @Override
        public void disconnect() {

        }

        @Override
        public Optional<Throwable> getError() {
           return null;
        }
    };
}
