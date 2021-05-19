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

import java.util.Optional;
import java.util.UUID;

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

    private StreamingJobListener listener;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testStateChangedToRunning() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockRunningState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedForFailure() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockFailedState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedForKilled() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockKilledState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedToFinish() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        listener = new StreamingJobListener(PROJECT, jobId);
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
