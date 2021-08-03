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

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Logger;
import org.apache.kylin.common.util.ShellException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class JobKillerTest extends StreamingTestCase {
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
    public void testKillApplication() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        JobKiller.killApplication(id);
    }

    @Test
    public void testMockCreateClusterManager() {
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                return true;
            }
        });
        val clusterManager = JobKiller.createClusterManager();
        Assert.assertTrue(clusterManager != null);
        val mock = ReflectionUtils.getField(JobKiller.class, "mock");
        Assert.assertTrue(mock != null);
        val isYarnEnv = (Boolean) ReflectionUtils.getField(JobKiller.class, "isYarnEnv");
        Assert.assertFalse(isYarnEnv);
        ReflectionUtils.setField(JobKiller.class, "mock", null);
    }

    @Test
    public void testCreateClusterManager() {
        val clusterManager = JobKiller.createClusterManager();
        Assert.assertTrue(!(clusterManager instanceof MockClusterManager));
    }

    @Test
    public void testKillYarnApplication() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                return true;
            }
        });
        JobKiller.killApplication(id);
        ReflectionUtils.setField(JobKiller.class, "mock", null);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnApplicationException() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                throw new UncheckedTimeoutException("mock timeout");
            }
        });
        JobKiller.killApplication(id);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillBuildProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        Assert.assertEquals(1, JobKiller.killProcess(streamingJobMeta));
    }

    @Test
    public void testKillMergeProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        Assert.assertEquals(1, JobKiller.killProcess(streamingJobMeta));
    }

    @Test
    public void testKillYarnProcess() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        JobKiller.killProcess(streamingJobMeta);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnEnvProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        val exec = new CliCommandExecutor();
        val strLogger = new JobKiller.StringLogger() {
            public List<String> getContents() {
                val mockList = new ArrayList<String>();
                mockList.add("1");
                return mockList;
            }
        };
        JobKiller.killYarnEnvProcess(exec, streamingJobMeta, strLogger);
    }

    @Test
    public void testKillYarnEnvProcessException() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        val exec = new CliCommandExecutor() {
            public CliCmdExecResult execute(String command, Logger logAppender) throws ShellException {
                throw new ShellException("test");
            }
        };
        JobKiller.killYarnEnvProcess(exec, streamingJobMeta, null);
    }

    @Test
    public void testGrepProcess() {
        val config = getTestConfig();

        CliCommandExecutor exec = config.getCliCommandExecutor();
        val strLogger = new JobKiller.StringLogger();
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b73_simple_test";

        try {
            int resultCode = JobKiller.grepProcess(exec, strLogger, jobId);
            Assert.assertEquals(0, resultCode);
            Assert.assertTrue(strLogger.getContents().isEmpty());
        } catch (Exception e) {
        }
    }

    @Test
    public void testDoKillProcess() {
        val config = getTestConfig();

        CliCommandExecutor exec = config.getCliCommandExecutor();
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b73_simple_test";

        try {
            int resultCode = JobKiller.doKillProcess(exec, jobId, false);
            Assert.assertEquals(0, resultCode);
        } catch (Exception e) {
        }
        try {
            int resultCode = JobKiller.doKillProcess(exec, jobId, true);
            Assert.assertEquals(0, resultCode);
        } catch (Exception e) {
        }
    }

    @Test
    public void testStringLog() {
        val log = new JobKiller.StringLogger();
        Assert.assertTrue(log.getContents().isEmpty());
        log.log("test");
        Assert.assertTrue(!log.getContents().isEmpty());
    }
}
