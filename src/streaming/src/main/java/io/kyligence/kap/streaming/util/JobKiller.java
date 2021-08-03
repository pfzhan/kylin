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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cluster.ClusterManagerFactory;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.val;

public class JobKiller {
    private static final Logger logger = LoggerFactory.getLogger(JobKiller.class);
    private static final String GREP_CMD = "ps -ef|grep '%s' | grep -v grep|awk '{print $2}'";

    private static boolean isYarnEnv = StreamingUtils.isJobOnCluster(KylinConfig.getInstanceFromEnv());

    private static IClusterManager mock = null;

    public static IClusterManager createClusterManager() {
        if (mock != null) {
            return mock;
        } else {
            return ClusterManagerFactory.create(KylinConfig.getInstanceFromEnv());
        }
    }

    public static synchronized void killApplication(String jobId) {
        if (isYarnEnv) {
            int errCnt = 0;
            while (errCnt++ < 3) {
                try {
                    final IClusterManager cm = createClusterManager();
                    if (cm.applicationExisted(jobId)) {
                        cm.killApplication("", jobId);
                        logger.info("kill jobId:{}", jobId);
                    }
                    return;
                } catch (UncheckedTimeoutException e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    /**
     * @param jobMeta
     * @return statusCode value
     *     0: process is killed successfully
     *     1: process is not existed or called from none cluster
     *     negative number: process is kill unsuccessfully
     */
    public static synchronized int killProcess(StreamingJobMeta jobMeta) {
        if (!isYarnEnv) {
            if (jobMeta.getJobType() == JobTypeEnum.STREAMING_BUILD) {
                StreamingEntry.stop();
            } else if (jobMeta.getJobType() == JobTypeEnum.STREAMING_MERGE) {
                StreamingMergeEntry.shutdown();
            }
            return 1;
        } else {
            val strLogger = new StringLogger();
            val exec = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            return killYarnEnvProcess(exec, jobMeta, strLogger);
        }
    }

    public static int killYarnEnvProcess(CliCommandExecutor exec, StreamingJobMeta jobMeta, StringLogger strLogger) {
        String nodeInfo = jobMeta.getNodeInfo();
        int statusCode = -1;

        try {
            String jobId = StreamingUtils.getJobId(jobMeta.getModelId(), jobMeta.getJobType().name());
            int retryCnt = 0;

            boolean forced = false;
            while (retryCnt++ < 6) {
                int errCode = grepProcess(exec, strLogger, jobId);
                if (errCode == 0) {
                    if (!strLogger.getContents().isEmpty()) {
                        if (retryCnt >= 3) {
                            forced = true;
                        }
                        statusCode = doKillProcess(exec, jobId, forced);
                    } else {
                        statusCode = 1;
                        break;
                    }
                }
                StreamingUtils.sleep(1000L * retryCnt);
            }

        } catch (ShellException e) {
            logger.warn("failed to kill driver {} on {}", nodeInfo, jobMeta.getProcessId());
        }
        return statusCode;
    }

    static class StringLogger implements org.apache.kylin.common.util.Logger {
        private List<String> contents = new ArrayList<>(2);

        @Override
        public void log(String message) {
            if (!StringUtils.isEmpty(message)) {
                contents.add(message);
            }
        }

        public List<String> getContents() {
            return contents;
        }
    }

    public static int grepProcess(CliCommandExecutor exec, StringLogger strLogger, String jobId) throws ShellException {
        String cmd = String.format(Locale.getDefault(), GREP_CMD, jobId);
        val result = exec.execute(cmd, strLogger).getCode();
        logger.info("grep process cmd={}, result ={} ", cmd, result);
        return result;
    }

    public static int doKillProcess(CliCommandExecutor exec, String jobId, boolean forced) throws ShellException {
        String cmd = String.format(Locale.getDefault(), GREP_CMD, jobId);
        val force = forced ? " -9" : StringUtils.EMPTY;
        val result = exec.execute(cmd + "|xargs kill" + force, null).getCode();
        logger.info("kill process cmd={}, result ={} ", cmd, result);
        return result;
    }
}
