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

import com.google.common.base.Predicate;
import io.kyligence.kap.cluster.ClusterManagerFactory;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobKiller {
    private static final Logger logger = LoggerFactory.getLogger(JobKiller.class);

    public static void killJob(String project, String jobId, Predicate<StreamingJobMeta> predicate) {
        val config = KylinConfig.getInstanceFromEnv();
        val jobMeta = StreamingJobManager.getInstance(config, project).getStreamingJobByUuid(jobId);
        if (predicate.apply(jobMeta)) {
            killProcess(jobMeta);
            killApplication(jobId);
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.ERROR);
        }
    }

    public static synchronized void killApplication(String jobId) {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv() && !StreamingUtils.isLocalMode()) {
            int errCnt = 0;
            while (errCnt++ < 3) {
                try {
                    final IClusterManager cm = ClusterManagerFactory.create(config);
                    if (cm.applicationExisted(jobId)) {
                        cm.killApplication("", jobId);
                        logger.info("kill jobId:" + jobId);
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
     *     2: called from none cluster
     *     0: process is kill successfully
     *     1: process is not existed
     *     negative number: process is kill unsuccessfully
     */
    public static synchronized int killProcess(StreamingJobMeta jobMeta) {
        val config = KylinConfig.getInstanceFromEnv();
        String pid = jobMeta.getProcessId();

        int statusCode = 2;
        if (!StreamingUtils.isJobOnCluster()) {
            return statusCode;
        }
        String nodeInfo = jobMeta.getNodeInfo();
        CliCommandExecutor exec = config.getCliCommandExecutor();
        try {
            String jobId = StreamingUtils.getJobId(jobMeta.getModelId(), jobMeta.getJobType().name());
            val cmd = "ps -ef|grep '" + jobId + "' | grep -v grep|awk '{print $2}'";
            val strLogger = new StringLogger();
            val result = exec.execute(cmd, strLogger);

            int errCode = result.getCode();
            if (errCode == 0) {
                if (!strLogger.getContents().isEmpty()) {
                    statusCode = exec.execute(cmd + "|xargs kill -9", null).getCode();
                } else {
                    statusCode = 1;
                }
            }
        } catch (ShellException e) {
            logger.warn("failed to kill driver {} on {}", nodeInfo, pid);
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
}
