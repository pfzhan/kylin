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

package org.apache.kylin.job.execution;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import lombok.val;

/**
 *
 */
public class NExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(NExecutableManager.class);
    /** Dummy value to reflection */
    private static final Object DUMMY_OBJECT = new Object();
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";

    private static final int CMD_EXEC_TIMEOUT_SEC = 60;
    private static final int LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE = 100;

    private static final String KILL_PROCESS_TREE = "kill-process-tree.sh";

    private static final Set<String> REMOVE_INFO = Sets.newHashSet(ExecutableConstants.YARN_APP_ID,
            ExecutableConstants.YARN_APP_URL, ExecutableConstants.YARN_JOB_WAIT_TIME,
            ExecutableConstants.YARN_JOB_RUN_TIME);

    public static NExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, NExecutableManager.class);
    }

    // called by reflection
    static NExecutableManager newInstance(KylinConfig config, String project) {
        return new NExecutableManager(config, project);
    }

    static NExecutableManager newInstance(KylinConfig config) {
        return new NExecutableManager(config, null);
    }

    // ============================================================================

    private final KylinConfig config;
    private String project;
    private final NExecutableDao executableDao;

    private NExecutableManager(KylinConfig config, String project) {
        logger.trace("Using metadata url: {}", config);
        this.config = config;
        this.project = project;
        this.executableDao = NExecutableDao.getInstance(config, project);
    }


    public List<ExecutablePO> getAllJobs() {
        return executableDao.getJobs();
    }

    public List<String> getJobs() {
        return Lists.newArrayList(executableDao.getJobIds());
    }

    public List<ExecutablePO> getAllJobs(long timeStartInMillis, long timeEndInMillis) {
        return executableDao.getJobs(timeStartInMillis, timeEndInMillis);
    }

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        val config = KylinConfig.getInstanceFromEnv();
        exec.setRunAtRemote(config.getRemoteHadoopCliHostname(), config.getRemoteSSHPort(),
                config.getRemoteSSHUsername(), config.getRemoteSSHPassword());
        return exec;
    }

    /** just used to update job error mess */
    public void updateJobError(String taskOrJobId, String failedStepId, String failedSegmentId, String failedStack,
            String failedReason) {
        val jobId = extractJobId(taskOrJobId);

        executableDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput = job.getOutput();
            if (jobOutput.getFailedReason() == null || failedReason == null) {
                jobOutput.setFailedStepId(failedStepId);
                jobOutput.setFailedSegmentId(failedSegmentId);
                jobOutput.setFailedStack(failedStack);
                jobOutput.setFailedReason(failedReason);
            }
            return true;
        });
    }

    public void destroyProcess(String jobId) {
        EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(jobId));
    }

    static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    public void destoryAllProcess() {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        List<String> jobs = getJobs();
        for (String job : jobs) {
            destroyProcess(job);
        }
    }
}
