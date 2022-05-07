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

package io.kyligence.kap.job.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobAddedNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.job.service.JobInfoService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATE_TRANSFER_ILLEGAL;

@Slf4j
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);

    private final KylinConfig config;
    private String project;

    private final NExecutableDao executableDao;
    private final JobInfoService jobInfoService;

    private static final Set<String> REMOVE_INFO = Sets.newHashSet(ExecutableConstants.YARN_APP_ID,
            ExecutableConstants.YARN_APP_URL, ExecutableConstants.YARN_JOB_WAIT_TIME,
            ExecutableConstants.YARN_JOB_RUN_TIME);

    public static ExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, ExecutableManager.class);
    }

    static ExecutableManager newInstance(KylinConfig config, String project) {
        return new ExecutableManager(config, project);
    }

    private ExecutableManager(KylinConfig config, String project) {
        log.trace("Using metadata url: {}", config);
        this.config = config;
        this.project = project;
        this.executableDao = NExecutableDao.getInstance(config, project);
        this.jobInfoService = SpringContext.getBean(JobInfoService.class);
    }

    public void addJob(ExecutablePO executablePO) {
        addJobOutput(executablePO);
        jobInfoService.addJob(executablePO);

        String jobType = executablePO.getJobType() == null ? "" : executablePO.getJobType().name();
        // dispatch job-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
            EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
        } else
            UnitOfWork.get().doAfterUnit(() -> {
                EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
                EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
            });
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executable.setOutput(executableOutputPO);
        if (CollectionUtils.isNotEmpty(executable.getTasks())) {
            for (ExecutablePO subTask : executable.getTasks()) {
                addJobOutput(subTask);
            }
        }
        if (MapUtils.isNotEmpty(executable.getStagesMap())) {
            for (Map.Entry<String, List<ExecutablePO>> entry : executable.getStagesMap().entrySet()) {
                entry.getValue().forEach(this::addJobOutput);
            }
        }
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus) {
        updateJobOutput(taskOrJobId, newStatus, null, null, null);
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, null, null);
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
                                Set<String> removeInfo, String output) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, removeInfo, output, 0);
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
                                Set<String> removeInfo, String output, long byteSize) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, removeInfo, output, byteSize, null);
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
                                Set<String> removeInfo, String output, long byteSize, String failedMsg) {
        val jobId = extractJobId(taskOrJobId);
        jobInfoService.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput;
            ExecutablePO taskOrJob = Objects.equals(taskOrJobId, jobId) ? job
                    : job.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst().orElse(null);
            jobOutput = taskOrJob.getOutput();
            assertOutputNotNull(jobOutput, taskOrJobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    logger.warn(
                            "[UNEXPECTED_THINGS_HAPPENED] wrong job state transfer! There is no valid state transfer from: {} to: {}, job id: {}",
                            oldStatus, newStatus, taskOrJobId);
                    throw new KylinException(JOB_STATE_TRANSFER_ILLEGAL);
                }
                jobOutput.setStatus(String.valueOf(newStatus));
                updateJobStatus(jobOutput, oldStatus, newStatus);
                logger.info("Job id: {} from {} to {}", taskOrJobId, oldStatus, newStatus);
            }
            Map<String, String> info = Maps.newHashMap(jobOutput.getInfo());
            Optional.ofNullable(updateInfo).ifPresent(info::putAll);
            Optional.ofNullable(removeInfo).ifPresent(set -> set.forEach(info::remove));
            if (ExecutableState.READY == newStatus) {
                Optional.ofNullable(REMOVE_INFO).ifPresent(set -> set.forEach(info::remove));
            }
            String oldNodeInfo = info.get("node_info");
            String newNodeInfo = config.getServerAddress();
            if (Objects.nonNull(oldNodeInfo) && !Objects.equals(oldNodeInfo, newNodeInfo)
                    && !Objects.equals(taskOrJobId, jobId)) {
                logger.info("The node running job has changed. Job id: {}, Step name: {}, Switch from {} to {}.", jobId,
                        taskOrJob.getName(), oldNodeInfo, newNodeInfo);
            }
            info.put("node_info", newNodeInfo);
            jobOutput.setInfo(info);
            String appId = info.get(ExecutableConstants.YARN_APP_ID);
            if (StringUtils.isNotEmpty(appId)) {
                logger.info("Add application id {} to {}.", appId, jobId);
                job.addYarnApplicationJob(appId);
            }
            Optional.ofNullable(output).ifPresent(jobOutput::setContent);
            jobOutput.setLastModified(System.currentTimeMillis());

            if (byteSize > 0) {
                jobOutput.setByteSize(byteSize);
            }

            jobOutput.setFailedMsg(failedMsg);

            if (needDestroyProcess(oldStatus, newStatus)) {
                logger.debug("need kill {}, from {} to {}", taskOrJobId, oldStatus, newStatus);
                // kill spark-submit process
                val context = UnitOfWork.get();
                context.doAfterUnit(() -> destroyProcess(taskOrJobId));
            }
            return true;
        });
    }

    static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
    }

    private void updateJobStatus(ExecutableOutputPO jobOutput, ExecutableState oldStatus, ExecutableState newStatus) {
        long time = System.currentTimeMillis();

        if (oldStatus == ExecutableState.RUNNING) {
            jobOutput.addEndTime(time);
            jobOutput.addDuration(time);
            return;
        }

        switch (newStatus) {
            case RUNNING:
                jobOutput.addStartTime(time);
                jobOutput.addLastRunningStartTime(time);
                break;
            case SKIP:
            case SUICIDAL:
            case DISCARDED:
                jobOutput.addStartTime(time);
                jobOutput.addEndTime(time);
                break;
            default:
                break;
        }
    }

    private boolean needDestroyProcess(ExecutableState from, ExecutableState to) {
        if (from != ExecutableState.RUNNING || to == null) {
            return false;
        }
        return to == ExecutableState.PAUSED || to == ExecutableState.READY || to == ExecutableState.DISCARDED
                || to == ExecutableState.ERROR || to == ExecutableState.SUICIDAL;
    }

    public void destroyProcess(String jobId) {
        EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(jobId));
    }

    private boolean resumeRunningJob(ExecutablePO po) {
        boolean result = false;
        if (po.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
            Map<String, String> info = Maps.newHashMap();
            if (Objects.nonNull(po.getOutput().getInfo())) {
                info.putAll(po.getOutput().getInfo());
            }
            Optional.ofNullable(REMOVE_INFO).ifPresent(set -> set.forEach(info::remove));
            po.getOutput().setInfo(info);
            po.getOutput().setStatus(ExecutableState.READY.toString());
            po.getOutput().addEndTime(System.currentTimeMillis());
            result = true;
        }
        for (ExecutablePO task : Optional.ofNullable(po.getTasks()).orElse(Lists.newArrayList())) {
            result = resumeRunningJob(task) || result;
        }
        return result;
    }

    public void resumeAllRunningJobs() {
        val jobs = executableDao.getJobs();
        CliCommandExecutor exe = getCliCommandExecutor();
        for (ExecutablePO executablePO : jobs) {
            try {
                executableDao.updateJob(executablePO.getUuid(), this::resumeRunningJob);
            } catch (Exception e) {
                logger.warn("Failed to resume running job {}", executablePO.getUuid(), e);
            }
            killRemoteProcess(executablePO, exe);
        }
    }

    private void killRemoteProcess(ExecutablePO executablePO, CliCommandExecutor exe) {
        if (!executablePO.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString()))
            return;
        val info = executablePO.getOutput().getInfo();
        val pid = info.get("process_id");
        if (StringUtils.isNotEmpty(pid)) {
            String nodeInfo = info.get("node_info");
            String host = nodeInfo.split(":")[0];
            if (!host.equals(AddressUtil.getLocalInstance().split(":")[0])
                    && !host.equals(config.getServerAddress().split(":")[0])) {
                exe.setRunAtRemote(host, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                        config.getRemoteSSHPassword());
            } else {
                exe.setRunAtRemote(null, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                        config.getRemoteSSHPassword());
            }
            try {
                exe.execute("kill -9 " + pid, null);
            } catch (ShellException e) {
                logger.warn("failed to kill remote driver {} on {}", nodeInfo, pid, e);
            }
        }
    }

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        val config = KylinConfig.getInstanceFromEnv();
        exec.setRunAtRemote(config.getRemoteHadoopCliHostname(), config.getRemoteSSHPort(),
                config.getRemoteSSHUsername(), config.getRemoteSSHPassword());
        return exec;
    }
}
