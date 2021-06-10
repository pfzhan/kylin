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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_JOB_STATUS;
import static org.apache.kylin.common.exception.ServerErrorCode.ILLEGAL_JOB_STATE_TRANSFER;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS_DELIMITER;
import static org.apache.kylin.job.execution.AbstractExecutable.RUNTIME_INFO;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kyligence.kap.common.metrics.MetricsTag;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

/**
 *
 */
public class NExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(NExecutableManager.class);
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";

    private static final int CMD_EXEC_TIMEOUT_SEC = 60;

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

    public static ExecutablePO toPO(AbstractExecutable executable, String project) {
        ExecutablePO result = new ExecutablePO();
        result.setProject(project);
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        result.setJobType(executable.getJobType());
        result.setTargetModel(executable.getTargetSubject());
        result.setTargetSegments(executable.getTargetSegments());
        result.setTargetPartitions(executable.getTargetPartitions());
        result.getOutput().setResumable(executable.isResumable());
        result.setPriority(executable.getPriority());
        Map<String, Object> runTimeInfo = executable.getRunTimeInfo();
        if (runTimeInfo != null && runTimeInfo.size() > 0) {
            Set<NDataSegment> segments = (HashSet<NDataSegment>) runTimeInfo.get(RUNTIME_INFO);
            if (segments != null) {
                result.getSegments().addAll(segments);
            }
        }
        if (executable instanceof ChainedExecutable) {
            List<ExecutablePO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((ChainedExecutable) executable).getTasks()) {
                tasks.add(toPO(task, project));
            }
            result.setTasks(tasks);
            if (executable instanceof DefaultChainedExecutableOnModel) {
                val handler = ((DefaultChainedExecutableOnModel) executable).getHandler();
                if (handler != null) {
                    result.setHandlerType(handler.getClass().getName());
                }
            }
        }
        return result;
    }

    // only for test
    public void addJob(AbstractExecutable executable) {
        val po = toPO(executable, project);
        addJob(po);
    }

    public void addJob(ExecutablePO executablePO) {
        addJobOutput(executablePO);
        executableDao.addJob(executablePO);

        String project = executablePO.getParams().get(NBatchConstants.P_PROJECT_NAME);
        MetricsGroup.hostTagCounterInc(MetricsName.JOB, MetricsCategory.PROJECT, project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), executablePO.getJobType() == null ? "" : executablePO.getJobType().name());
        MetricsGroup.counterInc(MetricsName.JOB_COUNT, MetricsCategory.PROJECT, project, tags);
        // dispatch job-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv())
            EventBusFactory.getInstance().postWithLimit(new JobReadyNotifier(project));
        else
            UnitOfWork.get()
                    .doAfterUnit(() -> EventBusFactory.getInstance().postWithLimit(new JobReadyNotifier(project)));
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executable.setOutput(executableOutputPO);
        if (CollectionUtils.isEmpty(executable.getTasks())) {
            return;
        }
        for (ExecutablePO subTask : executable.getTasks()) {
            addJobOutput(subTask);
        }
    }

    //for ut
    @VisibleForTesting
    public void deleteJob(String jobId) {
        checkJobCanBeDeleted(jobId);
        executableDao.deleteJob(jobId);
    }

    //for ut
    @VisibleForTesting
    public void deleteAllJob() {
        executableDao.deleteAllJob();
    }

    //for ut
    @VisibleForTesting
    public List<AbstractExecutable> getRunningExecutables(String project, String model) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(model)) {
            return listExecByModelAndStatus(model, ExecutableState::isRunning, null);
        } else {
            return getAllExecutables().stream().filter(e -> e.getStatus().isRunning()).collect(Collectors.toList());
        }
    }

    public void checkJobCanBeDeleted(String jobId) {
        AbstractExecutable executable = getJob(jobId);
        ExecutableState status = executable.getStatus();
        if (ExecutableState.SUCCEED != status && ExecutableState.DISCARDED != status
                && ExecutableState.SUICIDAL != status) {
            throw new IllegalStateException(
                    "Cannot drop running job " + executable.getDisplayName() + ", please discard it first.");
        }
    }

    public AbstractExecutable getJob(String id) {
        if (id == null) {
            return null;
        }
        ExecutablePO executablePO = executableDao.getJobByUuid(id);
        if (executablePO == null) {
            return null;
        }
        try {
            return fromPO(executablePO);
        } catch (Exception e) {
            logger.error(PARSE_ERROR_MSG, e);
            return null;
        }
    }

    public Set<String> getYarnApplicationJobs(String id) {
        ExecutablePO executablePO = executableDao.getJobByUuid(id);
        String appIds = executablePO.getOutput().getInfo().getOrDefault(YARN_APP_IDS, "");
        return StringUtils.isEmpty(appIds) ? new TreeSet<>()
                : new TreeSet<>(Arrays.asList(appIds.split(YARN_APP_IDS_DELIMITER)));
    }

    public long getCreateTime(String id) {
        ExecutablePO executablePO = executableDao.getJobByUuid(extractJobId(id));
        if (executablePO == null) {
            return 0L;
        }
        return executablePO.getOutput().getCreateTime();
    }

    public Output getOutput(String id) {
        val jobOutput = getJobOutput(id);
        assertOutputNotNull(jobOutput, id);
        return parseOutput(jobOutput);
    }

    public Output getOutputFromHDFSByJobId(String jobId) {
        return getOutputFromHDFSByJobId(jobId, jobId);
    }

    /**
     * get job output from hdfs json file;
     * if json file contains logPath,
     * the logPath is spark driver log hdfs path(*.json.log), read sample data from log file.
     *
     * @param jobId
     * @return
     */
    public Output getOutputFromHDFSByJobId(String jobId, String stepId, int nLines) {
        String outputStorePath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, stepId);
        ExecutableOutputPO jobOutput = getJobOutputFromHDFS(outputStorePath);
        assertOutputNotNull(jobOutput, outputStorePath);

        if (Objects.nonNull(jobOutput.getLogPath())) {
            if (isHdfsPathExists(jobOutput.getLogPath())) {
                jobOutput.setContent(getSampleDataFromHDFS(jobOutput.getLogPath(), nLines));
            } else if (StringUtils.isEmpty(jobOutput.getContent()) && Objects.nonNull(getJob(jobId))
                    && getJob(jobId).getStatus() == ExecutableState.RUNNING) {
                jobOutput.setContent("Wait a moment ... ");
            }
        }

        return parseOutput(jobOutput);
    }

    public Output getOutputFromHDFSByJobId(String jobId, String stepId) {
        return getOutputFromHDFSByJobId(jobId, stepId, 100);
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        result.setStartTime(jobOutput.getStartTime());
        result.setEndTime(jobOutput.getEndTime());
        result.setWaitTime(jobOutput.getWaitTime());
        result.setCreateTime(jobOutput.getCreateTime());
        result.setByteSize(jobOutput.getByteSize());
        return result;
    }

    public List<AbstractExecutable> getAllExecutables() {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs()) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (Exception e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public long countByModelAndStatus(String model, Predicate<ExecutableState> predicate) {
        return listExecByModelAndStatus(model, predicate, null).size();
    }

    public List<AbstractExecutable> listExecByModelAndStatus(String model, Predicate<ExecutableState> predicate,
            JobTypeEnum... jobTypes) {
        return getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model)) //
                .filter(e -> predicate.test(e.getStatus())) //
                .filter(e -> Array.isEmpty(jobTypes) || Lists.newArrayList(jobTypes).contains(e.getJobType())) //
                .collect(Collectors.toList());
    }

    public AbstractExecutable getLastSuccessExecByModel(String modelId, JobTypeEnum... jobTypes) {
        List<AbstractExecutable> executables = listExecByModelAndStatus(modelId, state -> ExecutableState.SUCCEED == state, jobTypes);
        if (CollectionUtils.isEmpty(executables)) {
            return null;
        }
        return executables.stream().max(Comparator.comparingLong(AbstractExecutable::getEndTime)).orElse(null);
    }

    public AbstractExecutable getMaxDurationRunningExecByModel(String modelId, JobTypeEnum... jobTypes) {
        List<AbstractExecutable> executables = listExecByModelAndStatus(modelId, state -> ExecutableState.RUNNING == state, jobTypes);
        if (CollectionUtils.isEmpty(executables)) {
            return null;
        }
        return executables.stream().max(Comparator.comparingLong(AbstractExecutable::getDuration)).orElse(null);
    }

    public List<AbstractExecutable> listExecByJobTypeAndStatus(Predicate<ExecutableState> predicate,
            JobTypeEnum... jobTypes) {
        return getAllExecutables().stream() //
                .filter(e -> e.getJobType() != null && (Lists.newArrayList(jobTypes).contains(e.getJobType())))
                .filter(e -> predicate.test(e.getStatus())).collect(Collectors.toList());
    }

    public List<AbstractExecutable> listMultiPartitionModelExec(String model, Predicate<ExecutableState> predicate,
            JobTypeEnum jobType, Set<Long> targetPartitions, Set<String> segmentIds) {
        return getAllExecutables().stream().filter(e -> e.getTargetSubject() != null)
                .filter(e -> e.getTargetSubject().equals(model)).filter(e -> predicate.test(e.getStatus()))
                .filter(e -> {
                    /**
                     *  Select jobs which partition is overlap.
                     *  Attention: Refresh/Index build job will include all partitions.
                     */
                    boolean checkAllPartition = CollectionUtils.isEmpty(targetPartitions)
                            || JobTypeEnum.INDEX_REFRESH == e.getJobType() //
                            || JobTypeEnum.INDEX_REFRESH == jobType //
                            || JobTypeEnum.INDEX_BUILD == e.getJobType() //
                            || JobTypeEnum.INDEX_BUILD == jobType;
                    if (checkAllPartition) {
                        return true;
                    }
                    return !Sets.intersection(e.getTargetPartitions(), targetPartitions).isEmpty();
                }).filter(e -> {
                    if (CollectionUtils.isEmpty(segmentIds)) {
                        return true;
                    }
                    return !Sets.intersection(new HashSet<>(e.getTargetSegments()), segmentIds).isEmpty();
                }).collect(Collectors.toList());
    }

    public List<AbstractExecutable> getExecutablesByStatus(List<String> jobIds, List<ExecutableState> statuses) {
        val executables = getAllExecutables();
        val resultExecutables = new ArrayList<AbstractExecutable>();
        if (CollectionUtils.isNotEmpty(jobIds)) {
            resultExecutables
                    .addAll(executables.stream().filter(t -> jobIds.contains(t.getId())).collect(Collectors.toList()));
        } else {
            resultExecutables.addAll(executables);
        }
        if (CollectionUtils.isNotEmpty(statuses)) {
            return resultExecutables.stream().filter(t -> statuses.contains(t.getStatus()))
                    .collect(Collectors.toList());
        } else {
            return resultExecutables;
        }
    }

    public List<AbstractExecutable> getExecutablesByStatusList(Set<ExecutableState> statusList) {
        Preconditions.checkNotNull(statusList);

        val executables = getAllExecutables();
        if (CollectionUtils.isNotEmpty(statusList)) {
            return executables.stream().filter(t -> statusList.isEmpty() || statusList.contains(t.getStatus()))
                    .collect(Collectors.toList());
        } else {
            return executables;
        }
    }

    public List<AbstractExecutable> getExecutablesByStatus(ExecutableState status) {

        val executables = getAllExecutables();

        if (Objects.nonNull(status)) {
            return executables.stream().filter(t -> t.getStatus() == status).collect(Collectors.toList());
        } else {
            return executables;
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (Exception e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public List<String> getJobs() {
        return Lists.newArrayList(executableDao.getJobIds());
    }

    public List<ExecutablePO> getRunningJobs(int priority) {
        return executableDao.getJobs().stream().filter(po -> {
            Output output = getOutput(po.getId());
            return ExecutablePO.isHigherPriority(po.getPriority(), priority) && output.getState().isProgressing();
        }).collect(Collectors.toList());
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
                    && !host.equals(AddressUtil.getZkLocalInstance().split(":")[0])) {
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

    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (Objects.isNull(job)) {
            return;
        }
        if (!job.getStatus().isNotProgressing()) {
            throw new KylinException(FAILED_UPDATE_JOB_STATUS, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidJobStatusTransaction(), "RESUME", jobId, job.getStatus()));
        }

        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream()
                    .filter(task -> task.getStatus().isNotProgressing() || task.getStatus() == ExecutableState.RUNNING)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
        }
        updateJobOutput(jobId, ExecutableState.READY);
    }

    public void restartJob(String jobId) {
        AbstractExecutable jobToRestart = getJob(jobId);
        if (Objects.isNull(jobToRestart)) {
            return;
        }
        if (jobToRestart.getStatus().isFinalState()) {
            throw new KylinException(FAILED_UPDATE_JOB_STATUS, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidJobStatusTransaction(), "RESTART", jobId, jobToRestart.getStatus()));
        }

        // to redesign: merge executableDao ops
        updateJobReady(jobId);
        executableDao.updateJob(jobId, job -> {
            job.getOutput().setResumable(false);
            job.getOutput().resetTime();
            job.getTasks().forEach(task -> {
                task.getOutput().setResumable(false);
                task.getOutput().resetTime();
            });
            return true;
        });
    }

    public void setJobResumable(final String taskOrJobId) {
        final String jobId = extractJobId(taskOrJobId);
        AbstractExecutable job = getJob(jobId);
        if (Objects.isNull(job)) {
            return;
        }
        if (Objects.equals(taskOrJobId, jobId)) {
            executableDao.updateJob(jobId, executablePO -> {
                executablePO.getOutput().setResumable(true);
                return true;
            });
        } else {
            executableDao.updateJob(jobId, executablePO -> {
                executablePO.getTasks().stream().filter(o -> Objects.equals(taskOrJobId, o.getId()))
                        .forEach(t -> t.getOutput().setResumable(true));
                return true;
            });
        }
    }

    private void updateJobReady(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.READY)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
        }
        updateJobOutput(jobId, ExecutableState.READY);
    }

    public long countCuttingInJobByModel(String model, AbstractExecutable job) {
        return getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model))
                .filter(executable -> executable.getCreateTime() > job.getCreateTime()).count();
    }

    public void suicideJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        job.cancelJob();

        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.SUICIDAL)
                    .filter(task -> task.getStatus() != ExecutableState.SUCCEED)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.SUICIDAL));
        }

        updateJobOutput(jobId, ExecutableState.SUICIDAL);
    }

    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        job.cancelJob();
        updateJobOutput(jobId, ExecutableState.DISCARDED);
    }

    public void errorJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.ERROR)
                    .filter(task -> task.getStatus() != ExecutableState.SUCCEED)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.ERROR));
        }

        updateJobOutput(jobId, ExecutableState.ERROR);
    }

    public void pauseJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (!job.getStatus().isProgressing()) {
            throw new KylinException(FAILED_UPDATE_JOB_STATUS, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidJobStatusTransaction(), "PAUSE", jobId, job.getStatus()));
        }

        updateJobOutput(jobId, ExecutableState.PAUSED);
        // pauseJob may happen when the job has not been scheduled
        // then call this hook after updateJobOutput
        job.onExecuteStopHook();
    }

    @VisibleForTesting
    public ExecutableOutputPO getJobOutput(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = executableDao.getJobByUuid(jobId);
        final ExecutableOutputPO jobOutput;
        if (Objects.isNull(executablePO)) {
            jobOutput = new ExecutableOutputPO();
        } else if (Objects.equals(taskOrJobId, jobId)) {
            jobOutput = executablePO.getOutput();
        } else {
            jobOutput = executablePO.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                    .map(ExecutablePO::getOutput).orElse(null);
        }
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    // for ut only
    @VisibleForTesting
    public void removeBreakPoints(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = executableDao.getJobByUuid(jobId);
        if (Objects.isNull(executablePO)) {
            return;
        }

        if (Objects.equals(taskOrJobId, jobId)) {
            executableDao.updateJob(jobId, job -> {
                job.getParams().remove(NBatchConstants.P_BREAK_POINT_LAYOUTS);
                return true;
            });
        } else {
            executableDao.updateJob(jobId, job -> {
                job.getTasks().stream().filter(t -> t.getId().equals(taskOrJobId))
                        .forEach(t -> t.getParams().remove(NBatchConstants.P_BREAK_POINT_LAYOUTS));
                return true;
            });
        }
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, removeInfo, output, 0);
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output, long byteSize) {
        val jobId = extractJobId(taskOrJobId);
        executableDao.updateJob(jobId, job -> {
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
                    throw new KylinException(ILLEGAL_JOB_STATE_TRANSFER,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getILLEGAL_STATE_TRANSFER(), jobId,
                                    oldStatus.toJobStatus(), newStatus.toJobStatus()));
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
            String newNodeInfo = AddressUtil.getZkLocalInstance();
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

            if (needDestroyProcess(oldStatus, newStatus)) {
                logger.debug("need kill {}, from {} to {}", taskOrJobId, oldStatus, newStatus);
                // kill spark-submit process
                val context = UnitOfWork.get();
                context.doAfterUnit(() -> destroyProcess(taskOrJobId));
            }
            return true;
        });
    }

    private void updateJobStatus(ExecutableOutputPO jobOutput, ExecutableState oldStatus, ExecutableState newStatus) {
        long time = System.currentTimeMillis();

        if (oldStatus == ExecutableState.RUNNING) {
            jobOutput.addEndTime(time);
            return;
        }

        switch (newStatus) {
        case RUNNING:
            jobOutput.addStartTime(time);
            break;
        case SUICIDAL:
        case DISCARDED:
            jobOutput.addStartTime(time);
            jobOutput.addEndTime(time);
            break;
        default:
            break;
        }
    }

    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus) {
        updateJobOutput(taskOrJobId, newStatus, null, null, null);
    }

    public void destroyProcess(String jobId) {
        EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(jobId));
    }

    private AbstractExecutable fromPO(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        Preconditions.checkArgument(StringUtils.isNotEmpty(type),
                "Cannot parse this job: " + executablePO.getId() + ", the type is empty");
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            result.setProject(project);
            result.setParams(executablePO.getParams());
            result.setJobType(executablePO.getJobType());
            result.setTargetSubject(executablePO.getTargetModel());
            result.setTargetSegments(executablePO.getTargetSegments());
            result.setResumable(executablePO.getOutput().isResumable());
            result.setTargetPartitions(executablePO.getTargetPartitions());
            result.setPriority(executablePO.getPriority());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    ((ChainedExecutable) result).addTask(fromPO(subTask));
                }
                if (result instanceof DefaultChainedExecutableOnModel) {
                    val handlerType = executablePO.getHandlerType();
                    if (handlerType != null) {
                        Class<? extends ExecutableHandler> hClazz = ClassUtil.forName(handlerType,
                                ExecutableHandler.class);
                        Constructor<? extends ExecutableHandler> hConstructor = hClazz.getConstructor(String.class,
                                String.class, String.class, String.class, String.class);
                        String segmentId = CollectionUtils.isNotEmpty(result.getTargetSegments())
                                ? result.getTargetSegments().get(0)
                                : null;
                        ExecutableHandler executableHandler = hConstructor.newInstance(project,
                                result.getTargetSubject(), result.getSubmitter(), segmentId, result.getId());
                        ((DefaultChainedExecutableOnModel) result).setHandler(executableHandler);
                    }
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            logger.error("Cannot parse this job...", e);
            throw new IllegalStateException("Cannot parse this job: " + executablePO.getId(), e);
        }
    }

    static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    private boolean needDestroyProcess(ExecutableState from, ExecutableState to) {
        if (from != ExecutableState.RUNNING || to == null) {
            return false;
        }
        return to == ExecutableState.PAUSED || to == ExecutableState.READY || to == ExecutableState.DISCARDED
                || to == ExecutableState.ERROR || to == ExecutableState.SUICIDAL;
    }

    public void updateJobOutputToHDFS(String resPath, ExecutableOutputPO obj) {
        DataOutputStream dout = null;
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            dout = fs.create(path, true);
            JsonUtil.writeValue(dout, obj);
        } catch (Exception e) {
            // the operation to update output to hdfs failed, next task should not be interrupted.
            logger.error("update job output [{}] to HDFS failed.", resPath, e);
        } finally {
            IOUtils.closeQuietly(dout);
        }
    }

    public ExecutableOutputPO getJobOutputFromHDFS(String resPath) {
        DataInputStream din = null;
        try {
            val path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                val executableOutputPO = new ExecutableOutputPO();
                executableOutputPO.setContent("job output not found, please check kylin.log");
                return executableOutputPO;
            }

            din = fs.open(path);
            return JsonUtil.readValue(din, ExecutableOutputPO.class);
        } catch (Exception e) {
            // If the output file on hdfs is corrupt, give an empty output
            logger.error("get job output [{}] from HDFS failed.", resPath, e);
            val executableOutputPO = new ExecutableOutputPO();
            executableOutputPO.setContent("job output broken, please check kylin.log");
            return executableOutputPO;
        } finally {
            IOUtils.closeQuietly(din);
        }
    }

    /**
     * check the hdfs path exists.
     *
     * @param hdfsPath
     * @return
     */
    public boolean isHdfsPathExists(String hdfsPath) {
        if (StringUtils.isBlank(hdfsPath)) {
            return false;
        }

        Path path = new Path(hdfsPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            return fs.exists(path);
        } catch (IOException e) {
            logger.error("check the hdfs path [{}] exists failed, ", hdfsPath, e);
        }

        return false;
    }

    /**
     * get sample data from hdfs log file.
     * specified the lines, will get the first num lines and last num lines.
     *
     * @param resPath
     * @return
     */
    public String getSampleDataFromHDFS(String resPath, final int nLines) {
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                return null;
            }

            FileStatus fileStatus = fs.getFileStatus(path);
            try (FSDataInputStream din = fs.open(path);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(din, Charset.defaultCharset()))) {

                String line;
                StringBuilder sampleData = new StringBuilder();
                for (int i = 0; i < nLines && (line = reader.readLine()) != null; i++) {
                    if (sampleData.length() > 0) {
                        sampleData.append('\n');
                    }
                    sampleData.append(line);
                }

                int offset = sampleData.toString().getBytes(Charset.defaultCharset()).length + 1;
                if (offset < fileStatus.getLen()) {
                    sampleData.append("\n================================================================\n");
                    sampleData.append(tailHdfsFileInputStream(din, offset, fileStatus.getLen(), nLines));
                }
                return sampleData.toString();
            }
        } catch (IOException e) {
            logger.error("get sample data from hdfs log file [{}] failed!", resPath, e);
            return null;
        }
    }

    /**
     * get the last N_LINES lines from the end of hdfs file input stream;
     * reference: https://olapio.atlassian.net/wiki/spaces/PD/pages/1306918958
     *
     * @param hdfsDin
     * @param startPos
     * @param endPos
     * @param nLines
     * @return
     * @throws IOException
     */
    private String tailHdfsFileInputStream(FSDataInputStream hdfsDin, final long startPos, final long endPos,
            final int nLines) throws IOException {
        Preconditions.checkNotNull(hdfsDin);
        Preconditions.checkArgument(startPos < endPos && startPos >= 0);
        Preconditions.checkArgument(nLines >= 0);

        Deque<String> deque = new ArrayDeque<>();
        int buffSize = 8192;
        byte[] byteBuf = new byte[buffSize];

        long pos = endPos;

        // cause by log last char is \n
        hdfsDin.seek(pos - 1);
        int lastChar = hdfsDin.read();
        if ('\n' == lastChar) {
            pos--;
        }

        int bytesRead = (int) ((pos - startPos) % buffSize);
        if (bytesRead == 0) {
            bytesRead = buffSize;
        }

        pos -= bytesRead;
        int lines = nLines;
        while (lines > 0 && pos >= startPos) {
            bytesRead = hdfsDin.read(pos, byteBuf, 0, bytesRead);

            int last = bytesRead;
            for (int i = bytesRead - 1; i >= 0 && lines > 0; i--) {
                if (byteBuf[i] == '\n') {
                    deque.push(new String(byteBuf, i, last - i, StandardCharsets.UTF_8));
                    lines--;
                    last = i;
                }
            }

            if (lines > 0 && last > 0) {
                deque.push(new String(byteBuf, 0, last, StandardCharsets.UTF_8));
            }

            bytesRead = buffSize;
            pos -= bytesRead;
        }

        StringBuilder sb = new StringBuilder();
        while (!deque.isEmpty()) {
            sb.append(deque.pop());
        }

        return sb.length() > 0 && sb.charAt(0) == '\n' ? sb.substring(1) : sb.toString();
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
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
