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

import static io.kyligence.kap.job.execution.AbstractExecutable.RUNTIME_INFO;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATE_TRANSFER_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobAddedNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.execution.ChainedStageExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.execution.handler.ExecutableHandler;
import io.kyligence.kap.job.execution.stage.StageBase;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);
    /** Dummy value to reflection */
    private static final Object DUMMY_OBJECT = new Object();
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";
    private static final int LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE = 100;
    private final KylinConfig config;
    private String project;

    private final JobInfoDao jobInfoDao;

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
        this.jobInfoDao = SpringContext.getBean(JobInfoDao.class);
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
        result.setTag(executable.getTag());
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
        if (executable instanceof ChainedStageExecutable) {
            Map<String, List<ExecutablePO>> taskMap = Maps.newHashMap();
            final Map<String, List<StageBase>> tasksMap = Optional
                    .ofNullable(((ChainedStageExecutable) executable).getStagesMap()).orElse(Maps.newHashMap());
            for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                final List<ExecutablePO> executables = entry.getValue().stream().map(stage -> toPO(stage, project))
                        .collect(Collectors.toList());
                taskMap.put(entry.getKey(), executables);
            }
            if (MapUtils.isNotEmpty(taskMap)) {
                result.setStagesMap(taskMap);
            }
        }
        return result;
    }

    public void addJob(ExecutablePO executablePO) {
        addJobOutput(executablePO);
        jobInfoDao.addJob(executablePO);

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
        jobInfoDao.updateJob(jobId, job -> {
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
                destroyProcess(taskOrJobId);
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

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath, String segmentOrStepId) {
        Preconditions.checkArgument(output != null,
                "there is no related output for job :" + idOrPath + " , segmentOrStep : " + segmentOrStepId);
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

    @Deprecated
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

    public void checkJobCanBeDeleted(AbstractExecutable executable) {
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
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(id);
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

    public AbstractExecutable fromPO(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        Preconditions.checkArgument(StringUtils.isNotEmpty(type),
                "Cannot parse this job: " + executablePO.getId() + ", the type is empty");
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);
            // no construction method to create a random number ID
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(Object.class);
            AbstractExecutable result = constructor.newInstance(DUMMY_OBJECT);
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
            result.setTag(executablePO.getTag());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    final AbstractExecutable abstractExecutable = fromPO(subTask);
                    if (abstractExecutable instanceof ChainedStageExecutable
                            && MapUtils.isNotEmpty(subTask.getStagesMap())) {
                        for (Map.Entry<String, List<ExecutablePO>> entry : subTask.getStagesMap().entrySet()) {
                            final List<StageBase> executables = entry.getValue().stream().map(po -> {
                                final AbstractExecutable executable = fromPO(po);
                                return (StageBase) executable;
                            }).collect(Collectors.toList());
                            ((ChainedStageExecutable) abstractExecutable).setStageMapWithSegment(entry.getKey(),
                                    executables);
                        }
                    }
                    ((ChainedExecutable) result).addTask(abstractExecutable);
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

    public long getCreateTime(String id) {
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(extractJobId(id));
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

    public Output getOutput(String id, String segmentId) {
        val jobOutput = getJobOutput(id, segmentId);
        assertOutputNotNull(jobOutput, id, segmentId);
        return parseOutput(jobOutput);
    }

    public Output getOutput(String id, ExecutablePO executablePO, String segmentId) {
        val jobOutput = getJobOutput(id, executablePO, segmentId);
        assertOutputNotNull(jobOutput, id, segmentId);
        return parseOutput(jobOutput);
    }

    public Output getOutput(String id, ExecutablePO executablePO) {
        val jobOutput = getJobOutput(id, executablePO);
        assertOutputNotNull(jobOutput, id);
        return parseOutput(jobOutput);
    }

    //TODO this will directly access db, need cache?
    public ExecutableOutputPO getJobOutput(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, jobId, executablePO);
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, String segmentId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        return getJobOutput(jobId, executablePO, segmentId);
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, ExecutablePO executablePO) {
        val jobId = extractJobId(taskOrJobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, jobId, executablePO);
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, ExecutablePO executablePO, String segmentId) {
        val jobId = extractJobId(taskOrJobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, jobId, executablePO);
        if (Objects.isNull(jobOutput)) {
            logger.trace("get job output from taskOrJobId : {} and segmentId : {}", taskOrJobId, segmentId);
            final Map<String, List<ExecutablePO>> stageMap = executablePO.getTasks().stream()
                    .map(ExecutablePO::getStagesMap)
                    .filter(map -> MapUtils.isNotEmpty(map) && map.containsKey(segmentId)).findFirst()
                    .orElse(Maps.newHashMap());
            jobOutput = stageMap.getOrDefault(segmentId, Lists.newArrayList()).stream()
                    .filter(po -> po.getId().equals(taskOrJobId)).findFirst().map(ExecutablePO::getOutput).orElse(null);
        }
        assertOutputNotNull(jobOutput, taskOrJobId, segmentId);
        return jobOutput;
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setVerboseMsgStream(jobOutput.getContentStream());
        result.setLastModified(jobOutput.getLastModified());
        result.setStartTime(jobOutput.getStartTime());
        result.setEndTime(jobOutput.getEndTime());
        result.setWaitTime(jobOutput.getWaitTime());
        result.setDuration(jobOutput.getDuration());
        result.setCreateTime(jobOutput.getCreateTime());
        result.setByteSize(jobOutput.getByteSize());
        result.setShortErrMsg(jobOutput.getFailedMsg());
        result.setFailedStepId(jobOutput.getFailedStepId());
        result.setFailedSegmentId(jobOutput.getFailedSegmentId());
        result.setFailedStack(jobOutput.getFailedStack());
        result.setFailedReason(jobOutput.getFailedReason());
        return result;
    }

    public ExecutableOutputPO getExecutableOutputPO(String taskOrJobId, String jobId, ExecutablePO executablePO) {
        ExecutableOutputPO jobOutput;
        if (Objects.isNull(executablePO)) {
            jobOutput = new ExecutableOutputPO();
        } else if (Objects.equals(taskOrJobId, jobId)) {
            jobOutput = executablePO.getOutput();
        } else {
            jobOutput = executablePO.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                    .map(ExecutablePO::getOutput).orElse(null);
        }
        return jobOutput;
    }


    /** just used to update job error mess */
    public void updateJobError(String taskOrJobId, String failedStepId, String failedSegmentId, String failedStack,
                               String failedReason) {
        val jobId = extractJobId(taskOrJobId);

        jobInfoDao.updateJob(jobId, job -> {
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

    @Transactional
    public void resumeJob(String jobId, AbstractExecutable job) {
        if (Objects.isNull(job)) {
            return;
        }
        if (!job.getStatus().isNotProgressing()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "RESUME", jobId, job.getStatus());
        }

        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream()
                    .filter(task -> task.getStatus().isNotProgressing() || task.getStatus() == ExecutableState.RUNNING)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            // update running stage to ready
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.RUNNING
                                            || stage.getStatus(entry.getKey()).isNotProgressing())//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.READY, null, null));
                        }
                    }
                }
            });
        }

        updateJobOutput(jobId, ExecutableState.READY);
    }

    // READY -> PENDING
    @Transactional
    public void publishJob(String jobId, AbstractExecutable job) {
        if (Objects.isNull(job) || job.getStatus() != ExecutableState.READY) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream()
                    .filter(task -> task.getStatus() == ExecutableState.READY)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.PENDING));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            // update running stage to ready
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.READY)//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.PENDING, null, null));
                        }
                    }
                }
            });
        }

        updateJobOutput(jobId, ExecutableState.PENDING);
    }

    /** just used to update stage */
    public void updateStageStatus(String taskOrJobId, String segmentId, ExecutableState newStatus,
                                  Map<String, String> updateInfo, String failedMsg) {
        updateStageStatus(taskOrJobId, segmentId, newStatus, updateInfo, failedMsg, false);
    }

    public void updateStageStatus(String taskOrJobId, String segmentId, ExecutableState newStatus,
                                  Map<String, String> updateInfo, String failedMsg, Boolean isRestart) {
        val jobId = extractJobId(taskOrJobId);
        jobInfoDao.updateJob(jobId, job -> {
            final List<Map<String, List<ExecutablePO>>> collect = job.getTasks().stream()//
                    .map(ExecutablePO::getStagesMap)//
                    .filter(MapUtils::isNotEmpty)//
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(collect)) {
                return false;
            }

            final Map<String, List<ExecutablePO>> stageMapFromSegment = collect.stream()//
                    .filter(map -> map.containsKey(segmentId))//
                    .findFirst().orElse(null);
            if (MapUtils.isNotEmpty(stageMapFromSegment)) {
                ExecutablePO stage = stageMapFromSegment.getOrDefault(segmentId, Lists.newArrayList()).stream()
                        .filter(po -> po.getId().equals(taskOrJobId))//
                        .findFirst().orElse(null);

                ExecutableOutputPO stageOutput = stage.getOutput();
                assertOutputNotNull(stageOutput, taskOrJobId, segmentId);
                return setStageOutput(stageOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
            } else {
                for (Map<String, List<ExecutablePO>> stageMap : collect) {
                    for (Map.Entry<String, List<ExecutablePO>> entry : stageMap.entrySet()) {
                        final ExecutablePO stage = entry.getValue().stream()
                                .filter(po -> po.getId().equals(taskOrJobId))//
                                .findFirst().orElse(null);
                        if (null == stage) {
                            // local spark job(RESOURCE_DETECT): waiteForResource
                            return false;
                        }
                        ExecutableOutputPO stageOutput = stage.getOutput();
                        assertOutputNotNull(stageOutput, taskOrJobId);
                        val flag = setStageOutput(stageOutput, taskOrJobId, newStatus, updateInfo, failedMsg,
                                isRestart);
                        if (!flag) {
                            return false;
                        }
                    }
                }
            }
            return true;
        });
    }

    public boolean setStageOutput(ExecutableOutputPO jobOutput, String taskOrJobId, ExecutableState newStatus,
                                  Map<String, String> updateInfo, String failedMsg, Boolean isRestart) {
        ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
        if (newStatus != null && oldStatus != newStatus) {
            if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                logger.warn(
                        "[UNEXPECTED_THINGS_HAPPENED] wrong job state transfer! There is no valid state transfer from: {} to: {}, job id: {}",
                        oldStatus, newStatus, taskOrJobId);
            }
            if ((oldStatus == ExecutableState.PAUSED && newStatus == ExecutableState.ERROR)
                    || (oldStatus == ExecutableState.SKIP && newStatus == ExecutableState.SUCCEED)) {
                return false;
            }
            if (isRestart || (oldStatus != ExecutableState.SUCCEED && oldStatus != ExecutableState.SKIP)) {
                jobOutput.setStatus(String.valueOf(newStatus));
                updateJobStatus(jobOutput, oldStatus, newStatus);
                logger.info("Job id: {} from {} to {}", taskOrJobId, oldStatus, newStatus);
            }
        }

        Map<String, String> info = Maps.newHashMap(jobOutput.getInfo());
        Optional.ofNullable(updateInfo).ifPresent(map -> {
            final int indexSuccessCount = Integer
                    .parseInt(map.getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0"));
            info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, String.valueOf(indexSuccessCount));
        });
        jobOutput.setInfo(info);
        jobOutput.setLastModified(System.currentTimeMillis());
        jobOutput.setFailedMsg(failedMsg);
        return true;
    }

    @Transactional
    public void restartJob(String jobId, AbstractExecutable jobToRestart) {
        if (Objects.isNull(jobToRestart)) {
            return;
        }
        if (jobToRestart.getStatus().isFinalState()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "RESTART", jobId, jobToRestart.getStatus());
        }

        // to redesign: merge executableDao ops
        updateJobReady(jobId, jobToRestart);
        jobInfoDao.updateJob(jobId, job -> {
            job.getOutput().setResumable(false);
            job.getOutput().resetTime();
            job.getTasks().forEach(task -> {
                task.getOutput().setResumable(false);
                task.getOutput().resetTime();
                if (MapUtils.isNotEmpty(task.getStagesMap())) {
                    for (Map.Entry<String, List<ExecutablePO>> entry : task.getStagesMap().entrySet()) {
                        Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList()).forEach(executablePO -> {
                            executablePO.getOutput().setResumable(false);
                            executablePO.getOutput().resetTime();
                            Map<String, String> stageInfo = Maps.newHashMap(executablePO.getOutput().getInfo());
                            stageInfo.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
                            executablePO.getOutput().setInfo(stageInfo);
                        });
                    }
                }
            });
            return true;
        });
    }

    @Transactional
    private void updateJobReady(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.READY)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList()) //
                                    .stream()//
                                    .filter(stage -> stage.getStatus(entry.getKey()) != ExecutableState.READY)
                                    .forEach(stage -> // when restart, reset stage
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.READY, null, null, true));
                        }
                    }
                }

            });
        }
        // restart waite time
        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_WAITE_TIME, "{}");
        updateJobOutput(jobId, ExecutableState.READY, info);
    }

    @Transactional
    public void discardJob(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            job.cancelJob();
            return null;
        }, project);
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.DISCARDED, null, null));
                        }
                    }
                }
            });
        }
        updateJobOutput(jobId, ExecutableState.DISCARDED);
    }

    public void pauseJob(String jobId, ExecutablePO executablePO, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        if (!job.getStatus().isProgressing()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "PAUSE", jobId, job.getStatus());
        }
        JdbcUtil.withTransaction(() -> {
            updateStagePaused(job);
            Map<String, String> info = getWaiteTime(executablePO, job);
            updateJobOutput(jobId, ExecutableState.PAUSED, info);
            return null;
        });
        // pauseJob may happen when the job has not been scheduled
        // then call this hook after updateJobOutput
        job.onExecuteStopHook();
    }

    public void updateStagePaused(AbstractExecutable job) {
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            // pause running stage
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.RUNNING)//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.PAUSED, null, null));
                        }
                    }
                }
            });
        }
    }

    public Map<String, String> getWaiteTime(ExecutablePO executablePO, AbstractExecutable job) {
        try {
            val oldInfo = Optional.ofNullable(job.getOutput().getExtra()).orElse(Maps.newHashMap());
            Map<String, String> info = Maps.newHashMap(oldInfo);
            if (job instanceof DefaultChainedExecutable) {
                Map<String, String> waiteTime = JsonUtil
                        .readValueAsMap(info.getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));

                final List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
                for (AbstractExecutable task : tasks) {
                    val waitTime = task.getWaitTime();
                    val oldWaitTime = Long.parseLong(waiteTime.getOrDefault(task.getId(), "0"));
                    waiteTime.put(task.getId(), String.valueOf(waitTime + oldWaitTime));
                    if (task instanceof ChainedStageExecutable) {
                        final ChainedStageExecutable stageExecutable = (ChainedStageExecutable) task;
                        Map<String, List<StageBase>> stageMap = Optional.ofNullable(stageExecutable.getStagesMap())
                                .orElse(Maps.newHashMap());
                        val taskStartTime = task.getStartTime();
                        for (Map.Entry<String, List<StageBase>> entry : stageMap.entrySet()) {
                            final String segmentId = entry.getKey();
                            if (waiteTime.containsKey(segmentId)) {
                                break;
                            }
                            final List<StageBase> stageBases = Optional.ofNullable(entry.getValue())
                                    .orElse(Lists.newArrayList());
                            if (CollectionUtils.isNotEmpty(stageBases)) {
                                final StageBase firstStage = stageBases.get(0);
                                val firstStageStartTime = getOutput(firstStage.getId(), executablePO, segmentId).getStartTime();
                                val stageWaiteTIme = firstStageStartTime - taskStartTime > 0
                                        ? firstStageStartTime - taskStartTime
                                        : 0;
                                val oldStageWaiteTIme = Long.parseLong(waiteTime.getOrDefault(segmentId, "0"));
                                waiteTime.put(segmentId, String.valueOf(stageWaiteTIme + oldStageWaiteTIme));
                            }
                        }
                    }
                }
                info.put(NBatchConstants.P_WAITE_TIME, JsonUtil.writeValueAsString(waiteTime));
            }
            return info;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public void discardJob(String jobId) {
        discardJob(jobId, getJob(jobId));
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

    public void makeStageSuccess(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable && StringUtils.equals(taskOrJobId, task.getId())) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) != ExecutableState.SUCCEED)//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.SUCCEED, null, null));
                        }
                    }
                }
            });
        }
    }

    public void makeStageError(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job instanceof DefaultChainedExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable && StringUtils.equals(taskOrJobId, task.getId())) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.RUNNING)//
                                    .forEach(stage -> //
                                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.ERROR, null, null));
                        }
                    }
                }
            });
        }
    }

    public long countCuttingInJobByModel(String model, AbstractExecutable job) {
        return getAllExecutables().stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model))
                .filter(executable -> executable.getCreateTime() > job.getCreateTime()).count();
    }

    public List<AbstractExecutable> getAllExecutables() {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : jobInfoDao.getJobs(project)) {
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
        return listExecutablePOByModelAndStatus(model, predicate, jobTypes).stream().map(this::fromPO)
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> listExecutablePOByModelAndStatus(String model, Predicate<ExecutableState> predicate,
                                                               List<ExecutablePO> jobs, JobTypeEnum... jobTypes) {
        boolean allPass = Array.isEmpty(jobTypes);
        return jobs.stream() //
                .filter(job -> job.getTargetModel() != null) //
                .filter(job -> job.getTargetModel().equals(model)) //
                .filter(job -> predicate.test(ExecutableState.valueOf(job.getOutput().getStatus()))) //
                .filter(job -> allPass || Lists.newArrayList(jobTypes).contains(job.getJobType())) //
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> listExecutablePOByModelAndStatus(String model, Predicate<ExecutableState> predicate,
                                                               JobTypeEnum... jobTypes) {
        return listExecutablePOByModelAndStatus(model, predicate, jobInfoDao.getJobs(project), jobTypes);
    }

    public void setJobResumable(final String taskOrJobId) {
        final String jobId = extractJobId(taskOrJobId);
        AbstractExecutable job = getJob(jobId);
        if (Objects.isNull(job)) {
            return;
        }
        if (Objects.equals(taskOrJobId, jobId)) {
            jobInfoDao.updateJob(jobId, executablePO -> {
                executablePO.getOutput().setResumable(true);
                return true;
            });
        } else {
            jobInfoDao.updateJob(jobId, executablePO -> {
                executablePO.getTasks().stream().filter(o -> Objects.equals(taskOrJobId, o.getId()))
                        .forEach(t -> t.getOutput().setResumable(true));
                return true;
            });
        }
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

    public List<ExecutablePO> getAllJobs() {
        return jobInfoDao.getJobs(project);
    }

    public List<ExecutablePO> getAllJobs(long timeStartInMillis, long timeEndInMillis) {
        return jobInfoDao.getJobs(project, timeStartInMillis, timeEndInMillis);
    }

    public Output getOutputFromHDFSByJobId(String jobId) {
        return getOutputFromHDFSByJobId(jobId, jobId);
    }

    public Output getOutputFromHDFSByJobId(String jobId, String stepId) {
        return getOutputFromHDFSByJobId(jobId, stepId, LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE);
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
                if (nLines == LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE) {
                    jobOutput.setContent(getSampleDataFromHDFS(jobOutput.getLogPath(), nLines));
                } else {
                    jobOutput.setContentStream(getLogStream(jobOutput.getLogPath()));
                }
            } else if (StringUtils.isEmpty(jobOutput.getContent()) && Objects.nonNull(getJob(jobId))
                    && getJob(jobId).getStatus() == ExecutableState.RUNNING) {
                jobOutput.setContent("Wait a moment ... ");
            }
        }

        return parseOutput(jobOutput);
    }

    public InputStream getLogStream(String resPath) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();

            Path path = new Path(resPath);
            if (!fs.exists(path)) {
                return null;
            }
            return fs.open(path);
        } catch (IOException e) {
            logger.error("get FileSystem from hdfs log file [{}] failed!", resPath, e);
            throw new KylinException(FAILED_DOWNLOAD_FILE, e);
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

}
