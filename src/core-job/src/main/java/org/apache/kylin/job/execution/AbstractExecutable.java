/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.constant.ExecutableConstants.MR_JOB_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.util.MailHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.common.util.ThrowableUtils;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;
import lombok.experimental.Delegate;

/**
 */
public abstract class AbstractExecutable extends AbstractJobExecutable implements Executable {

    public interface Callback {
        void process() throws Exception;

        default void onProcessError(Throwable throwable) {
        }
    }

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String PARENT_ID = "parentId";
    public static final String RUNTIME_INFO = "runtimeInfo";
    public static final String DEPENDENT_FILES = "dependentFiles";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);

    protected int retry = 0;

    @Getter
    @Setter
    private String name;
    @Getter
    @Setter
    private JobTypeEnum jobType;

    @Getter
    @Setter
    private String logPath;

    @Setter
    @Getter
    private String targetSubject; //uuid of the model or table identity if table sampling

    @Setter
    @Getter
    private List<String> targetSegments = Lists.newArrayList();//uuid of related segments

    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private boolean resumable = false;

    @Delegate
    private ExecutableParams executableParams = new ExecutableParams();
    protected String project;
    protected JobContext context;

    @Getter
    @Setter
    private Map<String, Object> runTimeInfo = Maps.newHashMap();

    @Setter
    @Getter
    private Set<Long> targetPartitions = Sets.newHashSet();

    public boolean isBucketJob() {
        return CollectionUtils.isNotEmpty(targetPartitions);
    }

    @Getter
    @Setter
    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @Getter
    @Setter
    private Object tag;

    @Getter
    @Setter
    private int stepId = -1;

    @Getter
    @Setter
    private ExecutablePO po;

    @Getter
    @Setter
    private JobSchedulerModeEnum jobSchedulerMode = JobSchedulerModeEnum.CHAIN;

    @Getter
    @Setter
    private String previousStep;

    @Getter
    @Setter
    private Set<String> nextSteps = Sets.newHashSet();

    public String getTargetModelAlias() {
        val modelManager = NDataModelManager.getInstance(getConfig(), getProject());
        NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), getProject())
                .getDataModelDesc(targetSubject);
        if (dataModelDesc != null) {
            if (modelManager.isModelBroken(targetSubject)) {
                return modelManager.getDataModelDescWithoutInit(targetSubject).getAlias();
            } else {
                return dataModelDesc.getFusionModelAlias();
            }
        }

        return null;
    }

    public String getTargetModelId() {
        return getTargetModelId(getProject(), targetSubject);
    }

    public static String getTargetModelId(String project, String targetSubject) {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModelDesc = modelManager.getDataModelDesc(targetSubject);
        if (dataModelDesc == null)
            return null;
        return modelManager.isModelBroken(targetSubject)
                ? modelManager.getDataModelDescWithoutInit(targetSubject).getId()
                : dataModelDesc.getId();
    }

    public String getTargetSubjectAlias() {
        return getTargetModelAlias();
    }

    public AbstractExecutable() {
        setId(RandomUtil.randomUUIDStr());
    }

    public AbstractExecutable(Object notSetId) {
    }

    public void cancelJob() {
    }

    /**
     * jude it will cause segment-holes or not if discard this job
     * @return true by default
     */
    public boolean safetyIfDiscard() {
        return true;
    }

    protected KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected ExecutableManager getManager() {
        return getExecutableManager(project);
    }

    /**
     * for job steps, they need to update step status and throw exception
     * so they need to use wrapWithCheckQuit
     */
    protected void wrapWithCheckQuit(Callback f) throws JobStoppedException {
        boolean tryAgain = true;

        while (tryAgain) {
            checkNeedQuit(true);

            // in this short period user might changed job state, say restart
            // if a worker thread is unaware of this, it may go ahead and register step 1 as succeed here.
            // However the user expects a total RESTART

            tryAgain = false;
            try {
                JobContextUtil.withTxAndRetry(()->{
                    checkNeedQuit(false);
                    f.process();

                    return true;
                });
            } catch (Exception e) {
                if (Throwables.getCausalChain(e).stream().anyMatch(x -> x instanceof JobStoppedException)) {
                    // "in this short period user might change job state" happens
                    logger.info("[LESS_LIKELY_THINGS_HAPPENED] JobStoppedException thrown from in a UnitOfWork", e);
                    tryAgain = true;
                } else {
                    throw new JobStoppedException(e);
                }
            }
        }
    }

    protected void onExecuteStart() throws JobStoppedException {
        wrapWithCheckQuit(() -> {
            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
        });
    }

    protected void onExecuteFinished(ExecuteResult result) throws ExecuteException {
        logger.info("Execute finished {}, state:{}", this.getDisplayName(), result.state());
        MetricsGroup.hostTagCounterInc(MetricsName.JOB_STEP_ATTEMPTED, MetricsCategory.PROJECT, project, retry);
        if (result.succeed()) {
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.SUCCEED, result.getExtraInfo(), result.output(),
                        null);
            });
        } else if (result.skip()) {
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.SKIP, result.getExtraInfo(), result.output(), null);
            });
        } else {
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_FAILED_STEP_ATTEMPTED, MetricsCategory.PROJECT, project,
                    retry);
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.ERROR, result.getExtraInfo(), result.getErrorMsg(),
                        result.getShortErrMsg(), this::onExecuteErrorHook);
                killOtherPipelineApplicationOrUpdateOtherPipelineStepStatus();
            });
            throw new ExecuteException(result.getThrowable());
        }
    }

    public void onExecuteStopHook() {
        onExecuteErrorHook(getId());
    }

    protected void onExecuteErrorHook(String jobId) {
        // At present, only instance of DefaultExecutableOnModel take full advantage of this method.
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, Consumer<String> hook) {
        updateJobOutput(project, jobId, newStatus, info, output, null, hook);
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, String failedMsg, Consumer<String> hook) {
        updateJobOutput(project, jobId, newStatus, info, output, this.getLogPath(), failedMsg, hook);
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, String logPath, String failedMsg, Consumer<String> hook) {

        JobContextUtil.withTxAndRetry(() -> {
            ExecutableManager executableManager = getExecutableManager(project);
            val existedInfo = executableManager.getOutput(jobId).getExtra();
            if (info != null) {
                existedInfo.putAll(info);
            }

            //The output will be stored in HDFS,not in RS
            if (this instanceof ChainedStageExecutable) {
                if (newStatus == ExecutableState.SUCCEED) {
                    executableManager.makeStageSuccess(jobId);
                } else if (newStatus == ExecutableState.ERROR) {
                    executableManager.makeStageError(jobId);
                }
            }
            executableManager.updateJobOutput(jobId, newStatus, existedInfo, null, null, 0, failedMsg);
            if (hook != null) {
                hook.accept(jobId);
            }

            return true;
        });

        //write output to HDFS
        updateJobOutputToHDFS(project, jobId, output, logPath);
    }

    private static void updateJobOutputToHDFS(String project, String jobId, String output, String logPath) {
        ExecutableManager executableManager = getExecutableManager(project);
        ExecutableOutputPO jobOutput = executableManager.getJobOutput(jobId);
        if (null != output) {
            jobOutput.setContent(output);
        }
        if (null != logPath) {
            jobOutput.setLogPath(logPath);
        }
        String outputHDFSPath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId);

        executableManager.updateJobOutputToHDFS(outputHDFSPath, jobOutput);
    }

    protected static ExecutableManager getExecutableManager(String project) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public final ExecuteResult execute(JobContext jobContext) throws ExecuteException {
        logger.info("Executing AbstractExecutable {}", this.getDisplayName());
        this.context = jobContext;
        ExecuteResult result;

        onExecuteStart();

        do {
            if (retry > 0) {
                pauseOnRetry();
                logger.info("Retrying for the {}th time ", retry);
            }

            try {
                result = wrapWithExecuteException(() -> doWork(jobContext));
            } catch (JobStoppedException jse) {
                // job quits voluntarily or non-voluntarily, in this case, the job is "finished"
                // we createSucceed() to run onExecuteFinished()
                result = ExecuteResult.createSucceed();
            } catch (Exception e) {
                result = ExecuteResult.createError(e);
            }

            retry++;

        } while (needRetry(this.retry, result.getThrowable())); //exception in ExecuteResult should handle by user itself.
        //check exception in result to avoid retry on ChainedExecutable(only need retry on subtask actually)

        onExecuteFinished(result);
        return result;
    }

    protected void killOtherPipelineApplicationOrUpdateOtherPipelineStepStatus() {
        logger.error("{} kill other piper line application or update other piper line step status", getDisplayName());
        List<AbstractExecutable> otherPipelineRunningStep = getOtherPipelineRunningStep();
        otherPipelineRunningStep.forEach(AbstractExecutable::killApplicationIfExistsOrUpdateStepStatus);
    }

    protected List<AbstractExecutable> getOtherPipelineRunningStep() {
        val parent = getParent();
        val previousStepId = getPreviousStep();
        if (parent instanceof DefaultExecutable && parent.getJobSchedulerMode().equals(JobSchedulerModeEnum.DAG)) {
            val otherPipelineTasks = getOtherPipelineTasks((DefaultExecutable) parent, previousStepId);
            val dagExecutablesMap = ((DefaultExecutable) parent).getTasks().stream()
                    .collect(Collectors.toMap(AbstractExecutable::getId, task -> task));
            return otherPipelineTasks.stream()
                    .map(task -> getStepOrNextStepsWithStatus(task, dagExecutablesMap, ExecutableState.RUNNING))
                    .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
        }
        return Lists.newArrayList();
    }

    private List<AbstractExecutable> getOtherPipelineTasks(DefaultExecutable parent, String previousStepId) {
        return parent.getTasks().stream() //
                .filter(task -> StringUtils.equals(task.getPreviousStep(), previousStepId))
                .filter(task -> !task.getId().equals(getId())).collect(Collectors.toList());
    }

    protected List<AbstractExecutable> getStepOrNextStepsWithStatus(AbstractExecutable executable,
            Map<String, AbstractExecutable> dagExecutablesMap, ExecutableState state) {
        if (executable.getStatus().equals(state)) {
            return Lists.newArrayList(executable);
        }
        return executable.getNextSteps().stream().map(dagExecutablesMap::get)
                .map(step -> getStepOrNextStepsWithStatus(step, dagExecutablesMap, state))
                .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
    }

    /**
     * default UpdateStepStatus when other piper line step failed
     */
    public void killApplicationIfExistsOrUpdateStepStatus() {
        ExecutableManager executableManager = getExecutableManager(project);
        executableManager.updateJobOutput(getId(), ExecutableState.PAUSED, null, null, null, 0, null);
    }

    protected void checkNeedQuit(boolean applyChange) throws JobStoppedException {
        // non voluntarily
        abortIfJobStopped(applyChange);
    }

    /**
     * For non-chained executable, depend on its parent(instance of DefaultExecutable).
     */
    public boolean checkSuicide() {
        final AbstractExecutable parent = getParent();
        if (parent == null) {
            return false;
        } else {
            return parent.checkSuicide();
        }
    }

    // If job need check external status change, override this method, by default return true.
    protected boolean needCheckState() {
        return true;
    }

    /**
     * will throw exception if necessary!
     */
    public void abortIfJobStopped(boolean applyChange) throws JobStoppedException {
        if (!needCheckState()) {
            return;
        }

        Boolean aborted = JobContextUtil.withTxAndRetry(() -> {
            boolean abort = false;
            val parent = getParent();
            ExecutableState state = parent.getStatus();
            switch (state) {
                case READY:
                case PENDING:
                case PAUSED:
                case DISCARDED:
                    //if a job is restarted(all steps' status changed to READY) or paused or discarded, the old thread may still be alive and attempt to update job output
                    //in this case the old thread should fail itself by calling this
                    if (applyChange) {
                        logger.debug("abort {} because parent job is {}", getId(), state);
                        updateJobOutput(project, getId(), state, null, null, null);
                    }
                    abort = true;
                    break;
                default:
                    break;
            }

            return abort;
        });

        if (aborted) {
            throw new JobStoppedNonVoluntarilyException();
        }
    }

    // Retry will happen in below cases:
    // 1) if property "kylin.job.retry-exception-classes" is not set or is null, all jobs with exceptions will retry according to the retry times.
    // 2) if property "kylin.job.retry-exception-classes" is set and is not null, only jobs with the specified exceptions will retry according to the retry times.
    public boolean needRetry(int retry, Throwable t) {
        if (t == null || this instanceof DefaultExecutable)
            return false;

        if (retry > KylinConfig.getInstanceFromEnv().getJobRetry())
            return false;

        if (ThrowableUtils.isInterruptedException(t))
            return false;

        return isRetryableException(t.getClass().getName());
    }

    // pauseOnRetry should only works when retry has been triggered
    private void pauseOnRetry() {
        int interval = KylinConfig.getInstanceFromEnv().getJobRetryInterval();
        logger.info("Pause {} milliseconds before retry", interval);
        try {
            TimeUnit.MILLISECONDS.sleep(interval);
        } catch (InterruptedException e) {
            logger.error("Job retry was interrupted, details: {}", e);
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    protected abstract ExecuteResult doWork(JobContext context) throws ExecuteException;

    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.PENDING;
    }

    public String getDisplayName() {
        return this.name + " (" + this.id + ")";
    }

    @Override
    public final ExecutableState getStatus() {
        ExecutableManager manager = getManager();
        return manager.getOutput(this.getId()).getState();
    }

    // This status is recorded when executable is inited.
    // Use method 'getStatus' to get the last status.
    public final ExecutableState getStatusInMem() {
        return getStatus(getPo());
    }

    public final ExecutableState getStatus(ExecutablePO po) {
        ExecutableManager manager = getManager();
        return manager.getOutput(this.getId(), po).getState();
    }

    public final long getLastModified() {
        return getLastModified(getOutput());
    }

    public static long getLastModified(Output output) {
        return output.getLastModified();
    }

    public final long getByteSize() {
        return getByteSize(getOutput());
    }

    public static long getByteSize(Output output) {
        return output.getByteSize();
    }

    public void notifyUserIfNecessary(NDataLayout[] addOrUpdateCuboids) {
        boolean hasEmptyLayout = false;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            if (dataCuboid.getRows() == 0) {
                hasEmptyLayout = true;
                break;
            }
        }
        if (hasEmptyLayout) {
            notifyUserJobIssue(JobIssueEnum.LOAD_EMPTY_DATA);
        }
    }

    public final void notifyUserJobIssue(JobIssueEnum jobIssue) {
        Preconditions.checkState((this instanceof DefaultExecutable) || this.getParent() instanceof DefaultExecutable);
        val projectConfig = NProjectManager.getInstance(getConfig()).getProject(project).getConfig();
        boolean needNotification = true;
        switch (jobIssue) {
        case JOB_ERROR:
            needNotification = projectConfig.getJobErrorNotificationEnabled();
            break;
        case LOAD_EMPTY_DATA:
            needNotification = projectConfig.getJobDataLoadEmptyNotificationEnabled();
            break;
        case SOURCE_RECORDS_CHANGE:
            needNotification = projectConfig.getJobSourceRecordsChangeNotificationEnabled();
            break;
        default:
            throw new IllegalArgumentException(String.format(Locale.ROOT, "no process for jobIssue: %s.", jobIssue));
        }
        if (!needNotification) {
            return;
        }
        List<String> users;
        users = getAllNotifyUsers(projectConfig);
        if (this instanceof DefaultExecutable) {
            MailHelper.notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this), users);
        } else {
            MailHelper.notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this.getParent()),
                    users);
        }
    }

    public void setSparkYarnQueueIfEnabled(String project, String yarnQueue) {
        ProjectInstance proj = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        KylinConfig config = proj.getConfig();
        // TODO check if valid queue
        if (config.isSetYarnQueueInTaskEnabled() && config.getYarnQueueInTaskAvailable().contains(yarnQueue)) {
            this.setSparkYarnQueue(yarnQueue);
        }
    }

    public final AbstractExecutable getParent() {
        return getManager().getJob(getParam(PARENT_ID));
    }

    public final AbstractExecutable getParent(ExecutablePO po) {
        return getManager().getJob(getParam(PARENT_ID), po);
    }

    public void checkParentJobStatus() {
        if (!getParent().getStatus().equals(ExecutableState.RUNNING)) {
            throw new IllegalStateException("invalid parent job state, parent job:" + getParent().getDisplayName()
                    + ", state:" + getParent().getStatus());
        }
    }

    public final String getProject() {
        if (project == null) {
            throw new IllegalStateException("project is not set for abstract executable " + getId());
        }
        return project;
    }

    public final void setProject(String project) {
        this.project = project;
    }

    public final String getJobId() {
        return getId();
    }

    @Override
    public final Output getOutput() {
        return getManager().getOutput(getId());
    }

    public final Output getOutput(ExecutablePO executablePO) {
        return getManager().getOutput(getId(), executablePO);
    }

    //will modify input info
    public Map<String, String> makeExtraInfo(Map<String, String> info) {
        if (info == null) {
            return Maps.newHashMap();
        }

        // post process
        if (info.containsKey(MR_JOB_ID) && !info.containsKey(YARN_APP_ID)) {
            String jobId = info.get(MR_JOB_ID);
            if (jobId.startsWith("job_")) {
                info.put(YARN_APP_ID, jobId.replace("job_", "application_"));
            }
        }

        if (info.containsKey(YARN_APP_ID)
                && !org.apache.commons.lang3.StringUtils.isEmpty(getConfig().getJobTrackingURLPattern())) {
            String pattern = getConfig().getJobTrackingURLPattern();
            try {
                String newTrackingURL = String.format(Locale.ROOT, pattern, info.get(YARN_APP_ID));
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: {}", getConfig().getJobTrackingURLPattern());
            }
        }

        return info;
    }
    public final long getStartTime() {
        return getStartTime(getOutput());
    }

    public static long getStartTime(Output output) {
        return output.getStartTime();
    }

    public final long getEndTime() {
        return getEndTime(getOutput());
    }

    public static long getEndTime(Output output) {
        return output.getEndTime();
    }
    
    public final long getEndTime(ExecutablePO po) {
        return getEndTime(getOutput(po));
    }

    public final Map<String, String> getExtraInfo() {
        return getOutput().getExtra();
    }

    public final long getCreateTime() {
        return getManager().getCreateTime(getId());
    }

    public static long getCreateTime(Output output) {
        return output.getCreateTime();
    }

    // just using to get job duration in get job list
    public long getDurationFromStepOrStageDurationSum(ExecutablePO executablePO) {
        var duration = getDuration(executablePO);
        if (this instanceof DagExecutable && getJobSchedulerMode().equals(JobSchedulerModeEnum.DAG)) {
            duration = calculateDagExecutableDuration(executablePO);
        } else if (this instanceof ChainedExecutable) {
            duration = calculateChainedExecutableDuration(executablePO);
        }
        return duration;
    }

    private long calculateDagExecutableDuration(ExecutablePO executablePO) {
        val tasks = ((DagExecutable) this).getTasks();
        val tasksMap = tasks.stream().collect(Collectors.toMap(AbstractExecutable::getId, task -> task));
        return tasks.stream().filter(task -> StringUtils.isBlank(task.getPreviousStep()))
                .map(task -> calculateDagTaskExecutableDuration(task, executablePO, tasksMap)).max(Long::compare).orElse(0L);
    }

    private Long calculateDagTaskExecutableDuration(AbstractExecutable task, ExecutablePO executablePO,
            Map<String, ? extends AbstractExecutable> tasksMap) {
        Long nextTaskDurationMax = task.getNextSteps().stream().map(tasksMap::get)
                .map(nextTask -> calculateDagTaskExecutableDuration(nextTask, executablePO, tasksMap)).max(Long::compare).orElse(0L);
        return getTaskDuration(task, executablePO) + nextTaskDurationMax;
    }

    private long calculateChainedExecutableDuration(ExecutablePO executablePO) {
        val tasks = ((ChainedExecutable) this).getTasks();
        val jobAtomicDuration = new AtomicLong(0);
        tasks.forEach(task -> {
            long taskDuration = getTaskDuration(task, executablePO);
            jobAtomicDuration.addAndGet(taskDuration);
        });
        return jobAtomicDuration.get();
    }

    @VisibleForTesting
    public long getTaskDurationToTest(AbstractExecutable task, ExecutablePO executablePO) {
        return getTaskDuration(task, executablePO);
    }

    private long getTaskDuration(AbstractExecutable task, ExecutablePO executablePO) {
        var taskDuration = task.getDuration(executablePO);
        if (task instanceof ChainedStageExecutable) {
            taskDuration = calculateSingleSegmentStagesDuration((ChainedStageExecutable) task, executablePO, taskDuration);
        }
        return taskDuration;
    }

    private long calculateSingleSegmentStagesDuration(ChainedStageExecutable task, ExecutablePO executablePO, long taskDuration) {
        val stagesMap = task.getStagesMap();
        if (stagesMap.size() == 1) {
            for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                taskDuration = entry.getValue().stream().map(stage -> getDuration(stage.getOutput(entry.getKey(), executablePO))) //
                        .mapToLong(Long::valueOf) //
                        .sum();
            }
        }
        return taskDuration;
    }

    public long getDuration() {
        return getDuration(getOutput());
    }

    public long getDuration(ExecutablePO executablePO) {
        return getDuration(getOutput(executablePO));
    }

    public static long getDuration(Output output) {
        if (output.getDuration() != 0) {
            var duration = output.getDuration();
            if (ExecutableState.RUNNING == output.getState()) {
                duration = duration + System.currentTimeMillis() - output.getLastRunningStartTime();
            }
            return duration;
        }
        if (output.getStartTime() == 0) {
            return 0;
        }
        return output.getEndTime() == 0 ? System.currentTimeMillis() - output.getStartTime()
                : output.getEndTime() - output.getStartTime();
    }

    public long getWaitTime() {
        String jobId = ExecutableManager.extractJobId(getId());
        return getWaitTime(getManager().getExecutablePO(jobId));
    }

    public long getWaitTime(ExecutablePO po) {
        Output output = getOutput(po);
        long startTime = output.getStartTime();

        long lastTaskEndTime = output.getCreateTime();
        var lastTaskStatus = output.getState();

        int stepId = getStepId();

        // get end_time of last task
        if (getParent(po) instanceof DefaultExecutable) {
            val parentExecutable = (DefaultExecutable) getParent(po);
            val lastExecutable = parentExecutable.getSubTaskByStepId(stepId - 1);

            lastTaskEndTime = lastExecutable.map(e -> e.getEndTime(po))
                    .orElse(parentExecutable.getOutput(po).getCreateTime());

            lastTaskStatus = lastExecutable.map(e -> e.getStatus(po)).orElse(parentExecutable.getStatus(po));
        }

        //if last task is not end, wait_time is 0
        if (stepId > 0 && (lastTaskEndTime == 0 || lastTaskStatus != ExecutableState.SUCCEED)) {
            return 0;
        }

        if (startTime == 0) {
            if (getParent(po) != null && getParent(po).getStatus(po) == ExecutableState.DISCARDED) {
                // job is discarded before started
                startTime = getParent(po).getEndTime(po);
            } else {
                //the job/task is not started, use the current time
                startTime = System.currentTimeMillis();
            }
        }

        long waitTime = startTime - lastTaskEndTime;
        return waitTime < 0 ? 0 : waitTime;
    }

    public long getTotalDurationTime() {
        return getDuration() + getWaitTime();
    }

    public final Set<String> getDependentFiles() {
        val value = getExtraInfo().getOrDefault(DEPENDENT_FILES, "");
        if (StringUtils.isEmpty(value)) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(value.split(","));
    }

    /**
     * job get DISCARD or PAUSE without job fetcher's awareness
     *
     * SUICIDE is not the case, as it is awared by job fetcher
     *
     */
    protected final boolean isStoppedNonVoluntarily() {
        Preconditions.checkState(getParent() == null);
        final ExecutableState status = getOutput().getState();
        return status.isStoppedNonVoluntarily();
    }

    protected boolean needRetry() {
        return this.retry <= getConfig().getJobRetry();
    }

    public Set<String> getDependencies(KylinConfig config) {
        return Sets.newHashSet();
    }

    private static int computeTableAnalyzeMemory() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getSparkEngineDriverMemoryTableSampling();
    }

    private static int computeSnapshotAnalyzeMemory() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getSparkEngineDriverMemorySnapshotBuilding();
    }

    public int computeStepDriverMemory() {
        if (getJobType() == JobTypeEnum.TABLE_SAMPLING) {
            return computeTableAnalyzeMemory();
        }

        if (getJobType() == JobTypeEnum.SNAPSHOT_BUILD || getJobType() == JobTypeEnum.SNAPSHOT_REFRESH) {
            return computeSnapshotAnalyzeMemory();
        }

        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (layouts != null) {
            return computeDriverMemory(StringHelper.splitAndTrim(layouts, ",").length);
        }
        return 0;
    }

    public static Integer computeDriverMemory(Integer cuboidNum) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] driverMemoryStrategy = config.getSparkEngineDriverMemoryStrategy();
        List<Integer> strategy = Lists.newArrayList(cuboidNum);
        Arrays.stream(driverMemoryStrategy).forEach(strategy::add);
        Collections.sort(strategy);
        int index = strategy.indexOf(cuboidNum);
        int driverMemoryMaximum = config.getSparkEngineDriverMemoryMaximum();
        int driverMemoryBase = config.getSparkEngineDriverMemoryBase();

        driverMemoryBase += driverMemoryBase * index;
        return Math.min(driverMemoryBase, driverMemoryMaximum);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }

    public <T> T wrapWithExecuteException(final Callable<T> lambda) throws ExecuteException {
        Exception exception = null;
        try {
            return lambda.call();
        } catch (ExecuteException e) {
            exception = e;
            throw e;
        } catch (Exception e) {
            exception = e;
            throw new ExecuteException(e);
        } finally {
            if (exception != null && !(exception instanceof JobStoppedNonVoluntarilyException)) {
                wrapWithExecuteExceptionUpdateJobError(exception);
            }
        }
    }

    protected void wrapWithExecuteExceptionUpdateJobError(Exception exception) {
        JobContextUtil.withTxAndRetry(() -> {
            getExecutableManager(project).updateJobError(getId(), getId(), null, ExceptionUtils.getStackTrace(exception),
                    exception.getMessage());

            return true;
        });
    }
}
