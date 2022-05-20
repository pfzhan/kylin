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

package io.kyligence.kap.job.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ACTION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionReason;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.JobExceptionReason;
import org.apache.kylin.common.exception.JobExceptionResolve;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobDiscardNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.execution.ChainedStageExecutable;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.ShellExecutable;
import io.kyligence.kap.job.execution.stage.StageBase;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.rest.ExecutableResponse;
import io.kyligence.kap.job.rest.ExecutableStepResponse;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.util.JobInfoUtil;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.util.SparkHistoryUIUtil;
import lombok.val;
import lombok.var;

@Service
public class JobInfoService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(JobInfoService.class);

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);

    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";
    public static final String EXCEPTION_CODE_PATH = "exception_to_code.json";
    public static final String EXCEPTION_CODE_DEFAULT = "KE-030001000";

    @Autowired
    private ProjectService projectService;

    @Autowired
    private JobInfoDao jobInfoDao;

    private AclEvaluate aclEvaluate;

    @Autowired(required = false)
    private ModelMetadataInvoker modelMetadataInvoker;

    @Autowired
    public JobInfoService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    public void checkJobStatus(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        jobStatuses.forEach(this::checkJobStatus);
    }

    public void checkJobStatus(String jobStatus) {
        if (Objects.isNull(JobStatusEnum.getByName(jobStatus))) {
            throw new KylinException(JOB_STATUS_ILLEGAL);
        }
    }

    public void checkJobStatusAndAction(String jobStatus, String action) {
        checkJobStatus(jobStatus);
        JobActionEnum.validateValue(action);
        JobStatusEnum jobStatusEnum = JobStatusEnum.valueOf(jobStatus);
        if (!jobStatusEnum.checkAction(JobActionEnum.valueOf(action))) {
            throw new KylinException(JOB_ACTION_ILLEGAL, jobStatus, jobStatusEnum.getValidActions());
        }

    }

    public void checkJobStatusAndAction(JobUpdateRequest jobUpdateRequest) {
        List<String> jobIds = jobUpdateRequest.getJobIds();
        List<String> jobStatuses = jobUpdateRequest.getStatuses() == null ? Lists.newArrayList()
                : jobUpdateRequest.getStatuses();
        jobIds.stream().map(this::getJobInstance).map(ExecutableResponse::getStatus).map(JobStatusEnum::toString)
                .forEach(jobStatuses::add);
        checkJobStatusAndAction(jobStatuses, jobUpdateRequest.getAction());
    }

    private void checkJobStatusAndAction(List<String> jobStatuses, String action) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        for (String jobStatus : jobStatuses) {
            checkJobStatusAndAction(jobStatus, action);
        }
    }

    /**
     * for 3x api
     *
     * @param jobId
     * @return
     */
    public ExecutableResponse getJobInstance(String jobId) {
        Preconditions.checkNotNull(jobId);
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        Preconditions.checkNotNull(executablePO, "Can not find the job: {}", jobId);
        ExecutableManager executableManager = getManager(ExecutableManager.class, executablePO.getProject());
        AbstractExecutable executable = executableManager.fromPO(executablePO);
        return convert(executable);
    }

    @VisibleForTesting
    public List<ProjectInstance> getReadableProjects() {
        return projectService.getReadableProjects(null, false);
    }

    // TODO model == null || !model.isFusionModel();
    // TODO query TABLE_SAMPLING, SNAPSHOT_BUILD, SNAPSHOT_REFRESH, SECOND_STORAGE_NODE_CLEAN by 'subject' (JobUtil.deduceTargetSubject)
    public DataResult<List<ExecutableResponse>> listJobs(final JobFilter jobFilter, int offset, int limit) {
        // TODO check permission when 'project' is empty
        if (StringUtils.isNotEmpty(jobFilter.getProject())) {
            aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        }
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobFilter, offset, limit);
        List<ExecutableResponse> result = jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo)).map(
                executablePO -> getManager(ExecutableManager.class, executablePO.getProject()).fromPO(executablePO))
                .map(this::convert).collect(Collectors.toList());
        return new DataResult<>(result, result.size());
    }

    public List<ExecutableStepResponse> getJobDetail(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        if (executablePO == null) {
            throw new KylinException(JOB_NOT_EXIST, jobId);
        }
        AbstractExecutable executable = null;
        ExecutableManager executableManager = getManager(ExecutableManager.class, project);
        try {
            executable = executableManager.fromPO(executablePO);
        } catch (Exception e) {
            logger.error(PARSE_ERROR_MSG, e);
            return null;
        }

        // waite time in output
        Map<String, String> waiteTimeMap;
        val output = executable.getOutput();
        try {
            waiteTimeMap = JsonUtil.readValueAsMap(output.getExtra().getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            waiteTimeMap = Maps.newHashMap();
        }
        final String targetSubject = executable.getTargetSubject();
        List<ExecutableStepResponse> executableStepList = new ArrayList<>();
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
        for (AbstractExecutable task : tasks) {
            final ExecutableStepResponse executableStepResponse = parseToExecutableStep(task,
                    executableManager.getOutput(task.getId(), executablePO), waiteTimeMap,
                    output.getState());
            if (task.getStatus() == ExecutableState.ERROR) {
                executableStepResponse.setFailedStepId(output.getFailedStepId());
                executableStepResponse.setFailedSegmentId(output.getFailedSegmentId());
                executableStepResponse.setFailedStack(output.getFailedStack());
                executableStepResponse.setFailedStepName(task.getName());

                setExceptionResolveAndCodeAndReason(output, executableStepResponse);
            }
            if (task instanceof ChainedStageExecutable) {
                Map<String, List<StageBase>> stagesMap = Optional.ofNullable(((NSparkExecutable) task).getStagesMap())
                        .orElse(Maps.newHashMap());

                Map<String, ExecutableStepResponse.SubStages> stringSubStageMap = Maps.newHashMap();
                List<ExecutableStepResponse> subStages = Lists.newArrayList();

                for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                    String segmentId = entry.getKey();
                    ExecutableStepResponse.SubStages segmentSubStages = new ExecutableStepResponse.SubStages();

                    List<StageBase> stageBases = Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList());
                    List<ExecutableStepResponse> stageResponses = Lists.newArrayList();
                    for (StageBase stage : stageBases) {
                        val stageResponse = parseStageToExecutableStep(task, stage,
                                executableManager.getOutput(stage.getId(), executablePO, segmentId));
                        setStage(subStages, stageResponse);
                        stageResponses.add(stageResponse);

                        if (StringUtils.equals(output.getFailedStepId(), stage.getId())) {
                            executableStepResponse.setFailedStepName(stage.getName());
                        }
                    }

                    // table sampling and snapshot table don't have some segment
                    if (!StringUtils.equals(task.getId(), segmentId)) {
                        setSegmentSubStageParams(project, targetSubject, task, segmentId, segmentSubStages, stageBases,
                                stageResponses, waiteTimeMap, output.getState());
                        stringSubStageMap.put(segmentId, segmentSubStages);
                    }
                }
                if (MapUtils.isNotEmpty(stringSubStageMap)) {
                    executableStepResponse.setSegmentSubStages(stringSubStageMap);
                }
                if (CollectionUtils.isNotEmpty(subStages)) {
                    executableStepResponse.setSubStages(subStages);
                    if (MapUtils.isEmpty(stringSubStageMap) || stringSubStageMap.size() == 1) {
                        val taskDuration = subStages.stream() //
                                .map(ExecutableStepResponse::getDuration) //
                                .mapToLong(Long::valueOf).sum();
                        executableStepResponse.setDuration(taskDuration);

                    }
                }
            }
            executableStepList.add(executableStepResponse);
        }
        if (executable.getStatus() == ExecutableState.DISCARDED) {
            executableStepList.forEach(executableStepResponse -> {
                executableStepResponse.setStatus(JobStatusEnum.DISCARDED);
                Optional.ofNullable(executableStepResponse.getSubStages()).orElse(Lists.newArrayList())
                        .forEach(subtask -> subtask.setStatus(JobStatusEnum.DISCARDED));
                val subStageMap = //
                        Optional.ofNullable(executableStepResponse.getSegmentSubStages()).orElse(Maps.newHashMap());
                for (Map.Entry<String, ExecutableStepResponse.SubStages> entry : subStageMap.entrySet()) {
                    entry.getValue().getStage().forEach(stage -> stage.setStatus(JobStatusEnum.DISCARDED));
                }
            });
        }
        return executableStepList;
    }

    @Transaction(project = 0)
    public void batchDropJob(String project, List<String> jobIds, List<String> filterStatuses) {
        aclEvaluate.checkProjectOperationPermission(project);
        batchDropJob0(project, jobIds, filterStatuses);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchDropGlobalJob(List<String> jobIds, List<String> filterStatuses) {
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            batchDropJob0(project, jobIds, filterStatuses);
        }
    }

    private void batchDropJob0(String project, List<String> jobIds, List<String> filterStatuses) {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);

        ExecutableManager executableManager = getManager(ExecutableManager.class, project);
        jobs.forEach(job -> executableManager.checkJobCanBeDeleted(job));

        jobs.forEach(job -> jobInfoDao.dropJob(job.getId()));
    }

    @Transaction(project = 1)
    public void batchUpdateJobStatus(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
    }

    public void batchUpdateGlobalJobStatus(List<String> jobIds, String action, List<String> filterStatuses) throws IOException {
        logger.info("Owned projects is {}", projectService.getOwnedProjects());
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
        }
    }

    private void batchUpdateJobStatus0(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        val executablePos = jobInfoDao.getExecutablePoByStatus(project, jobIds, filterStatuses);
        for (ExecutablePO executablePO : executablePos) {
            updateJobStatus(executablePO.getId(), executablePO, project, action);
        }
    }

    @VisibleForTesting
    public void updateJobStatus(String jobId, ExecutablePO executablePO, String project, String action) throws IOException {
        val executableManager = getManager(ExecutableManager.class, project);
        AbstractExecutable executable = executableManager.fromPO(executablePO);
        UnitOfWorkContext.UnitTask afterUnitTask = () -> EventBusFactory.getInstance()
                .postWithLimit(new JobReadyNotifier(project));
        JobActionEnum.validateValue(action.toUpperCase(Locale.ROOT));
        switch (JobActionEnum.valueOf(action.toUpperCase(Locale.ROOT))) {
            case RESUME:
                // TODO SecondStorage
//                SecondStorageUtil.checkJobResume(project, jobId, executable, executablePO);
                executableManager.updateJobError(jobId, null, null, null, null);
                executableManager.resumeJob(jobId, executable);
                UnitOfWork.get().doAfterUnit(afterUnitTask);
                MetricsGroup.hostTagCounterInc(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, project);
                break;
            case RESTART:
//                SecondStorageUtil.checkJobRestart(project, jobId, executable);
                killExistApplication(executable);
                executableManager.updateJobError(jobId, null, null, null, null);
                executableManager.restartJob(jobId, executable);
                UnitOfWork.get().doAfterUnit(afterUnitTask);
                break;
            case DISCARD:
                discardJob(project, jobId, executable);
                JobTypeEnum jobTypeEnum = executable.getJobType();
                String jobType = jobTypeEnum == null ? "" : jobTypeEnum.name();
                UnitOfWork.get().doAfterUnit(
                        () -> EventBusFactory.getInstance().postAsync(new JobDiscardNotifier(project, jobType)));
                break;
            case PAUSE:
                killExistApplication(executable);
                executableManager.pauseJob(jobId, executablePO, executable);
                break;
            default:
                throw new IllegalStateException("This job can not do this action: " + action);
        }

    }

    public void updateJobError(String project, String jobId, String failedStepId, String failedSegmentId,
                               String failedStack, String failedReason) {
        if (StringUtils.isBlank(failedStepId)) {
            return;
        }
        val executableManager = getManager(ExecutableManager.class, project);
        executableManager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);
    }

    public void updateStageStatus(String project, String taskId, String segmentId, String status,
                                  Map<String, String> updateInfo, String errMsg) {
        final ExecutableState newStatus = convertToExecutableState(status);
        val executableManager = getManager(ExecutableManager.class, project);
        executableManager.updateStageStatus(taskId, segmentId, newStatus, updateInfo, errMsg);
    }

    public ExecutableState convertToExecutableState(String status) {
        if (StringUtils.isBlank(status)) {
            return null;
        }
        return ExecutableState.valueOf(status);
    }

    /**
     * update the spark job info, such as yarnAppId, yarnAppUrl.
     *
     * @param project
     * @param jobId
     * @param taskId
     * @param yarnAppId
     * @param yarnAppUrl
     */
    public void updateSparkJobInfo(String project, String jobId, String taskId, String yarnAppId, String yarnAppUrl) {
        val executableManager = getManager(ExecutableManager.class, project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_ID, yarnAppId);
        extraInfo.put(ExecutableConstants.YARN_APP_URL, yarnAppUrl);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }

    public void updateSparkTimeInfo(String project, String jobId, String taskId, String waitTime, String buildTime) {
        val executableManager = getManager(ExecutableManager.class, project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_JOB_WAIT_TIME, waitTime);
        extraInfo.put(ExecutableConstants.YARN_JOB_RUN_TIME, buildTime);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }

    public String getJobOutput(String project, String jobId) {
        return getJobOutput(project, jobId, jobId);
    }

    public String getJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(ExecutableManager.class, project);
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId).getVerboseMsg();
    }

    public void killExistApplication(AbstractExecutable job) {
        if (job instanceof ChainedExecutable) {
            // if job's task is running spark job, will kill this application
            ((ChainedExecutable) job).getTasks().stream() //
                    .filter(task -> task.getStatus() == ExecutableState.RUNNING) //
                    .filter(task -> task instanceof NSparkExecutable) //
                    .forEach(task -> ((NSparkExecutable) task).killOrphanApplicationIfExists(task.getId()));
        }
    }

    private void discardJob(String project, String jobId, AbstractExecutable job) {
        if (ExecutableState.SUCCEED == job.getStatus()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "DISCARD", jobId, job.getStatus());
        }
        if (ExecutableState.DISCARDED == job.getStatus()) {
            return;
        }
        killExistApplication(job);
        getManager(ExecutableManager.class, project).discardJob(job.getId(), job);
    }

    private List<AbstractExecutable> getJobsByStatus(String project, List<String> jobIds, List<String> filterStatuses) {
        return jobInfoDao.getExecutablePoByStatus(project, jobIds, filterStatuses).stream()
                .map(executablePO -> getManager(ExecutableManager.class, executablePO.getProject()).fromPO(executablePO))
                .collect(Collectors.toList());
    }

    private ExecutableResponse convert(AbstractExecutable executable) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    @VisibleForTesting
    public ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, Output stepOutput,
            Map<String, String> waiteTimeMap, ExecutableState jobState) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(task.getStepId());

        if (stepOutput == null) {
            logger.warn("Cannot found output for task: id={}", task.getId());
            return result;
        }

        result.setStatus(stepOutput.getState().toJobStatus());
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        if (KylinConfig.getInstanceFromEnv().isHistoryServerEnable()
                && result.getInfo().containsKey(ExecutableConstants.YARN_APP_ID)) {
            result.putInfo(ExecutableConstants.SPARK_HISTORY_APP_URL,
                    SparkHistoryUIUtil.getHistoryTrackerUrl(result.getInfo().get(ExecutableConstants.YARN_APP_ID)));
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stepOutput));

        result.setDuration(AbstractExecutable.getDuration(stepOutput));
        // if resume job, need sum of waite time
        long waiteTime = Long.parseLong(waiteTimeMap.getOrDefault(task.getId(), "0"));
        if (jobState != ExecutableState.PAUSED) {
            val taskWaitTime = task.getWaitTime();
            // Refactoring: When task Wait Time is equal to waite Time, waiteTimeMap saves the latest waiting time
            if (taskWaitTime != waiteTime) {
                waiteTime = taskWaitTime + waiteTime;
            }
        }
        result.setWaitTime(waiteTime);

        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        result.setShortErrMsg(stepOutput.getShortErrMsg());
        return result;
    }

    public void setExceptionResolveAndCodeAndReason(Output output, ExecutableStepResponse executableStepResponse) {
        try {
            val exceptionCode = getExceptionCode(output);
            executableStepResponse.setFailedResolve(ExceptionResolve.getResolve(exceptionCode));
            executableStepResponse.setFailedCode(ErrorCode.getLocalizedString(exceptionCode));
            if (StringUtils.equals(exceptionCode, EXCEPTION_CODE_DEFAULT)) {
                val reason = StringUtils.isBlank(output.getFailedReason())
                        ? JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason()
                        : JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": "
                                + output.getFailedReason();
                executableStepResponse.setFailedReason(reason);
            } else {
                executableStepResponse.setFailedReason(ExceptionReason.getReason(exceptionCode));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            executableStepResponse
                    .setFailedResolve(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve());
            executableStepResponse.setFailedCode(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString());
            executableStepResponse
                    .setFailedReason(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason());
        }
    }

    public String getExceptionCode(Output output) {
        try {
            var exceptionOrExceptionMessage = output.getFailedReason();

            if (StringUtils.isBlank(exceptionOrExceptionMessage)) {
                if (StringUtils.isBlank(output.getFailedStack())) {
                    return EXCEPTION_CODE_DEFAULT;
                }
                exceptionOrExceptionMessage = output.getFailedStack().split("\n")[0];
            }

            val exceptionCodeStream = getClass().getClassLoader().getResource(EXCEPTION_CODE_PATH).openStream();
            val exceptionCodes = JsonUtil.readValue(exceptionCodeStream, Map.class);
            for (Object o : exceptionCodes.entrySet()) {
                val exceptionCode = (Map.Entry) o;
                if (StringUtils.contains(exceptionOrExceptionMessage, String.valueOf(exceptionCode.getKey()))
                        || StringUtils.contains(String.valueOf(exceptionCode.getKey()), exceptionOrExceptionMessage)) {
                    val code = exceptionCodes.getOrDefault(exceptionCode.getKey(), EXCEPTION_CODE_DEFAULT);
                    return String.valueOf(code);
                }
            }
            return EXCEPTION_CODE_DEFAULT;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return EXCEPTION_CODE_DEFAULT;
        }
    }

    private ExecutableStepResponse parseStageToExecutableStep(AbstractExecutable task, StageBase stageBase,
            Output stageOutput) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(stageBase.getId());
        result.setName(stageBase.getName());
        result.setSequenceID(stageBase.getStepId());

        if (stageOutput == null) {
            logger.warn("Cannot found output for task: id={}", stageBase.getId());
            return result;
        }
        for (Map.Entry<String, String> entry : stageOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setStatus(stageOutput.getState().toJobStatus());
        result.setExecStartTime(AbstractExecutable.getStartTime(stageOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stageOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stageOutput));

        result.setDuration(AbstractExecutable.getDuration(stageOutput));

        String indexCount = Optional.ofNullable(task.getParam(NBatchConstants.P_INDEX_COUNT)).orElse("0");
        result.setIndexCount(Long.parseLong(indexCount));
        if (result.getStatus() == JobStatusEnum.FINISHED) {
            result.setSuccessIndexCount(Long.parseLong(indexCount));
        } else {
            val successIndexCount = stageOutput.getExtra().getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
            result.setSuccessIndexCount(Long.parseLong(successIndexCount));
        }
        return result;
    }

    private void setStage(List<ExecutableStepResponse> responses, ExecutableStepResponse newResponse) {
        final ExecutableStepResponse oldResponse = responses.stream()
                .filter(response -> response.getId().equals(newResponse.getId()))//
                .findFirst().orElse(null);
        if (null != oldResponse) {
            /*
             * As long as there is a task executing, the step of this step is executing;
             * when all Segments are completed, the status of this step is changed to complete.
             *
             * if one segment is skip, other segment is success, the status of this step is success
             */
            Set<JobStatusEnum> jobStatusEnums = Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED,
                    JobStatusEnum.DISCARDED);
            Set<JobStatusEnum> jobFinishOrSkip = Sets.newHashSet(JobStatusEnum.FINISHED, JobStatusEnum.SKIP);
            if (oldResponse.getStatus() != newResponse.getStatus()
                    && !jobStatusEnums.contains(oldResponse.getStatus())) {
                if (jobStatusEnums.contains(newResponse.getStatus())) {
                    oldResponse.setStatus(newResponse.getStatus());
                } else if (jobFinishOrSkip.contains(newResponse.getStatus())
                        && jobFinishOrSkip.contains(oldResponse.getStatus())) {
                    oldResponse.setStatus(JobStatusEnum.FINISHED);
                } else {
                    oldResponse.setStatus(JobStatusEnum.RUNNING);
                }
            }

            if (newResponse.getExecStartTime() != 0) {
                oldResponse.setExecStartTime(Math.min(newResponse.getExecStartTime(), oldResponse.getExecStartTime()));
            }
            oldResponse.setExecEndTime(Math.max(newResponse.getExecEndTime(), oldResponse.getExecEndTime()));

            val successIndex = oldResponse.getSuccessIndexCount() + newResponse.getSuccessIndexCount();
            oldResponse.setSuccessIndexCount(successIndex);
            val index = oldResponse.getIndexCount() + newResponse.getIndexCount();
            oldResponse.setIndexCount(index);
        } else {
            ExecutableStepResponse res = new ExecutableStepResponse();
            res.setId(newResponse.getId());
            res.setName(newResponse.getName());
            res.setSequenceID(newResponse.getSequenceID());
            res.setExecStartTime(newResponse.getExecStartTime());
            res.setExecEndTime(newResponse.getExecEndTime());
            res.setDuration(newResponse.getDuration());
            res.setWaitTime(newResponse.getWaitTime());
            res.setIndexCount(newResponse.getIndexCount());
            res.setSuccessIndexCount(newResponse.getSuccessIndexCount());
            res.setStatus(newResponse.getStatus());
            res.setCmdType(newResponse.getCmdType());
            responses.add(res);
        }
    }

    private void setSegmentSubStageParams(String project, String targetSubject, AbstractExecutable task,
            String segmentId, ExecutableStepResponse.SubStages segmentSubStages, List<StageBase> stageBases,
            List<ExecutableStepResponse> stageResponses, Map<String, String> waiteTimeMap, ExecutableState jobState) {
        segmentSubStages.setStage(stageResponses);

        // when job restart, taskStartTime is zero
        if (CollectionUtils.isNotEmpty(stageResponses)) {
            val taskStartTime = task.getStartTime();
            var firstStageStartTime = stageResponses.get(0).getExecStartTime();
            if (taskStartTime != 0 && firstStageStartTime == 0) {
                firstStageStartTime = System.currentTimeMillis();
            }
            long waitTime = Long.parseLong(waiteTimeMap.getOrDefault(segmentId, "0"));
            if (jobState != ExecutableState.PAUSED) {
                waitTime = firstStageStartTime - taskStartTime + waitTime;
            }
            segmentSubStages.setWaitTime(waitTime);
        }

        val execStartTime = stageResponses.stream()//
                .filter(ex -> ex.getStatus() != JobStatusEnum.PENDING)//
                .map(ExecutableStepResponse::getExecStartTime)//
                .min(Long::compare).orElse(0L);
        segmentSubStages.setExecStartTime(execStartTime);

        // If this segment has running stage, this segment is running, this segment doesn't have end time
        // If this task is running and this segment has pending stage, this segment is running, this segment doesn't have end time
        val stageStatuses = stageResponses.stream().map(ExecutableStepResponse::getStatus).collect(Collectors.toSet());
        if (!stageStatuses.contains(JobStatusEnum.RUNNING)
                && !(task.getStatus() == ExecutableState.RUNNING && stageStatuses.contains(JobStatusEnum.PENDING))) {
            val execEndTime = stageResponses.stream()//
                    .map(ExecutableStepResponse::getExecEndTime)//
                    .max(Long::compare).orElse(0L);
            segmentSubStages.setExecEndTime(execEndTime);
        }

        val segmentDuration = stageResponses.stream() //
                .map(ExecutableStepResponse::getDuration) //
                .mapToLong(Long::valueOf).sum();
        segmentSubStages.setDuration(segmentDuration);

        final Segments<NDataSegment> segmentsByRange = modelMetadataInvoker.getSegmentsByRange(targetSubject, project,
                "", "");
        final NDataSegment segment = segmentsByRange.stream()//
                .filter(seg -> StringUtils.equals(seg.getId(), segmentId))//
                .findFirst().orElse(null);
        if (null != segment) {
            val segRange = segment.getSegRange();
            segmentSubStages.setName(segment.getName());
            segmentSubStages.setStartTime(Long.parseLong(segRange.getStart().toString()));
            segmentSubStages.setEndTime(Long.parseLong(segRange.getEnd().toString()));
        }

        /*
         * In the segment details, the progress formula of each segment
         *
         * CurrentProgress = numberOfStepsCompleted / totalNumberOfSteps，Accurate to single digit percentage。
         * This step only retains the steps in the parallel part of the Segment，
         * Does not contain other public steps, such as detection resources, etc.。
         *
         * Among them, the progress of the "BUILD_LAYER"
         *   step = numberOfCompletedIndexes / totalNumberOfIndexesToBeConstructed,
         * the progress of other steps will not be refined
         */
        val stepCount = stageResponses.size() == 0 ? 1 : stageResponses.size();
        val stepRatio = (float) ExecutableResponse.calculateSuccessStage(task, segmentId,
                stageBases, true) / stepCount;
        segmentSubStages.setStepRatio(stepRatio);
    }

    @SneakyThrows
    public InputStream getAllJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(ExecutableManager.class, project);
        val output = executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE);
        return Optional.ofNullable(output.getVerboseMsgStream()).orElse(
                IOUtils.toInputStream(Optional.ofNullable(output.getVerboseMsg()).orElse(StringUtils.EMPTY), "UTF-8"));
    }
}
