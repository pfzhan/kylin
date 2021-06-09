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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_JOB_STATUS;
import static org.apache.kylin.common.exception.ServerErrorCode.ILLEGAL_JOB_ACTION;
import static org.apache.kylin.common.exception.ServerErrorCode.ILLEGAL_JOB_STATUS;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kyligence.kap.common.scheduler.JobDiscardNotifier;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableSortBean;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("jobService")
public class JobService extends BasicService {

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    private static final Logger logger = LoggerFactory.getLogger("schedule");

    private static final Map<String, String> jobTypeMap = Maps.newHashMap();

    static {
        jobTypeMap.put("INDEX_REFRESH", "Refresh Data");
        jobTypeMap.put("INDEX_MERGE", "Merge Data");
        jobTypeMap.put("INDEX_BUILD", "Build Index");
        jobTypeMap.put("INC_BUILD", "Load Data");
        jobTypeMap.put("TABLE_SAMPLING", "Sample Table");
    }

    @VisibleForTesting
    public ExecutableResponse convert(AbstractExecutable executable) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    private DataResult<List<ExecutableResponse>> filterAndSort(final JobFilter jobFilter, List<AbstractExecutable> jobs,
            int offset, int limit) {
        Preconditions.checkNotNull(jobFilter);
        Preconditions.checkNotNull(jobs);

        Comparator<ExecutableSortBean> comparator = propertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? "last_modified" : jobFilter.getSortBy(),
                !jobFilter.isReverse());
        Set<JobStatusEnum> matchedJobStatusEnums = jobFilter.getStatuses().stream().map(JobStatusEnum::valueOf)
                .collect(Collectors.toSet());
        Set<ExecutableState> matchedExecutableStates = matchedJobStatusEnums.stream().map(this::parseToExecutableState)
                .collect(Collectors.toSet());
        List<ExecutableSortBean> sortBeanList = jobs.stream()
                .filter(((Predicate<AbstractExecutable>) (abstractExecutable -> {
                    if (CollectionUtils.isEmpty(jobFilter.getStatuses())) {
                        return true;
                    }
                    ExecutableState state = abstractExecutable.getStatus();
                    return matchedExecutableStates.contains(state)
                            || matchedJobStatusEnums.contains(state.toJobStatus());
                })).and(abstractExecutable -> {
                    String subject = StringUtils.trim(jobFilter.getKey());
                    if (StringUtils.isEmpty(subject)) {
                        return true;
                    }
                    return StringUtils.containsIgnoreCase(abstractExecutable.getTargetSubjectAlias(), subject)
                            || StringUtils.containsIgnoreCase(abstractExecutable.getId(), subject);
                }).and(abstractExecutable -> {
                    List<String> jobNames = jobFilter.getJobNames();
                    if (CollectionUtils.isEmpty(jobNames)) {
                        return true;
                    }
                    return jobNames.contains(abstractExecutable.getName());
                }).and(abstractExecutable -> {
                    String subject = jobFilter.getSubject();
                    if (StringUtils.isEmpty(subject)) {
                        return true;
                    }
                    //if filter on uuid, then it must be accurate
                    return abstractExecutable.getTargetSubject().equals(jobFilter.getSubject().trim());
                })).map(ExecutableSortBean::create).sorted(comparator).collect(Collectors.toList());

        List<ExecutableResponse> result = PagingUtil.cutPage(sortBeanList, offset, limit).stream()
                .map(ExecutableSortBean::getExecutable).map(this::convert).collect(Collectors.toList());
        return new DataResult<>(result, sortBeanList.size(), offset, limit);
    }

    private List<ExecutableResponse> filterAndSort(final JobFilter jobFilter, List<AbstractExecutable> jobs) {
        Preconditions.checkNotNull(jobFilter);
        Preconditions.checkNotNull(jobs);

        Comparator<ExecutableResponse> comparator = propertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? "last_modified" : jobFilter.getSortBy(),
                !jobFilter.isReverse());
        Set<JobStatusEnum> matchedJobStatusEnums = jobFilter.getStatuses().stream().map(JobStatusEnum::valueOf)
                .collect(Collectors.toSet());
        Set<ExecutableState> matchedExecutableStates = matchedJobStatusEnums.stream().map(this::parseToExecutableState)
                .collect(Collectors.toSet());
        return jobs.stream().filter(((Predicate<AbstractExecutable>) (abstractExecutable -> {
            if (CollectionUtils.isEmpty(jobFilter.getStatuses())) {
                return true;
            }
            ExecutableState state = abstractExecutable.getStatus();
            return matchedExecutableStates.contains(state) || matchedJobStatusEnums.contains(state.toJobStatus());
        })).and(abstractExecutable -> {
            String subject = jobFilter.getKey();
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            return StringUtils.containsIgnoreCase(abstractExecutable.getTargetSubjectAlias(), subject)
                || StringUtils.containsIgnoreCase(abstractExecutable.getId(), subject);
        }).and(abstractExecutable -> {
            List<String> jobNames = jobFilter.getJobNames();
            if (CollectionUtils.isEmpty(jobNames)) {
                return true;
            }
            return jobNames.contains(abstractExecutable.getName());
        }).and(abstractExecutable -> {
            String subject = jobFilter.getSubject();
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            //if filter on uuid, then it must be accurate
            return abstractExecutable.getTargetSubject().equals(jobFilter.getSubject().trim());
        })).map(this::convert).sorted(comparator).collect(Collectors.toList());
    }

    private List<AbstractExecutable> listJobs0(final JobFilter jobFilter) {
        JobTimeFilterEnum filterEnum = JobTimeFilterEnum.getByCode(jobFilter.getTimeFilter());
        Preconditions.checkNotNull(filterEnum, "Can not find the JobTimeFilterEnum by code: %s",
                jobFilter.getTimeFilter());

        NExecutableManager executableManager = getExecutableManager(jobFilter.getProject());
        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, filterEnum);
        long timeEndInMillis = Long.MAX_VALUE;
        return executableManager.getAllExecutables(timeStartInMillis, timeEndInMillis);
    }

    public List<ExecutableResponse> listJobs(final JobFilter jobFilter) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listJobs0(jobFilter));
    }

    public DataResult<List<ExecutableResponse>> listJobs(final JobFilter jobFilter, int offset, int limit) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listJobs0(jobFilter), offset, limit);
    }

    public List<ExecutableResponse> addOldParams(List<ExecutableResponse> executableResponseList) {
        executableResponseList.forEach(executableResponse -> {
            ExecutableResponse.OldParams oldParams = new ExecutableResponse.OldParams();
            NDataModel nDataModel = modelService.getDataModelManager(executableResponse.getProject())
                    .getDataModelDesc(executableResponse.getTargetModel());
            String modelName = Objects.isNull(nDataModel) ? null : nDataModel.getAlias();

            List<ExecutableStepResponse> stepResponseList = getJobDetail(executableResponse.getProject(),
                    executableResponse.getId());
            stepResponseList.forEach(stepResponse -> {
                ExecutableStepResponse.OldParams stepOldParams = new ExecutableStepResponse.OldParams();
                stepOldParams.setExecWaitTime(stepResponse.getWaitTime());
                stepResponse.setOldParams(stepOldParams);
            });

            oldParams.setProjectName(executableResponse.getProject());
            oldParams.setRelatedCube(modelName);
            oldParams.setDisplayCubeName(modelName);
            oldParams.setUuid(executableResponse.getId());
            oldParams.setType(jobTypeMap.get(executableResponse.getJobName()));
            oldParams.setName(executableResponse.getJobName());
            oldParams.setExecInterruptTime(0L);
            oldParams.setMrWaiting(executableResponse.getWaitTime());

            executableResponse.setOldParams(oldParams);
            executableResponse.setSteps(stepResponseList);
        });

        return executableResponseList;
    }

    @VisibleForTesting
    public List<ProjectInstance> getReadableProjects() {
        return projectService.getReadableProjects(null, false);
    }

    public DataResult<List<ExecutableResponse>> listGlobalJobs(final JobFilter jobFilter, int offset, int limit) {
        List<AbstractExecutable> jobs = new ArrayList<>();
        for (ProjectInstance project : getReadableProjects()) {
            jobFilter.setProject(project.getName());
            jobs.addAll(listJobs0(jobFilter));
        }
        jobFilter.setProject(null);

        return filterAndSort(jobFilter, jobs, offset, limit);
    }

    private long getTimeStartInMillis(Calendar calendar, JobTimeFilterEnum timeFilter) {
        Message msg = MsgPicker.getMsg();

        switch (timeFilter) {
        case LAST_ONE_DAY:
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_WEEK:
            calendar.add(Calendar.WEEK_OF_MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_MONTH:
            calendar.add(Calendar.MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_YEAR:
            calendar.add(Calendar.YEAR, -1);
            return calendar.getTimeInMillis();
        case ALL:
            return 0;
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getILLEGAL_TIME_FILTER());
        }
    }

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        Message msg = MsgPicker.getMsg();
        switch (status) {
        case SUICIDAL:
        case DISCARDED:
            return ExecutableState.SUICIDAL;
        case ERROR:
            return ExecutableState.ERROR;
        case FINISHED:
            return ExecutableState.SUCCEED;
        case NEW:
            return ExecutableState.READY;
        case PENDING:
            return ExecutableState.READY;
        case RUNNING:
            return ExecutableState.RUNNING;
        case STOPPED:
            return ExecutableState.PAUSED;
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getILLEGAL_EXECUTABLE_STATE());
        }
    }

    private void dropJob(String project, String jobId) {
        NExecutableManager executableManager = getExecutableManager(project);
        executableManager.deleteJob(jobId);
    }

    @VisibleForTesting
    public void updateJobStatus(String jobId, String project, String action) throws IOException {
        val executableManager = getExecutableManager(project);
        UnitOfWorkContext.UnitTask afterUnitTask = () -> EventBusFactory.getInstance()
                .postWithLimit(new JobReadyNotifier(project));
        JobActionEnum.validateValue(action.toUpperCase(Locale.ROOT));
        switch (JobActionEnum.valueOf(action.toUpperCase(Locale.ROOT))) {
        case RESUME:
            executableManager.resumeJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, project);
            break;
        case RESTART:
            executableManager.restartJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            break;
        case DISCARD:
            discardJob(project, jobId);
            JobTypeEnum jobTypeEnum = executableManager.getJob(jobId).getJobType();
            String jobType = jobTypeEnum == null ? "" : jobTypeEnum.name();
            UnitOfWork.get().doAfterUnit(() ->
                    EventBusFactory.getInstance().postAsync(new JobDiscardNotifier(project, jobType)));
            break;
        case PAUSE:
            executableManager.pauseJob(jobId);
            break;
        default:
            throw new IllegalStateException("This job can not do this action: " + action);
        }

    }

    private void discardJob(String project, String jobId) throws IOException {
        AbstractExecutable job = getExecutableManager(project).getJob(jobId);
        if (ExecutableState.SUCCEED == job.getStatus()) {
            throw new KylinException(FAILED_UPDATE_JOB_STATUS, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidJobStatusTransaction(), "DISCARD", jobId, job.getStatus()));
        }
        if (ExecutableState.DISCARDED == job.getStatus()) {
            return;
        }
        getExecutableManager(project).discardJob(job.getId());
    }

    /**
     * for 3x api, jobId is unique.
     *
     * @param jobId
     * @return
     */
    public String getProjectByJobId(String jobId) {
        Preconditions.checkNotNull(jobId);

        for (ProjectInstance projectInstance : getReadableProjects()) {
            NExecutableManager executableManager = getExecutableManager(projectInstance.getName());
            if (Objects.nonNull(executableManager.getJob(jobId))) {
                return projectInstance.getName();
            }
        }
        return null;
    }

    /**
     * for 3x api
     *
     * @param jobId
     * @return
     */
    public ExecutableResponse getJobInstance(String jobId) {
        Preconditions.checkNotNull(jobId);
        String project = getProjectByJobId(jobId);
        Preconditions.checkNotNull(project, "Can not find the job: {}", jobId);

        NExecutableManager executableManager = getExecutableManager(project);
        AbstractExecutable executable = executableManager.getJob(jobId);

        return convert(executable);
    }

    /**
     * for 3x api
     *
     * @param project
     * @param job
     * @param action
     * @return
     * @throws IOException
     */
    @Transaction(project = 0)
    public ExecutableResponse manageJob(String project, ExecutableResponse job, String action) throws IOException {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(job);
        Preconditions.checkArgument(!StringUtils.isBlank(action));

        if (JobActionEnum.DISCARD == JobActionEnum.valueOf(action)) {
            return job;
        }

        updateJobStatus(job.getId(), project, action);
        return getJobInstance(job.getId());
    }

    public List<ExecutableStepResponse> getJobDetail(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        NExecutableManager executableManager = getExecutableManager(project);
        //executableManager.getJob only reply ChainedExecutable
        AbstractExecutable executable = executableManager.getJob(jobId);
        List<ExecutableStepResponse> executableStepList = new ArrayList<>();
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
        for (int i = 0; i < tasks.size(); ++i) {
            AbstractExecutable task = tasks.get(i);
            executableStepList.add(parseToExecutableStep(task, getExecutableManager(project).getOutput(task.getId())));
        }
        if (executable.getStatus() == ExecutableState.DISCARDED) {
            executableStepList
                    .forEach(executableStepResponse -> executableStepResponse.setStatus(JobStatusEnum.DISCARDED));
        }
        return executableStepList;

    }

    private ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, Output stepOutput) {
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
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stepOutput));
        result.setDuration(AbstractExecutable.getDuration(stepOutput));
        result.setWaitTime(task.getWaitTime());
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        return result;
    }

    private void batchUpdateJobStatus0(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);
        for (val job : jobs) {
            updateJobStatus(job.getId(), project, action);
        }
    }

    @Transaction(project = 1)
    public void batchUpdateJobStatus(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
    }

    public void batchUpdateGlobalJobStatus(List<String> jobIds, String action, List<String> filterStatuses) {
        logger.info("Owned projects is {}", projectService.getOwnedProjects());
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
                return null;
            }, project);
        }
    }

    private void batchDropJob0(String project, List<String> jobIds, List<String> filterStatuses) {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);

        NExecutableManager executableManager = getExecutableManager(project);
        jobs.forEach(job -> executableManager.checkJobCanBeDeleted(job.getId()));

        jobs.forEach(job -> dropJob(project, job.getId()));
    }

    private List<AbstractExecutable> getJobsByStatus(String project, List<String> jobIds, List<String> filterStatuses) {
        Preconditions.checkNotNull(project);

        val executableManager = getExecutableManager(project);
        List<ExecutableState> executableStates = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(filterStatuses)) {
            for (String status : filterStatuses) {
                JobStatusEnum jobStatus = JobStatusEnum.getByName(status);
                if (Objects.nonNull(jobStatus)) {
                    executableStates.add(parseToExecutableState(jobStatus));
                }
            }
        }

        return executableManager.getExecutablesByStatus(jobIds, executableStates);
    }

    @Transaction(project = 0)
    public void batchDropJob(String project, List<String> jobIds, List<String> filterStatuses) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        batchDropJob0(project, jobIds, filterStatuses);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchDropGlobalJob(List<String> jobIds, List<String> filterStatuses) {
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                batchDropJob0(project, jobIds, filterStatuses);
                return null;
            }, project);
        }
    }

    public JobStatisticsResponse getJobStats(String project, long startTime, long endTime) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getJobStatisticsManager(project);
        Pair<Integer, JobStatistics> stats = manager.getOverallJobStats(startTime, endTime);
        JobStatistics jobStatistics = stats.getSecond();
        return new JobStatisticsResponse(stats.getFirst(), jobStatistics.getTotalDuration(),
                jobStatistics.getTotalByteSize());
    }

    public Map<String, Integer> getJobCount(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getJobStatisticsManager(project);
        if (dimension.equals("model"))
            return manager.getJobCountByModel(startTime, endTime);

        return manager.getJobCountByTime(startTime, endTime, dimension);
    }

    public Map<String, Double> getJobDurationPerByte(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getJobStatisticsManager(project);
        if (dimension.equals("model"))
            return manager.getDurationPerByteByModel(startTime, endTime);

        return manager.getDurationPerByteByTime(startTime, endTime, dimension);
    }

    public Map<String, Object> getEventsInfoGroupByModel(String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        Map<String, Object> result = Maps.newHashMap();
        result.put("data", null);
        result.put("size", 0);
        return result;
    }

    public String getJobOutput(String project, String jobId) {
        return getJobOutput(project, jobId, jobId);
    }

    public String getJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getExecutableManager(project);
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId).getVerboseMsg();
    }

    public String getAllJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getExecutableManager(project);
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE).getVerboseMsg();
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
    @Transaction(project = 0)
    public void updateSparkJobInfo(String project, String jobId, String taskId, String yarnAppId, String yarnAppUrl) {
        val executableManager = getExecutableManager(project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_ID, yarnAppId);
        extraInfo.put(ExecutableConstants.YARN_APP_URL, yarnAppUrl);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }

    @Transaction(project = 0)
    public void updateSparkTimeInfo(String project, String jobId, String taskId, String waitTime, String buildTime) {
        val executableManager = getExecutableManager(project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_JOB_WAIT_TIME, waitTime);
        extraInfo.put(ExecutableConstants.YARN_JOB_RUN_TIME, buildTime);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }

    public void checkJobStatus(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        jobStatuses.forEach(this::checkJobStatus);
    }

    public void checkJobStatus(String jobStatus) {
        Message msg = MsgPicker.getMsg();
        if (Objects.isNull(JobStatusEnum.getByName(jobStatus))) {
            throw new KylinException(ILLEGAL_JOB_STATUS, msg.getILLEGAL_JOB_STATE());
        }
    }

    public void checkJobStatusAndAction(String jobStatus, String action) {
        checkJobStatus(jobStatus);
        JobActionEnum.validateValue(action);
        JobStatusEnum jobStatusEnum = JobStatusEnum.valueOf(jobStatus);
        if (!jobStatusEnum.checkAction(JobActionEnum.valueOf(action))) {
            throw new KylinException(ILLEGAL_JOB_ACTION, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getILLEGAL_JOB_ACTION(), jobStatus, jobStatusEnum.getValidActions()));
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

}
