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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
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
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
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
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import io.kyligence.kap.rest.request.JobActionEnum;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.EventModelResponse;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableSortBean;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.var;

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

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

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
        executableResponse.setStatus(parseToJobStatus(executable.getStatus()));
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
                            || matchedJobStatusEnums.contains(parseToJobStatus(state));
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
            return matchedExecutableStates.contains(state) || matchedJobStatusEnums.contains(parseToJobStatus(state));
        })).and(abstractExecutable -> {
            String subject = jobFilter.getKey();
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            return StringUtils.containsIgnoreCase(abstractExecutable.getTargetSubjectAlias(), subject);
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
        Calendar calendar = Calendar.getInstance();
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
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
            throw new BadRequestException(String.format(msg.getILLEGAL_TIME_FILTER(), timeFilter));
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
            throw new BadRequestException(String.format(msg.getILLEGAL_EXECUTABLE_STATE(), status));
        }
    }

    private JobStatusEnum parseToJobStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStatusEnum.PENDING;
        case RUNNING:
            return JobStatusEnum.RUNNING;
        case ERROR:
            return JobStatusEnum.ERROR;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case PAUSED:
            return JobStatusEnum.STOPPED;
        case SUICIDAL:
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    private void dropJob(String project, String jobId) {
        NExecutableManager executableManager = getExecutableManager(project);
        executableManager.deleteJob(jobId);
        tableExtService.removeJobIdFromTableExt(jobId, project);
    }

    @VisibleForTesting
    public void updateJobStatus(String jobId, String project, String action) throws IOException {
        val executableManager = getExecutableManager(project);
        UnitOfWorkContext.AfterUnitTask afterUnitTask = () -> SchedulerEventBusFactory
                .getInstance(KylinConfig.getInstanceFromEnv()).postWithLimit(new JobReadyNotifier(project));
        switch (JobActionEnum.valueOf(action)) {
        case RESUME:
            executableManager.resumeJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            NMetricsGroup.counterInc(NMetricsName.JOB_RESUMED, NMetricsCategory.PROJECT, project);
            break;
        case RESTART:
            executableManager.restartJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            break;
        case DISCARD:
            discardJob(project, jobId);
            NMetricsGroup.counterInc(NMetricsName.JOB_DISCARDED, NMetricsCategory.PROJECT, project);
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
        if (job.getStatus().equals(ExecutableState.SUCCEED)) {
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been succeed and cannot be discarded.");
        }
        if (job.getStatus().equals(ExecutableState.DISCARDED)) {
            return;
        }
        getExecutableManager(project).discardJob(job.getId());
    }

    /**
     * for 3x api, jobId is unique.
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
        return job;
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
            executableStepList
                    .add(parseToExecutableStep(task, i, getExecutableManager(project).getOutput(task.getId())));
        }
        if (executable.getStatus() == ExecutableState.DISCARDED) {
            executableStepList
                    .forEach(executableStepResponse -> executableStepResponse.setStatus(JobStatusEnum.DISCARDED));
        }
        return executableStepList;

    }

    private ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, int i, Output stepOutput) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);

        if (stepOutput == null) {
            logger.warn("Cannot found output for task: id={}", task.getId());
            return result;
        }

        result.setStatus(parseToJobStatus(stepOutput.getState()));
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

    private void batchUpdateJobStatus0(List<String> jobIds, String project, String action, String filterStatus)
            throws IOException {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(jobIds);

        val executableManager = getExecutableManager(project);
        JobStatusEnum jobStatus = JobStatusEnum.getByName(filterStatus);
        ExecutableState executableState = Objects.isNull(jobStatus) ? null : parseToExecutableState(jobStatus);
        val jobs = executableManager.getExecutablesByStatus(jobIds, executableState);
        for (val job : jobs) {
            updateJobStatus(job.getId(), project, action);
        }
    }

    @Transaction(project = 1)
    public void batchUpdateJobStatus(List<String> jobIds, String project, String action, String filterStatus)
            throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        batchUpdateJobStatus0(jobIds, project, action, filterStatus);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchUpdateGlobalJobStatus(List<String> jobIds, String action, String filterStatus) {
        UnitOfAllWorks.doInTransaction(() -> {
            for (ProjectInstance project : getReadableProjects()) {
                batchUpdateJobStatus0(jobIds, project.getName(), action, filterStatus);
            }
            return null;
        }, false);
    }

    private void batchDropJob0(String project, List<String> jobIds, String filterStatus) throws IOException {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(jobIds);

        val executableManager = getExecutableManager(project);
        JobStatusEnum jobStatus = JobStatusEnum.getByName(filterStatus);
        ExecutableState executableState = Objects.isNull(jobStatus) ? null : parseToExecutableState(jobStatus);
        val jobs = executableManager.getExecutablesByStatus(jobIds, executableState);
        for (val job : jobs) {
            dropJob(project, job.getId());
        }
    }

    @Transaction(project = 0)
    public void batchDropJob(String project, List<String> jobIds, String filterStatus) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        batchDropJob0(project, jobIds, filterStatus);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchDropGlobalJob(List<String> jobIds, String filterStatus) {
        UnitOfAllWorks.doInTransaction(() -> {
            for (ProjectInstance project : getReadableProjects()) {
                batchDropJob0(project.getName(), jobIds, filterStatus);
            }
            return null;
        }, false);
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
        Map<String, EventModelResponse> models = Maps.newHashMap();
        List<Event> jobRelatedEvents = getEventDao(project).getJobRelatedEvents();
        int jobSize = 0;

        for (var event : jobRelatedEvents) {
            String modelId = event.getModelId();
            EventModelResponse eventModelResponse = models.get(modelId);

            if (eventModelResponse == null) {
                val model = getDataModelManager(project).getDataModelDesc(modelId);
                if (model == null)
                    continue;
                eventModelResponse = new EventModelResponse(0, model.getAlias());
            }

            eventModelResponse.updateSize();
            models.put(modelId, eventModelResponse);
            jobSize++;
        }

        result.put("data", models);
        result.put("size", jobSize);
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

    /**
     * write hdfs file input stream to output stream
     *
     * @param outputStream
     * @param hdfsFilePath
     * @return
     */
    public boolean hdfsFileWrite2OutputStream(OutputStream outputStream, String hdfsFilePath) {
        try {
            Path path = new Path(hdfsFilePath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                logger.warn("can not find the hdfs file: {}", hdfsFilePath);
                return false;
            }

            try (DataInputStream din = fs.open(path)) {
                IOUtils.copyLarge(din, outputStream);
            }
            return true;
        } catch (IOException e) {
            logger.error("read hdfs file and write to the output stream failed!", e);
        }

        return false;
    }

    /**
     * get the log path by project and jobId(step id)
     *
     * @param project
     * @param jobId
     * @return
     */
    public String getHdfsLogPath(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        String hdfsPath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId);

        val executableManager = getExecutableManager(project);
        ExecutableOutputPO jobOutput = executableManager.getJobOutputFromHDFS(hdfsPath);

        if (Objects.isNull(jobOutput)) {
            logger.info("the jobOutput is null, project: {}, jobId {}", project, jobId);
            return null;
        }

        if (Objects.nonNull(jobOutput.getLogPath()) && !executableManager.isHdfsPathExists(jobOutput.getLogPath())) {
            logger.info("job output hdfs path is not exists, path: {}.", jobOutput.getLogPath());
            return null;
        }

        return jobOutput.getLogPath();
    }

    /**
     * user from web to download the hdfs log file
     *
     * @param response
     */
    public boolean downloadHdfsLogFile(final HttpServletResponse response, String hdfsLogPath) {
        try (OutputStream outputStream = response.getOutputStream()) {
            return hdfsFileWrite2OutputStream(outputStream, hdfsLogPath);
        } catch (IOException e) {
            logger.error("read hdfs file and write to the output stream failed!", e);
        }

        return false;
    }

    public List<EventResponse> getWaitingJobsByModel(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        List<Event> jobRelatedEvents = getEventDao(project).getJobRelatedEventsByModel(modelId);

        jobRelatedEvents.sort(Comparator.comparingLong(Event::getSequenceId).reversed());
        return jobRelatedEvents.stream().map(event -> new EventResponse(getJobType(event), event.getLastModified()))
                .collect(Collectors.toList());
    }

    private String getJobType(Event event) {
        if (event instanceof AddCuboidEvent)
            return JobTypeEnum.INDEX_BUILD.toString();

        if (event instanceof AddSegmentEvent)
            return JobTypeEnum.INC_BUILD.toString();

        if (event instanceof MergeSegmentEvent)
            return JobTypeEnum.INDEX_MERGE.toString();

        if (event instanceof RefreshSegmentEvent)
            return JobTypeEnum.INDEX_REFRESH.toString();

        throw new IllegalStateException(String.format("Illegal type of event %s", event.getId()));
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

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }

    @Transaction(project = 0)
    public void updateSparkTimeInfo(String project, String jobId, String taskId, String waitTime, String buildTime) {
        val executableManager = getExecutableManager(project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_JOB_WAIT_TIME, waitTime);
        extraInfo.put(ExecutableConstants.YARN_JOB_RUN_TIME, buildTime);

        executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
    }
}
