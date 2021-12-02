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
import static org.apache.kylin.common.exception.ServerErrorCode.JOB_NOT_EXIST;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobDiscardNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@Component("jobService")
public class JobService extends BasicService {

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @Autowired
    private ProjectService projectService;

    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    private static final Logger logger = LoggerFactory.getLogger("schedule");

    private static final Map<String, String> jobTypeMap = Maps.newHashMap();

    private static final String TOTAL_DURATION = "total_duration";
    private static final String LAST_MODIFIED = "last_modified";
    public static final String EXCEPTION_RESOLVE_PATH = "exception_resolve.json";
    public static final String EXCEPTION_RESOLVE_DEFAULT = "{\"en\": \"/monitor/exception_resolve.html\","
            + "\"zh-cn\": \"/monitor/exception_resolve.html\"}";

    static {
        jobTypeMap.put("INDEX_REFRESH", "Refresh Data");
        jobTypeMap.put("INDEX_MERGE", "Merge Data");
        jobTypeMap.put("INDEX_BUILD", "Build Index");
        jobTypeMap.put("INC_BUILD", "Load Data");
        jobTypeMap.put("TABLE_SAMPLING", "Sample Table");
    }

    @Autowired
    public JobService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    @VisibleForTesting
    public ExecutableResponse convert(AbstractExecutable executable) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    private List<ExecutablePOSortBean> filterAndSortExecutablePO(final JobFilter jobFilter, List<ExecutablePO> jobs) {
        Preconditions.checkNotNull(jobFilter);
        Preconditions.checkNotNull(jobs);

        Comparator<ExecutablePOSortBean> comparator = propertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? LAST_MODIFIED : jobFilter.getSortBy(),
                !jobFilter.isReverse());
        Set<JobStatusEnum> matchedJobStatusEnums = jobFilter.getStatuses().stream().map(JobStatusEnum::valueOf)
                .collect(Collectors.toSet());
        Set<ExecutableState> matchedExecutableStates = matchedJobStatusEnums.stream().map(this::parseToExecutableState)
                .collect(Collectors.toSet());
        return jobs.stream().filter(((Predicate<ExecutablePO>) (executablePO -> {
            if (CollectionUtils.isEmpty(jobFilter.getStatuses())) {
                return true;
            }
            ExecutableState state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
            return matchedExecutableStates.contains(state) || matchedJobStatusEnums.contains(state.toJobStatus());
        })).and(executablePO -> {
            String subject = StringUtils.trim(jobFilter.getKey());
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            return StringUtils.containsIgnoreCase(getTargetSubjectAlias(executablePO), subject)
                    || StringUtils.containsIgnoreCase(executablePO.getId(), subject);
        }).and(executablePO -> {
            List<String> jobNames = jobFilter.getJobNames();
            if (CollectionUtils.isEmpty(jobNames)) {
                return true;
            }
            return jobNames.contains(executablePO.getName());
        }).and(executablePO -> {
            String subject = jobFilter.getSubject();
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            //if filter on uuid, then it must be accurate
            return executablePO.getTargetModel().equals(jobFilter.getSubject().trim());
        })).map(this::createExecutablePOSortBean).sorted(comparator).collect(Collectors.toList());
    }

    private DataResult<List<ExecutableResponse>> filterAndSort(final JobFilter jobFilter, List<ExecutablePO> jobs,
            int offset, int limit) {
        val beanList = filterAndSortExecutablePO(jobFilter, jobs);
        List<ExecutableResponse> result = PagingUtil.cutPage(beanList, offset, limit).stream()
                .map(in -> in.getExecutablePO())
                .map(executablePO -> getExecutableManager(executablePO.getProject()).fromPO(executablePO))
                .map(this::convert).collect(Collectors.toList());
        return new DataResult<>(sortTotalDurationList(result, jobFilter), beanList.size(), offset, limit);
    }

    private List<ExecutableResponse> sortTotalDurationList(List<ExecutableResponse> result, final JobFilter jobFilter) {
        //constructing objects takes time
        if (StringUtils.isNotEmpty(jobFilter.getSortBy()) && jobFilter.getSortBy().equals(TOTAL_DURATION)) {
            Collections.sort(result, propertyComparator(TOTAL_DURATION, !jobFilter.isReverse()));
        }
        return result;
    }

    private String getTargetSubjectAlias(ExecutablePO executablePO) {
        val modelManager = NDataModelManager.getInstance(getConfig(), executablePO.getProject());
        NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), executablePO.getProject())
                .getDataModelDesc(executablePO.getTargetModel());
        String targetModel = executablePO.getTargetModel();
        String subjectAlia = null;
        if (dataModelDesc != null) {
            subjectAlia = modelManager.isModelBroken(targetModel)
                    ? modelManager.getDataModelDescWithoutInit(targetModel).getAlias()
                    : dataModelDesc.getAlias();
        }
        return subjectAlia;
    }

    private List<ExecutableResponse> filterAndSort(final JobFilter jobFilter, List<ExecutablePO> jobs) {
        val beanList = filterAndSortExecutablePO(jobFilter, jobs).stream()//
                .map(in -> in.getExecutablePO())
                .map(executablePO -> getExecutableManager(executablePO.getProject()).fromPO(executablePO))
                .map(this::convert).collect(Collectors.toList());
        return sortTotalDurationList(beanList, jobFilter);
    }

    private List<ExecutablePO> listExecutablePO(final JobFilter jobFilter) {
        JobTimeFilterEnum filterEnum = JobTimeFilterEnum.getByCode(jobFilter.getTimeFilter());
        Preconditions.checkNotNull(filterEnum, "Can not find the JobTimeFilterEnum by code: %s",
                jobFilter.getTimeFilter());

        NExecutableManager executableManager = getExecutableManager(jobFilter.getProject());
        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, filterEnum);
        long timeEndInMillis = Long.MAX_VALUE;
        return executableManager.getAllJobs(timeStartInMillis, timeEndInMillis);
    }

    public List<ExecutableResponse> listJobs(final JobFilter jobFilter) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listExecutablePO(jobFilter));
    }

    public DataResult<List<ExecutableResponse>> listJobs(final JobFilter jobFilter, int offset, int limit) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listExecutablePO(jobFilter), offset, limit);
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
        List<ExecutablePO> jobs = new ArrayList<>();
        for (ProjectInstance project : getReadableProjects()) {
            jobFilter.setProject(project.getName());
            jobs.addAll(listExecutablePO(jobFilter));
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
            killExistApplication(project, jobId);
            executableManager.restartJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            break;
        case DISCARD:
            discardJob(project, jobId);
            JobTypeEnum jobTypeEnum = executableManager.getJob(jobId).getJobType();
            String jobType = jobTypeEnum == null ? "" : jobTypeEnum.name();
            UnitOfWork.get().doAfterUnit(
                    () -> EventBusFactory.getInstance().postAsync(new JobDiscardNotifier(project, jobType)));
            break;
        case PAUSE:
            killExistApplication(project, jobId);
            executableManager.pauseJob(jobId);
            break;
        default:
            throw new IllegalStateException("This job can not do this action: " + action);
        }

    }

    private void discardJob(String project, String jobId) {
        AbstractExecutable job = getExecutableManager(project).getJob(jobId);
        if (ExecutableState.SUCCEED == job.getStatus()) {
            throw new KylinException(FAILED_UPDATE_JOB_STATUS, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInvalidJobStatusTransaction(), "DISCARD", jobId, job.getStatus()));
        }
        if (ExecutableState.DISCARDED == job.getStatus()) {
            return;
        }
        killExistApplication(job);
        getExecutableManager(project).discardJob(job.getId());
    }

    public void killExistApplication(String project, String jobId){
        AbstractExecutable job = getExecutableManager(project).getJob(jobId);
        killExistApplication(job);
    }

    public void killExistApplication(AbstractExecutable job){
        if (job instanceof ChainedExecutable) {
            // if job's task is running spark job, will kill this application
            ((ChainedExecutable) job).getTasks().stream() //
                    .filter(task -> task.getStatus() == ExecutableState.RUNNING) //
                    .filter(task -> task instanceof NSparkExecutable) //
                    .forEach(task -> ((NSparkExecutable) task).killOrphanApplicationIfExists(task.getId()));
        }
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
        if (executable == null) {
            throw new KylinException(JOB_NOT_EXIST,
                    String.format(Locale.ROOT, "Can't find job %s Please check and try again.", jobId));
        }

        // waite time in output
        Map<String, String> waiteTimeMap;
        Map<String, String> pausedTimeMap;
        val output = executable.getOutput();
        try {
            waiteTimeMap = JsonUtil.readValueAsMap(output.getExtra().getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));
            pausedTimeMap = JsonUtil
                    .readValueAsMap(output.getExtra().getOrDefault(NBatchConstants.P_PAUSED_TIME, "{}"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            waiteTimeMap = Maps.newHashMap();
            pausedTimeMap = Maps.newHashMap();
        }
        final String targetSubject = executable.getTargetSubject();
        List<ExecutableStepResponse> executableStepList = new ArrayList<>();
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
        for (AbstractExecutable task : tasks) {
            final ExecutableStepResponse executableStepResponse = parseToExecutableStep(task,
                    getExecutableManager(project).getOutput(task.getId()), waiteTimeMap, output.getState(),
                    pausedTimeMap);
            if (task.getStatus() == ExecutableState.ERROR) {
                executableStepResponse.setFailedStepId(output.getFailedStepId());
                executableStepResponse.setFailedSegmentId(output.getFailedSegmentId());
                executableStepResponse.setFailedStack(output.getFailedStack());
                val exception = output.getFailedStack().split("\n")[0];
                val exceptionResolve = getExceptionResolve(exception);
                executableStepResponse.setFailedResolve(exceptionResolve);
                executableStepResponse.setFailedStepName(task.getName());
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
                        val pausedStage = segmentId + stage.getId();
                        val pausedTime = pausedTimeMap.getOrDefault(pausedStage, "0");
                        val stageResponse = parseStageToExecutableStep(task, stage,
                                getExecutableManager(project).getOutput(stage.getId(), segmentId), pausedTime);
                        setStage(subStages, stageResponse);
                        stageResponses.add(stageResponse);

                        if (StringUtils.equals(output.getFailedStepId(), stage.getId())) {
                            executableStepResponse.setFailedStepName(stage.getName());
                        }
                    }

                    // table sampling and snapshot table don't have some segment
                    if (!StringUtils.equals(task.getId(), segmentId)) {
                        setSegmentSubStageParams(project, targetSubject, task, segmentId, segmentSubStages, stageBases,
                                stageResponses, waiteTimeMap, output.getState(), pausedTimeMap);
                        stringSubStageMap.put(segmentId, segmentSubStages);
                    }
                }
                if (MapUtils.isNotEmpty(stringSubStageMap)) {
                    executableStepResponse.setSegmentSubStages(stringSubStageMap);
                }
                if (CollectionUtils.isNotEmpty(subStages)) {
                    executableStepResponse.setSubStages(subStages);
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

    public String getExceptionResolve(String exceptionOrExceptionMessage) {
        try {
            val exceptionResolveStream = getClass().getClassLoader().getResource(EXCEPTION_RESOLVE_PATH).openStream();
            val exceptionResolve = JsonUtil.readValue(exceptionResolveStream, Map.class);
            for (Object o : exceptionResolve.entrySet()) {
                val entry = (Map.Entry) o;
                if (StringUtils.contains(exceptionOrExceptionMessage, String.valueOf(entry.getKey()))) {
                    val resolve = exceptionResolve.getOrDefault(entry.getKey(), EXCEPTION_RESOLVE_DEFAULT);
                    return JsonUtil.writeValueAsString(resolve);
                }
            }
            return EXCEPTION_RESOLVE_DEFAULT;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return EXCEPTION_RESOLVE_DEFAULT;
        }
    }

    private long getSegmentDuration(long execStartTime, long execEndTime) {
        if (execStartTime == 0) {
            return 0;
        }

        return execEndTime == 0 ? System.currentTimeMillis() - execStartTime : execEndTime - execStartTime;
    }

    private void setSegmentSubStageParams(String project, String targetSubject, AbstractExecutable task,
            String segmentId, ExecutableStepResponse.SubStages segmentSubStages, List<StageBase> stageBases,
            List<ExecutableStepResponse> stageResponses, Map<String, String> waiteTimeMap, ExecutableState jobState,
            Map<String, String> pausedTimeMap) {
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

        val execEndTime = stageResponses.stream()//
                .map(ExecutableStepResponse::getExecEndTime)//
                .max(Long::compare).orElse(0L);
        segmentSubStages.setExecEndTime(execEndTime);

        val segmentDuration = getSegmentDuration(execStartTime, execEndTime);
        var pauseTime = 0L;
        for (Map.Entry<String, String> entry : pausedTimeMap.entrySet()) {
            if (StringUtils.startsWith(entry.getKey(), segmentId)) {
                final String value = pausedTimeMap.getOrDefault(entry.getValue(), "0");
                if (Long.parseLong(value) > pauseTime) {
                    pauseTime = Long.parseLong(value);
                }
            }
        }
        segmentSubStages.setDuration(segmentDuration - pauseTime);

        final Segments<NDataSegment> segmentsByRange = modelService.getSegmentsByRange(targetSubject, project, "", "");
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
        val stepRatio = (float) ExecutableResponse.calculateSuccessStage(task, segmentId, stageBases, true) / stepCount;
        segmentSubStages.setStepRatio(stepRatio);
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

    private ExecutableStepResponse parseStageToExecutableStep(AbstractExecutable task, StageBase stageBase,
            Output stageOutput, String pausedTime) {
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

        val duration = AbstractExecutable.getDuration(stageOutput) - Long.parseLong(pausedTime);
        result.setDuration(duration);

        val indexCount = Optional.ofNullable(task.getParam(NBatchConstants.P_INDEX_COUNT)).orElse("0");
        result.setIndexCount(Long.parseLong(indexCount));
        if (result.getStatus() == JobStatusEnum.FINISHED) {
            result.setSuccessIndexCount(Long.parseLong(indexCount));
        } else {
            val successIndexCount = stageOutput.getExtra().getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
            result.setSuccessIndexCount(Long.parseLong(successIndexCount));
        }
        return result;
    }

    private ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, Output stepOutput,
            Map<String, String> waiteTimeMap, ExecutableState jobState, Map<String, String> pausedTimeMap) {
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

        long pausedTime = Long.parseLong(pausedTimeMap.getOrDefault(task.getId(), "0"));
        val duration = AbstractExecutable.getDuration(stepOutput) - pausedTime;
        result.setDuration(duration);
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

    private void batchUpdateJobStatus0(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);
        for (val job : jobs) {
            updateJobStatus(job.getId(), project, action);
        }
    }

    @Transaction(project = 0)
    public void updateJobError(String project, String jobId, String failedStepId, String failedSegmentId,
            String failedStack) {
        if (StringUtils.isBlank(failedStepId)) {
            return;
        }
        val executableManager = getExecutableManager(project);
        executableManager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack);
    }

    @Transaction(project = 0)
    public void updateStageStatus(String project, String taskId, String segmentId, String status,
            Map<String, String> updateInfo, String errMsg) {
        final ExecutableState newStatus = convertToExecutableState(status);
        val executableManager = getExecutableManager(project);
        executableManager.updateStageStatus(taskId, segmentId, newStatus, updateInfo, errMsg);
    }

    @VisibleForTesting
    public ExecutableState convertToExecutableState(String status) {
        if (StringUtils.isBlank(status)) {
            return null;
        }
        return ExecutableState.valueOf(status);
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
    public void batchDropJob(String project, List<String> jobIds, List<String> filterStatuses) {
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

    @SneakyThrows
    public InputStream getAllJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getExecutableManager(project);
        val output = executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE);
        return Optional.ofNullable(output.getVerboseMsgStream())
                .orElse(IOUtils.toInputStream(output.getVerboseMsg(), "UTF-8"));
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

    private ExecutablePOSortBean createExecutablePOSortBean(ExecutablePO executablePO) {
        ExecutablePOSortBean sortBean = new ExecutablePOSortBean();
        sortBean.setProject(executablePO.getProject());
        sortBean.setJobName(executablePO.getName());
        sortBean.setId(executablePO.getId());
        sortBean.setTargetSubject(executablePO.getTargetModel());
        sortBean.setLastModified(executablePO.getLastModified());
        sortBean.setCreateTime(executablePO.getCreateTime());
        sortBean.setTotalDuration(sortBean.computeTotalDuration(executablePO));

        sortBean.setExecutablePO(executablePO);
        return sortBean;
    }

    public void stopBatchJob(String project, TableDesc tableDesc) {
        for (NDataModel tableRelatedModel : getDataflowManager(project).getModelsUsingTable(tableDesc)) {
            stopBatchJobByModel(project, tableRelatedModel.getId());
        }
    }

    private void stopBatchJobByModel(String project, String modelId) {

        NDataModel model = getDataModelManager(project).getDataModelDesc(modelId);
        FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        FusionModel fusionModel = fusionModelManager.getFusionModel(modelId);
        if (!model.isFusionModel() || Objects.isNull(fusionModel)) {
            logger.warn("model is not fusion model or fusion model is null, {}", modelId);
            return;
        }

        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        executableManager.getJobs().stream().map(executableManager::getJob).filter(
                job -> StringUtils.equalsIgnoreCase(job.getTargetModelId(), fusionModel.getBatchModel().getUuid()))
                .forEach(job -> {
                    Set<ExecutableState> matchedExecutableStates = Stream
                            .of(JobStatusEnum.FINISHED, JobStatusEnum.ERROR, JobStatusEnum.DISCARDED)
                            .map(this::parseToExecutableState).collect(Collectors.toSet());
                    if (!matchedExecutableStates.contains(job.getOutput().getState())) {
                        executableManager.discardJob(job.getId());
                    }
                });
    }

    @Setter
    @Getter
    static class ExecutablePOSortBean implements IKeep {

        private String project;

        private String id;

        @JsonProperty("job_name")
        private String jobName;

        @JsonProperty("last_modified")
        private long lastModified;

        @JsonProperty("target_subject")
        private String targetSubject;

        @JsonProperty("create_time")
        private long createTime;

        @JsonProperty("total_duration")
        private long totalDuration;

        private ExecutablePO executablePO;

        private long computeTotalDuration(ExecutablePO executablePO) {
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (CollectionUtils.isEmpty(tasks)) {
                return 0L;
            }

            long taskCreateTime = executablePO.getOutput().getCreateTime();
            ExecutableState state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
            if (state.isProgressing()) {
                return System.currentTimeMillis() - taskCreateTime;
            }
            long duration = 0L;
            for (ExecutablePO subTask : tasks) {
                if (subTask.getOutput().getStartTime() == 0L) {
                    break;
                }
                duration = getExecutablePOEndTime(subTask) - taskCreateTime;
            }
            return duration == 0L ? getExecutablePOEndTime(executablePO) - taskCreateTime : duration;
        }

        private long getExecutablePOEndTime(ExecutablePO executablePO) {
            long time = executablePO.getOutput().getEndTime();
            return time == 0L ? System.currentTimeMillis() : time;
        }
    }
}
