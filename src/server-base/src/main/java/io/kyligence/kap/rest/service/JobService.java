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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.rest.response.EventModelResponse;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.JobActionEnum;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("jobService")
public class JobService extends BasicService {

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);
    private static final String JOB_NAME = "job_name";
    private static final String CREATE_TIME = "create_time";
    private static final String TARGET_SUBJECT_ALIAS = "target_subject_alias";
    private static final String JOB_STATUS = "job_status";
    private static final String EXEC_START_TIME = "exec_start_time";
    private static final String DURATION = "duration";

    public List<ExecutableResponse> listJobs(final JobFilter jobFilter) {
        NExecutableManager executableManager = getExecutableManager(jobFilter.getProject());
        // prepare time range
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, JobTimeFilterEnum.getByCode(jobFilter.getTimeFilter()));
        long timeEndInMillis = Long.MAX_VALUE;
        List<AbstractExecutable> jobs = executableManager.getAllExecutables(timeStartInMillis, timeEndInMillis);
        List<ExecutableResponse> filteredJobs = jobs.stream().filter(

                ((Predicate<AbstractExecutable>) (abstractExecutable -> {
                    if (StringUtils.isEmpty(jobFilter.getStatus())) {
                        return true;
                    }
                    ExecutableState state = abstractExecutable.getStatus();
                    return state.equals(parseToExecutableState(JobStatusEnum.valueOf(jobFilter.getStatus())));
                })).and(abstractExecutable -> {
                    String subject = jobFilter.getSubjectAlias();
                    if (StringUtils.isEmpty(subject)) {
                        return true;
                    }
                    return StringUtils.containsIgnoreCase(abstractExecutable.getTargetModelAlias(), subject);
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
                    return abstractExecutable.getTargetModel().equals(jobFilter.getSubject());
                })

        ).map(abstractExecutable -> {
            ExecutableResponse executableResponse = ExecutableResponse.create(abstractExecutable);
            executableResponse.setStatus(parseToJobStatus(abstractExecutable.getStatus()));
            return executableResponse;
        }).sorted((o1, o2) -> {
            String sortBy = jobFilter.getSortBy();
            return sortJobs(sortBy, o1, o2);
        }).collect(Collectors.toList());

        if (jobFilter.isReverse()) {
            filteredJobs = Lists.reverse(filteredJobs);
        }
        return filteredJobs;
    }

    private int sortJobs(String sortBy, ExecutableResponse o1, ExecutableResponse o2) {
        switch (sortBy) {
        case JOB_NAME:
            return o1.getJobName().compareTo(o2.getJobName());
        case TARGET_SUBJECT_ALIAS:
            return o1.getTargetModelAlias().compareTo(o2.getTargetModelAlias());
        case JOB_STATUS:
            return o1.getStatus().compareTo(o2.getStatus());
        case EXEC_START_TIME:
            return o1.getExecStartTime() < o2.getExecStartTime() ? -1 : 1;
        case DURATION:
            return o1.getDuration() < o2.getDuration() ? -1 : 1;
        default:
            return o1.getLastModified() < o2.getLastModified() ? -1 : 1;
        }
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
        case DISCARDED:
            return ExecutableState.DISCARDED;
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
            return ExecutableState.STOPPED;
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
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case STOPPED:
            return JobStatusEnum.STOPPED;
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    private void dropJob(String project, String jobId) throws IOException {
        NExecutableManager executableManager = getExecutableManager(project);
        executableManager.deleteJob(jobId);
        tableExtService.removeJobIdFromTableExt(jobId, project);
    }

    private void updateJobStatus(String jobId, String project, String action) throws IOException {
        val executableManager = getExecutableManager(project);
        switch (JobActionEnum.valueOf(action)) {
        case RESUME:
            executableManager.resumeJob(jobId);
            break;
        case DISCARD:
            cancelJob(project, jobId);
            break;
        case PAUSE:
            executableManager.pauseJob(jobId);
            break;
        default:
            throw new IllegalStateException("This job can not do this action: " + action);
        }

    }

    private void cancelJob(String project, String jobId) throws IOException {
        AbstractExecutable job = getExecutableManager(project).getJob(jobId);
        if (job.getStatus().equals(ExecutableState.SUCCEED)) {
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been succeed and cannot be discarded.");
        }
        if (job.getStatus().equals(ExecutableState.DISCARDED)) {
            return;
        }
        job.cancelJob();
        getExecutableManager(project).discardJob(job.getId());
    }

    public List<ExecutableStepResponse> getJobDetail(String project, String jobId) {
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
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        return result;
    }

    @Transaction(project = 1)
    public void updateJobStatusBatchly(List<String> jobIds, String project, String action, String status)
            throws IOException {
        val executableManager = getExecutableManager(project);
        val jobs = executableManager.getExecutablesByStatus(jobIds, status);
        for (val job : jobs) {
            updateJobStatus(job.getId(), project, action);
        }
    }

    @Transaction(project = 0)
    public void dropJobBatchly(String project, List<String> jobIds, String status) throws IOException {
        val executableManager = getExecutableManager(project);
        val jobs = executableManager.getExecutablesByStatus(jobIds, status);
        for (val job : jobs) {
            dropJob(project, job.getId());
        }
    }

    @Transaction(project = 0)
    public void addJob(String project, ExecutablePO executablePO) {
        val executableManager = getExecutableManager(project);
        executableManager.addJob(executablePO);
    }

    @Transaction(project = 0)
    public void resumeJob(String project, String jobId) {
        val executableManager = getExecutableManager(project);
        executableManager.resumeJob(jobId);
    }

    public JobStatisticsResponse getJobStats(String project, long startTime, long endTime) {
        JobStatisticsManager manager = getJobStatisticsManager(project);
        Pair<Integer, Double> stats = manager.getOverallJobStats(startTime, endTime);

        return new JobStatisticsResponse(stats.getFirst(), stats.getSecond());
    }

    public Map<String, Integer> getJobCount(String project, long startTime, long endTime, String dimension) {
        JobStatisticsManager manager = getJobStatisticsManager(project);
        if (dimension.equals("model"))
            return manager.getJobCountByModel(startTime, endTime);

        return manager.getJobCountByTime(startTime, endTime, dimension);
    }

    public Map<String, Double> getJobDurationPerByte(String project, long startTime, long endTime, String dimension) {
        JobStatisticsManager manager = getJobStatisticsManager(project);
        if (dimension.equals("model"))
            return manager.getDurationPerByteByModel(startTime, endTime);

        return manager.getDurationPerByteByTime(startTime, endTime, dimension);
    }

    public Map<String, Object> getEventsInfoGroupByModel(String project) {
        Map<String, Object> result = Maps.newHashMap();
        Map<String, EventModelResponse> models = Maps.newHashMap();
        List<Event> jobRelatedEvents = getEventDao(project).getJobRelatedEvents();

        jobRelatedEvents.forEach(event -> {
            String modelName = event.getModelName();
            EventModelResponse eventModelResponse = models.get(modelName);

            if (eventModelResponse == null) {
                String modelAlias = getDataModelManager(project).getDataModelDesc(modelName).getAlias();
                eventModelResponse = new EventModelResponse(0, modelAlias);
            }

            eventModelResponse.updateSize();
            models.put(modelName, eventModelResponse);
        });

        result.put("data", models);
        result.put("size", jobRelatedEvents.size());
        return result;
    }

    public List<EventResponse> getWaitingJobsByModel(String project, String modelName) {
        List<Event> jobRelatedEvents = getEventDao(project).getJobRelatedEventsByModel(modelName);

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
}
