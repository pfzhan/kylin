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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.kyligence.kap.rest.request.JobActionEnum;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
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

@Component("jobService")
public class JobService extends BasicService {

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;


    private static final Logger logger = LoggerFactory.getLogger(JobService.class);
    private static final String JOB_NAME = "job_name";
    private static final String TARGET_SUBJECT = "target_subject";
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
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();
        Integer[] status = jobFilter.getStatus();
        if (ArrayUtils.isNotEmpty(status)) {
            for (int stat : status) {
                statusList.add(JobStatusEnum.getByCode(stat));
            }
        }
        final Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        List<AbstractExecutable> jobs = executableManager.getAllExecutables(timeStartInMillis, timeEndInMillis);
        ImmutableList<ExecutableResponse> filteredJobs =
                FluentIterable.from(jobs).filter(Predicates.and(new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(AbstractExecutable abstractExecutable) {
                        if (CollectionUtils.isEmpty(states)) {
                            return true;
                        }
                        ExecutableState state = abstractExecutable.getStatus();
                        return states.contains(state);
                    }
                }, new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(AbstractExecutable abstractExecutable) {
                        String[] subjects = jobFilter.getSubjects();
                        if (ArrayUtils.isEmpty(subjects)) {
                            return true;
                        }
                        return Lists.newArrayList(subjects).contains(abstractExecutable.getTargetSubject());
                    }
                }, new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(AbstractExecutable abstractExecutable) {
                        String jobName = jobFilter.getJobName();
                        if (StringUtils.isEmpty(jobName)) {
                            return true;
                        }
                        return abstractExecutable.getName().toLowerCase().contains(jobName.toLowerCase());
                    }
                })).transform(new Function<AbstractExecutable, ExecutableResponse>() {
                    @Override
                    public ExecutableResponse apply(AbstractExecutable abstractExecutable) {
                        ExecutableResponse executableResponse = ExecutableResponse.create(abstractExecutable);
                        executableResponse.setStatus(parseToJobStatus(abstractExecutable.getStatus()));
                        return executableResponse;
                    }
                }).toSortedList(new Comparator<ExecutableResponse>() {
                    @Override
                    public int compare(ExecutableResponse o1, ExecutableResponse o2) {
                        String sortBy = jobFilter.getSortBy();
                        return sortJobs(sortBy, o1, o2);
                    }
                });
        if (jobFilter.isReverse()) {
            filteredJobs = filteredJobs.reverse();
        }
        List<ExecutableResponse> executableResponseResults = Lists.newArrayList(filteredJobs);
        return executableResponseResults;
    }

    private int sortJobs(String sortBy, ExecutableResponse o1, ExecutableResponse o2) {
        switch (sortBy) {
            case JOB_NAME:
                return o1.getJobName().compareTo(o2.getJobName());
            case TARGET_SUBJECT:
                return o1.getTargetSubject().compareTo(o2.getTargetSubject());
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

    private Set<ExecutableState> convertStatusEnumToStates(List<JobStatusEnum> statusList) {
        Set<ExecutableState> states;
        if (statusList == null || statusList.isEmpty()) {
            states = EnumSet.allOf(ExecutableState.class);
        } else {
            states = Sets.newHashSet();
            for (JobStatusEnum status : statusList) {
                states.add(parseToExecutableState(status));
            }
        }
        return states;
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


    public void dropJob(String project, String jobId) throws IOException {
        NExecutableManager executableManager = getExecutableManager(project);
        executableManager.deleteJob(jobId);
        tableExtService.removeJobIdFromTableExt(jobId, project);
    }

    public void updateJobStatus(String jobId, String project, String action) throws IOException {
        NExecutableManager executableManager = getExecutableManager(project);
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
            executableStepList.add(parseToExecutableStep(task, i, getExecutableManager(project).getOutput(task.getId())));
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
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        return result;
    }
}
