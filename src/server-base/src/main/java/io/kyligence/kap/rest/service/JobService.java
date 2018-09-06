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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;

@Component("jobService")
public class JobService extends BasicService {
    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    public ArrayList<AbstractExecutable> listJobs(final String project, final List<JobStatusEnum> statusList,
            JobTimeFilterEnum timeFilter, final String[] subjects, final String jobType) throws PersistentException {
        NExecutableManager executableManager = getExecutableManager(project);
        aclEvaluate.checkProjectOperationPermission(project);
        // prepare time range
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, timeFilter);
        long timeEndInMillis = Long.MAX_VALUE;
        final Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        List<AbstractExecutable> jobs = executableManager.getAllExecutables(timeStartInMillis, timeEndInMillis);
        ArrayList<AbstractExecutable> filteredJobs = Lists
                .newArrayList(FluentIterable.from(jobs).filter(new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(@Nullable AbstractExecutable abstractExecutable) {
                        if (abstractExecutable instanceof DefaultChainedExecutable) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).filter(Predicates.and(new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(@Nullable AbstractExecutable abstractExecutable) {
                        if (states == null || states.size() == 0) {
                            return true;
                        }
                        try {
                            ExecutableState state = abstractExecutable.getStatus();
                            return states.contains(state);

                        } catch (Exception e) {
                            throw e;
                        }
                    }
                }, new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(@Nullable AbstractExecutable abstractExecutable) {
                        if (subjects == null || subjects.length == 0) {
                            return true;
                        }
                        return Lists.newArrayList(subjects).contains(abstractExecutable.getTargetSubject());
                    }
                }, new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(@Nullable AbstractExecutable abstractExecutable) {
                        if (StringUtils.isEmpty(jobType)) {
                            return true;
                        }
                        return jobType.equals(abstractExecutable.getJobType());
                    }
                })));

        return filteredJobs;
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

    public List<JobInstance> parseToJobInstance(ArrayList<AbstractExecutable> executables, Integer offset,
            Integer limit, String project) {
        List<AbstractExecutable> subExecutables = PagingUtil.cutPage(executables, offset, limit);
        List<JobInstance> jobs = new ArrayList<>();
        for (AbstractExecutable executable : subExecutables) {
            JobInstance jobInstance = new JobInstance();
            jobInstance.setDuration(executable.getDuration());
            jobInstance.setExecEndTime(executable.getEndTime());
            jobInstance.setExecInterruptTime(executable.getInterruptTime());
            jobInstance.setExecStartTime(executable.getStartTime());
            jobInstance.setName(executable.getName());
            jobInstance.setStatus(parseToJobStatus(executable.getStatus()));
            jobInstance.setSubmitter(executable.getSubmitter());
            jobInstance.setJobType(executable.getJobType());
            jobInstance.setTargetSubject(executable.getTargetSubject());
            jobInstance.setDataRangeStart(executable.getDataRangeStart());
            jobInstance.setDataRangeEnd(executable.getDataRangeEnd());
            jobInstance.setUuid(executable.getId());
            List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
            for (int i = 0; i < tasks.size(); ++i) {
                AbstractExecutable task = tasks.get(i);
                jobInstance.addStep(parseToJobStep(task, i, getExecutableManager(project).getOutput(task.getId())));
            }
            jobs.add(jobInstance);
        }
        return jobs;
    }

    public static JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i, Output stepOutput) {
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);

        if (stepOutput == null) {
            logger.warn("Cannot found output for task: id={}", task.getId());
            return result;
        }

        result.setStatus(parseToJobStepStatus(stepOutput.getState()));
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
        if (task instanceof NSparkExecutable) {
            /*result.setExecCmd(((NSparkExecutable) task).getParam());
            result.setExecWaitTime(
                    AbstractExecutable.getExtraInfoAsLong(stepOutput, NSparkExecutable.getExtraInfoAsLong(task.getOutput(),), 0L)
                            / 1000);*/
        }
        return result;
    }

    public static JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStepStatusEnum.PENDING;
        case RUNNING:
            return JobStepStatusEnum.RUNNING;
        case ERROR:
            return JobStepStatusEnum.ERROR;
        case DISCARDED:
            return JobStepStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStepStatusEnum.FINISHED;
        case STOPPED:
            return JobStepStatusEnum.STOPPED;
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    public static JobStatusEnum parseToJobStatus(ExecutableState state) {
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
}
