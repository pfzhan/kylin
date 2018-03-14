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

package io.kyligence.kap.rest;

import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.JobService;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.service.SchedulerJobService;

public class ScheduleBuildJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleBuildJob.class);

    @Autowired
    private JobService jobService;

    @Autowired
    private SchedulerJobService schedulerJobService;

    public synchronized void execute(JobExecutionContext context) throws JobExecutionException {
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);

        if (jobService == null || schedulerJobService == null) {
            return;
        }

        JobDetail jobDetail = context.getJobDetail();
        JobDataMap dataMap = jobDetail.getJobDataMap();
        Scheduler scheduler = context.getScheduler();

        String jobName = dataMap.getString("name");
        String userName = dataMap.getString("user");

        logger.info("Scheduler of jobName " + jobName + " is triggered.");
        try {
            Long startTime = dataMap.getLong("startTime");
            SchedulerJobInstance schedulerInstance = schedulerJobService.getSchedulerJob(jobName);
            String errorJobId = null;
            CubeInstance cube = jobService.getCubeManager().getCube(schedulerInstance.getRelatedRealization());

            if (cube.getStatus() == RealizationStatusEnum.DISABLED && cube.getSegments().size() > 0) {
                logger.info("Cube " + cube.getName() + " is disabled, skip the scheduler this time.");
                return;
            }

            if (cube.getLatestReadySegment() != null) {
                startTime = cube.getDateRangeEnd();
            }

            boolean schedulerRemoved = false;
            boolean jobShouldBeResumed = false;
            List<CubingJob> cubingJobs = jobService.listJobsByRealizationName(cube.getName(),
                    schedulerInstance.getProject(), EnumSet.of(ExecutableState.RUNNING));

            if (cubingJobs != null && cubingJobs.size() > 0) {
                logger.info("Scheduler of jobName " + jobName + " is still cubing, skip this time.");
                return;
            }

            List<CubingJob> errorJobs = jobService.listJobsByRealizationName(cube.getName(),
                    schedulerInstance.getProject(), EnumSet.of(ExecutableState.ERROR, ExecutableState.STOPPED));

            if (errorJobs.size() > 1) {
                throw new InternalErrorException("Cube " + cube.getName() + " has more than one error/stopped jobs.");
            } else if (errorJobs.size() == 1) {
                CubingJob job = errorJobs.get(0);
                JobInstance jobInstance = jobService.getJobInstance(job.getId());
                CubeSegment segment = cube.getSegmentById(jobInstance.getRelatedSegment());

                if (segment != null && startTime == segment.getTSRange().getStart()) {
                    jobShouldBeResumed = true;
                    errorJobId = job.getId();
                    logger.info("Scheduled job " + jobName + " failed or stopped last time, resume it this time.");
                }
            }

            if (!jobShouldBeResumed) {

                Long endTime = 0L;

                //partitionInterval's cases are:
                //1. partitionInterval > 0 : Normal cases, partitionInterval is set to be the mill seconds each time appended
                //2. partitionInterval == 0: Build till triggered time is selected, use current mill seconds as partition's end time
                //3. partitionInterval < 0: Append with certain months' time interval

                if (dataMap.getLong("partitionInterval") > 0) {
                    endTime = startTime + dataMap.getLong("partitionInterval");
                } else if (dataMap.getLong("partitionInterval") == 0) {
                    endTime = System.currentTimeMillis();
                } else {
                    long monthsNum = -dataMap.getLong("partitionInterval");
                    Date startDate = new Date(startTime);
                    Calendar calender = Calendar.getInstance();
                    calender.setTime(startDate);
                    calender.add(Calendar.MONTH, (int) monthsNum);
                    endTime = calender.getTimeInMillis();
                }

                if (cube.getModel().getRootFactTable().getTableDesc().getSourceType() == ISourceAware.ID_STREAMING) {
                    jobService.submitJobInternal(cube, null, CubeBuildTypeEnum.BUILD, false, userName);
                } else {
                    jobService.submitJobInternal(cube, new SegmentRange.TimePartitionedSegmentRange(startTime, endTime),
                            CubeBuildTypeEnum.BUILD, false, userName);
                }
            } else {
                jobService.getExecutableManager().resumeJob(errorJobId);
            }

            // Stop scheduler if has run scheduled times
            if (schedulerInstance.getCurRepeatCount() == (schedulerInstance.getRepeatCount() - 1)) {
                schedulerJobService.deleteSchedulerJobInternal(jobName);
                scheduler.deleteJob(JobKey.jobKey(jobName));
                schedulerRemoved = true;
            }

            // Reset scheduler job
            if (!schedulerRemoved) {
                Map<String, Long> settings = new HashMap();

                settings.put("partitionStartTime", startTime);
                settings.put("curRepeatCount", schedulerInstance.getCurRepeatCount() + 1);

                schedulerJobService.updateSchedulerJobInternal(schedulerInstance, settings);
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }
}