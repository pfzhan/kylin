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

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.service.SchedulerJobService;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.JobService;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.impl.JobDetailImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ScheduleBuildJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleBuildJob.class);

    public synchronized void execute(JobExecutionContext context) throws JobExecutionException {
        JobDetail jobDetail = context.getJobDetail();
        JobDataMap dataMap = jobDetail.getJobDataMap();
        Object jobServiceObj = dataMap.get("jobService");
        Object schedulerJobServiceObj = dataMap.get("schedulerJobService");
        Object authenticationObj = dataMap.get("authentication");
        Scheduler scheduler = context.getScheduler();

        if(!JobService.class.isInstance(jobServiceObj))
            throw new InternalErrorException("Invalid job service instance.");

        if(!Authentication.class.isInstance(authenticationObj))
            throw new InternalErrorException("Invalid authentication instance.");

        SchedulerJobService schedulerJobService = SchedulerJobService.class.cast(schedulerJobServiceObj);
        JobService jobService = JobService.class.cast(jobServiceObj);
        Authentication authentication = Authentication.class.cast(authenticationObj);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        String jobName = dataMap.getString("name");

        logger.info(jobName + " is triggered.");
        try {
            String cubeName = dataMap.getString("cube");
            JobDetailImpl buildingJobs = (JobDetailImpl) scheduler.getJobDetail(JobKey.jobKey("building_jobs"));
            JobDataMap buildingMap = buildingJobs.getJobDataMap();
            String lastJobUuid = buildingMap.getString(cubeName);
            Long startTime = dataMap.getLong("startTime");
            JobInstance  jobInstance = null;
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

            if(lastJobUuid != null) {
                jobInstance = jobService.getJobInstance(lastJobUuid);
            }

            if(jobInstance == null || jobInstance.getStatus() == JobStatusEnum.FINISHED ||
                    jobInstance.getStatus() == JobStatusEnum.DISCARDED || jobInstance.getStatus() == JobStatusEnum.STOPPED) {
                if(cube.getLatestReadySegment() != null) {
                    startTime = cube.getDateRangeEnd();
                }

                Long endTime = 0L;

                //partitionInterval's cases are:
                //1. partitionInterval > 0 : Normal cases, partitionInterval is set to be the mill seconds each time appended
                //2. partitionInterval == 0: Build till triggered time is selected, use current mill seconds as partition's end time
                //3. partitionInterval < 0: Append with certain months' time interval

                if(dataMap.getLong("partitionInterval") > 0) {
                    endTime = startTime + dataMap.getLong("partitionInterval");
                } else if(dataMap.getLong("partitionInterval") == 0) {
                    endTime = System.currentTimeMillis();
                } else {
                    long monthsNum = -dataMap.getLong("partitionInterval");
                    Date startDate = new Date(startTime);
                    Calendar calender = Calendar.getInstance();
                    calender.setTime(startDate);
                    calender.add(Calendar.MONTH, (int) monthsNum);
                    endTime = calender.getTimeInMillis();
                }

                jobInstance = jobService.submitJob(cube, startTime, endTime, 0, 0, null, null, CubeBuildTypeEnum.BUILD, false, authentication.getName());
                buildingMap.put(cubeName, jobInstance.getUuid());
                buildingJobs.setJobDataMap(buildingMap);
                scheduler.deleteJob(JobKey.jobKey("building_jobs"));
                scheduler.addJob(buildingJobs, true);

                // Reset scheduler job
                SchedulerJobInstance schedulerJobInstance = schedulerJobService.getSchedulerJob(jobName);
                if(schedulerJobInstance.getCurRepeatCount() == (schedulerJobInstance.getRepeatCount() - 1)) {
                    schedulerJobService.deleteSchedulerJob(jobName);
                    scheduler.deleteJob(JobKey.jobKey(jobName));
                } else {
                    Map<String, Long> settings = new HashMap();

                    settings.put("partitionStartTime", startTime);
                    settings.put("scheduledRunTime", schedulerJobInstance.getScheduledRunTime() + schedulerJobInstance.getRepeatInterval());
                    settings.put("curRepeatCount", schedulerJobInstance.getCurRepeatCount() + 1);

                    schedulerJobService.updateSchedulerJob(jobName, settings);
                }
            } else if(jobInstance.getStatus() == JobStatusEnum.ERROR) {
                jobService.resumeJob(jobInstance);
            }

        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }
}