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
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.JobService;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobManager;
import io.kyligence.kap.rest.ScheduleBuildJob;

@Component("schedulerJobService")
public class SchedulerJobService extends BasicService {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobService.class);

    private Scheduler scheduler;

    private Map<String, TriggerKey> cubeTriggerKeyMap = new HashMap<>();

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    public SchedulerJobService() throws SchedulerException {
        scheduler = createScheduler();
        scheduler.start();
        //Create a faked job to record each cube's latest building job
        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setName("building_jobs");
        JobDataMap dataMap = jobDetail.getJobDataMap();
        jobDetail.setJobDataMap(dataMap);
        scheduler.addJob(jobDetail, true);
    }

    private Scheduler createScheduler() throws SchedulerException {
        return StdSchedulerFactory.getDefaultScheduler();
    }

    private String createCronExpression(long intervalTimeMillis, long firstTriggerTime) {
        StringBuilder sb = new StringBuilder("");

        Date firstTriggerDate = new Date(firstTriggerTime);
        Calendar cal = Calendar.getInstance();
        cal.setTime(firstTriggerDate);

        if (intervalTimeMillis < 0) {
            sb.append(cal.get(Calendar.SECOND) + " " + cal.get(Calendar.MINUTE) + " " + cal.get(Calendar.HOUR_OF_DAY)
                    + " " + cal.get(Calendar.DAY_OF_MONTH) + " * ?");
        } else {
            long minutes = intervalTimeMillis / (60 * 1000);
            long hours = minutes / 60;
            long days = hours / 24;
            if (days > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " + cal.get(Calendar.MINUTE) + " "
                        + cal.get(Calendar.HOUR_OF_DAY) + " " + cal.get(Calendar.DAY_OF_MONTH) + "/" + days + " * ?");
            } else if (hours > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " + cal.get(Calendar.MINUTE) + " "
                        + cal.get(Calendar.HOUR_OF_DAY) + "/" + hours + " *  * ?");
            } else if (minutes > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " + cal.get(Calendar.MINUTE) + "/" + minutes + " * * * ?");
            } else {
                sb.append(cal.get(Calendar.SECOND) + "/" + intervalTimeMillis / 1000 + " * * * * ?");
            }
        }
        return sb.toString();
    }

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    public SchedulerJobManager getSchedulerJobManager() {
        return SchedulerJobManager.getInstance(getConfig());
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs(final String projectName, final String cubeName,
            final Integer limit, final Integer offset) throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().getSchedulerJobs(projectName, cubeName);

        int climit = (null == limit) ? Integer.MAX_VALUE : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (jobs.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - coffset) < climit) {
            return jobs.subList(coffset, jobs.size());
        }

        return jobs.subList(coffset, coffset + climit);
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs(final String projectName, final String cubeName)
            throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().getSchedulerJobs(projectName, cubeName);
        return jobs;
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs() throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().listAllSchedulerJobs();
        return jobs;
    }

    public SchedulerJobInstance getSchedulerJob(final String jobName) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(jobName);

        return job;
    }

    public void keepModifyTs(SchedulerJobInstance schedulerJobInstance) throws IOException {
        String name = schedulerJobInstance.getName();

        SchedulerJobInstance youngerSelf = getSchedulerJob(name);

        if (youngerSelf != null) {
            schedulerJobInstance.setLastModified(youngerSelf.getLastModified());
        } else {
            schedulerJobInstance.setLastModified(0);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(SchedulerJobInstance job) throws IOException {
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(String name, String project, String cube, long triggerTime,
            long startTime, long repeatCount, long curRepeatCount, long repeatInterval, long partitionInterval)
            throws IOException {

        SchedulerJobInstance job = new SchedulerJobInstance(name, project, "cube", cube, startTime, triggerTime,
                repeatCount, curRepeatCount, repeatInterval, partitionInterval);
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance updateSchedulerJob(String name, Map<String, Long> settings) throws Exception {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);

        for (String key : settings.keySet()) {

            switch (key) {
            case "partitionStartTime":
                job.setPartitionStartTime(settings.get(key));
                break;
            case "scheduledRunTime":
                job.setScheduledRunTime(settings.get(key));
                break;
            case "repeatCount":
                job.setRepeatCount(settings.get(key));
                break;
            case "curRepeatCount":
                job.setCurRepeatCount(settings.get(key));
                break;
            case "repeatInterval":
                job.setRepeatInterval(settings.get(key));
                break;
            case "setPartitionInterval":
                job.setPartitionInterval(settings.get(key));
                break;
            default:
                throw new Exception("Unrecognized key " + key + ".");
            }
        }

        getSchedulerJobManager().updateSchedulerJobInstance(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance deleteSchedulerJob(String name) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);
        getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance deleteSchedulerJob(SchedulerJobInstance job) throws IOException, SchedulerException {
        if (cubeTriggerKeyMap.containsKey(job.getRelatedRealization())) {
            CronTrigger trigger = (CronTrigger) scheduler
                    .getTrigger(cubeTriggerKeyMap.get(job.getRelatedRealization()));
            JobKey jobKey = trigger.getJobKey();
            if (jobKey != null) {
                scheduler.deleteJob(jobKey);
            }
            cubeTriggerKeyMap.remove(job.getRelatedRealization());
        }
        getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }

    // SchedulerJob will be triggered once its trigger_time is set.
    public void enableSchedulerJob(SchedulerJobInstance instance) throws ParseException, SchedulerException {
        if (!validateScheduler(instance))
            return;

        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setName(instance.getName());
        jobDetail.setGroup(Scheduler.DEFAULT_GROUP);
        jobDetail.setJobClass(ScheduleBuildJob.class);

        JobDataMap dataMap = jobDetail.getJobDataMap();
        dataMap.put("name", instance.getName());
        dataMap.put("cube", instance.getRelatedRealization());
        dataMap.put("startTime", instance.getPartitionStartTime());
        dataMap.put("partitionInterval", instance.getPartitionInterval());
        dataMap.put("authentication", SecurityContextHolder.getContext().getAuthentication());
        dataMap.put("schedulerJobService", this);
        dataMap.put("jobService", jobService);
        jobDetail.setJobDataMap(dataMap);

        CronExpression cronExp = new CronExpression(
                createCronExpression(instance.getRepeatInterval(), instance.getScheduledRunTime()));
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExp);

        CronTrigger trigger = null;

        if (cubeTriggerKeyMap.containsKey(instance.getRelatedRealization())) {
            trigger = (CronTrigger) scheduler.getTrigger(cubeTriggerKeyMap.get(instance.getRelatedRealization()));
            JobKey jobKey = trigger.getJobKey();

            if (jobKey != null) {
                scheduler.deleteJob(jobKey);
            }
        }
        trigger = TriggerBuilder.newTrigger().startAt(new Date(instance.getScheduledRunTime()))
                .withSchedule(cronScheduleBuilder).build();
        cubeTriggerKeyMap.put(instance.getRelatedRealization(), trigger.getKey());
        scheduler.scheduleJob(jobDetail, trigger);
    }

    public void pauseScheduler(String cubeName) throws SchedulerException {
        scheduler.pauseTrigger(cubeTriggerKeyMap.get(cubeName));
    }

    public void resumeSchedulers() {
        List<SchedulerJobInstance> schedulerList = null;
        try {
            schedulerList = listAllSchedulerJobs();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        for (SchedulerJobInstance schedulerInstance : schedulerList) {
            try {
                enableSchedulerJob(schedulerInstance);
            } catch (ParseException e) {
                throw new RuntimeException(e.getMessage(), e);
            } catch (SchedulerException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private boolean validateScheduler(SchedulerJobInstance instance) {
        boolean isValid = true;

        if (!StringUtils.isNoneBlank(instance.getName(), instance.getProject(), instance.getRealizationType(),
                instance.getRelatedRealization(), instance.getRelatedRealizationUuid())) {
            isValid = false;
        }

        if (instance.getScheduledRunTime() <= 0 || instance.getPartitionInterval() <= 0
                || instance.getRepeatInterval() <= 0) {
            isValid = false;
        }

        return isValid;
    }
}
