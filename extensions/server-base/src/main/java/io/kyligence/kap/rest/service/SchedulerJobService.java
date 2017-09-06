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
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobManager;
import io.kyligence.kap.rest.ScheduleBuildJob;

@Component("schedulerJobService")
public class SchedulerJobService extends BasicService implements InitializingBean {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobService.class);

    private Scheduler scheduler;

    private Map<String, TriggerKey> cubeTriggerKeyMap = new HashMap<>();

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
        scheduler = createScheduler();
        scheduler.start();
        resumeSchedulers();
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
        }
        return sb.toString();
    }

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
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(SchedulerJobInstance job, IRealization realization, ProjectInstance project)
            throws IOException {
        if (job.getUuid() == null)
            job.updateRandomUuid();

        getSchedulerJobManager().addSchedulerJob(job);

        accessService.init(job, AclPermission.ADMINISTRATION);

        if (realization instanceof AclEntity)
            accessService.inherit(job, (AclEntity) realization);
        return job;
    }

    // test only
    SchedulerJobInstance saveSchedulerJob(String name, String project, String cube, boolean enabled, long triggerTime,
            long startTime, long repeatCount, long curRepeatCount, long repeatInterval, long partitionInterval)
            throws IOException {

        SchedulerJobInstance job = new SchedulerJobInstance(name, project, "cube", cube, enabled, startTime,
                triggerTime, repeatCount, curRepeatCount, repeatInterval, partitionInterval);
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }



    public SchedulerJobInstance updateSchedulerJobInternal(SchedulerJobInstance job, Map<String, Long> settings)
            throws Exception {
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

    public SchedulerJobInstance deleteSchedulerJobInternal(String name) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);

        if (job != null)
            getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }

    public SchedulerJobInstance deleteSchedulerJob(SchedulerJobInstance job, ProjectInstance project)
            throws IOException, SchedulerException {
        aclEvaluate.hasProjectOperationPermission(project);
        disableSchedulerJob(job, project);
        getSchedulerJobManager().removeSchedulerJob(job);
        accessService.clean(job, true);
        return job;
    }

    // only for test
    public SchedulerJobInstance deleteSchedulerJob(String name) throws IOException {
        return deleteSchedulerJobInternal(name);
    }

    public SchedulerJobInstance disableSchedulerJob(SchedulerJobInstance job,  ProjectInstance project)
            throws IOException, SchedulerException {
        aclEvaluate.hasProjectOperationPermission(project);
        if (cubeTriggerKeyMap.containsKey(job.getRelatedRealization())) {
            Trigger trigger = scheduler.getTrigger(cubeTriggerKeyMap.get(job.getRelatedRealization()));
            if (trigger != null) {
                JobKey jobKey = trigger.getJobKey();

                if (jobKey != null) {
                    scheduler.deleteJob(jobKey);
                    logger.info("Job of " + job.getName() + " is disabled.");
                }
            }
            cubeTriggerKeyMap.remove(job.getRelatedRealization());
        }
        job.setEnabled(false);
        getSchedulerJobManager().updateSchedulerJobInstance(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public SchedulerJobInstance cloneSchedulerJob(SchedulerJobInstance job, String newJobName,
            String newRealizationUuid, long partitionStartTime, CubeInstance newCube, ProjectInstance project)
            throws IOException, ParseException, SchedulerException {
        SchedulerJobInstance newJob = job.getCopyOf();
        newJob.setName(newJobName);
        newJob.setRelatedRealization(newJobName);
        newJob.setRelatedRealizationUuid(newRealizationUuid);
        newJob.setPartitionStartTime(partitionStartTime);
        newJob.setCurRepeatCount(0);

        newJob.setEnabled(false);
        saveSchedulerJob(newJob, newCube, project);

        getSchedulerJobManager().reloadSchedulerJobLocal(newJobName);
        return newJob;
    }

    public void enableSchedulerJob(SchedulerJobInstance job, ProjectInstance project)
            throws ParseException, SchedulerException, IOException {
        if (!validateScheduler(job))
            return;

        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setName(job.getName());
        jobDetail.setGroup(Scheduler.DEFAULT_GROUP);
        jobDetail.setJobClass(ScheduleBuildJob.class);

        JobDataMap dataMap = jobDetail.getJobDataMap();
        dataMap.put("name", job.getName());
        dataMap.put("realization", job.getRelatedRealization());
        dataMap.put("startTime", job.getPartitionStartTime());
        dataMap.put("partitionInterval", job.getPartitionInterval());
        dataMap.put("user", "JOB_SCHEDULER");
        jobDetail.setJobDataMap(dataMap);

        long localTimestamp = utcLocalConvert(job.getScheduledRunTime(), true);
        Date startTime = new Date(localTimestamp);
        Trigger trigger = null;

        if (cubeTriggerKeyMap.containsKey(job.getRelatedRealization())) {
            trigger = scheduler.getTrigger(cubeTriggerKeyMap.get(job.getRelatedRealization()));
            JobKey jobKey = trigger.getJobKey();

            if (jobKey != null) {
                scheduler.deleteJob(jobKey);
            }
        }

        if (job.getRepeatInterval() < 0) {
            CronExpression cronExp = new CronExpression(createCronExpression(job.getRepeatInterval(), localTimestamp));
            CronScheduleBuilder builder = CronScheduleBuilder.cronSchedule(cronExp);
            trigger = TriggerBuilder.newTrigger().startAt(startTime).withSchedule(builder)
                    .withDescription("Cron scheduler with expression = " + cronExp).build();
        } else {
            SimpleScheduleBuilder builder = null;
            long minutes = job.getRepeatInterval() / (60 * 1000);
            long hours = minutes / 60;
            String timeDesc = null;

            if (hours > 0 && minutes % 60 == 0) {
                builder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours((int) hours).repeatForever();
                timeDesc = hours + "hours.";
            } else if (minutes > 0) {
                builder = SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes((int) minutes).repeatForever();
                timeDesc = minutes + "minutes.";
            } else {
                builder = SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds((int) (job.getRepeatInterval() / 1000)).repeatForever();
                timeDesc = (job.getRepeatInterval() / 1000) + "seconds.";
            }

            trigger = TriggerBuilder.newTrigger().startAt(startTime).withSchedule(builder)
                    .withDescription("Simple scheduler with interval of " + timeDesc).build();
        }

        logger.info("Scheduler of cube " + job.getRelatedRealization() + " is scheduled to be run first at "
                + startTime.toString() + " with description: " + trigger.getDescription());
        cubeTriggerKeyMap.put(job.getRelatedRealization(), trigger.getKey());
        scheduler.scheduleJob(jobDetail, trigger);
        job.setEnabled(true);
        getSchedulerJobManager().updateSchedulerJobInstance(job);
    }

    public void pauseScheduler(String cubeName) throws SchedulerException {
        scheduler.pauseTrigger(cubeTriggerKeyMap.get(cubeName));
    }

    public void updateSchedulerRealizationType(SchedulerJobInstance job, String realizationType) throws IOException {
        job.setRealizationType(realizationType);
        getSchedulerJobManager().updateSchedulerJobInstance(job);
    }

    public void resumeSchedulers() throws IOException {
        List<SchedulerJobInstance> schedulerList = null;
        try {
            schedulerList = listAllSchedulerJobs();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        for (SchedulerJobInstance schedulerInstance : schedulerList) {
            try {
                if (schedulerInstance.isEnabled()) {
                    enableSchedulerJob(schedulerInstance, getProjectManager().getProject(schedulerInstance.getProject()));
                }
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

    public long utcLocalConvert(long baseTime, boolean utc2Local) {
        Calendar calendar = Calendar.getInstance();

        int zoneOffset = calendar.get(java.util.Calendar.ZONE_OFFSET);
        int dstOffset = calendar.get(java.util.Calendar.DST_OFFSET);

        calendar.setTimeInMillis(baseTime);

        if (utc2Local) {
            calendar.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        } else {
            calendar.add(java.util.Calendar.MILLISECOND, zoneOffset + dstOffset);
        }

        return calendar.getTimeInMillis();
    }

}
