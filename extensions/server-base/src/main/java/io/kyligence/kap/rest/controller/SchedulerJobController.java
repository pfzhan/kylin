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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.ScheduleBuildJob;
import io.kyligence.kap.rest.request.ScheduleJobRequest;
import io.kyligence.kap.rest.service.SchedulerJobService;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.JobService;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "schedulers")
public class SchedulerJobController extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobController.class);

    @Autowired
    @Qualifier("schedulerJobService")
    private SchedulerJobService schedulerJobService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    private Scheduler scheduler;


    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {

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
        Calendar cal =  Calendar.getInstance();
        cal.setTime(firstTriggerDate);

        if(intervalTimeMillis < 0) {
            sb.append(cal.get(Calendar.SECOND) + " " +  cal.get(Calendar.MINUTE) + " " + cal.get(Calendar.HOUR_OF_DAY) + " " + cal.get(Calendar.DAY_OF_MONTH) +  " * ?");
        } else {
            long minutes = intervalTimeMillis / (60 * 1000);
            long hours = minutes / 60;
            long days = hours / 24;
            if(days > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " +  cal.get(Calendar.MINUTE) + " " + cal.get(Calendar.HOUR_OF_DAY) + " " + cal.get(Calendar.DAY_OF_MONTH) + "/" + days + " * ?");
            } else if(hours > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " +  cal.get(Calendar.MINUTE) + " " + cal.get(Calendar.HOUR_OF_DAY) + "/" + hours  + " *  * ?");
            } else if(minutes > 0) {
                sb.append(cal.get(Calendar.SECOND) + " " +  cal.get(Calendar.MINUTE) + "/" + minutes + " * * * ?");
            } else {
                sb.append(cal.get(Calendar.SECOND) + "/" + intervalTimeMillis / 1000 + " * * * * ?");
            }
        }
        return sb.toString();
    }

    /**
     * get all scheduler jobs
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<SchedulerJobInstance> getSchedulerJobs(@RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {

        List<SchedulerJobInstance> jobs;
        try {
            jobs = schedulerJobService.listAllSchedulerJobs(projectName, cubeName, limit, offset);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        int climit = (null == limit) ? jobs.size() : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (jobs.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - coffset) < climit) {
            return jobs.subList(coffset, jobs.size());
        }

        return jobs.subList(coffset, coffset + climit);
    }

    /**
     * get scheduler job with name
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{schedulerName}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public SchedulerJobInstance getSchedulerJob(@PathVariable String schedulerName) {
        SchedulerJobInstance job = null;
        try {
            job = schedulerJobService.getSchedulerJobManager().getSchedulerJob(schedulerName);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
        if (job == null) {
            throw new InternalErrorException("Cannot find scheduler job " + schedulerName);
        }
        return job;
    }

    /** Schedule a cube build */
    @RequestMapping(value = "/{name}/{project}/{cubeName}/schedule", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public SchedulerJobInstance scheduleJob(@PathVariable String name, @PathVariable String project, @PathVariable String cubeName, @RequestBody ScheduleJobRequest req) throws SchedulerException, ParseException {

        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setName(name);
        jobDetail.setGroup(scheduler.DEFAULT_GROUP);
        jobDetail.setJobClass(ScheduleBuildJob.class);

        JobDataMap dataMap = jobDetail.getJobDataMap();
        dataMap.put("name", name);
        dataMap.put("cube", cubeName);
        dataMap.put("startTime", req.getStartTime());
        dataMap.put("partitionInterval", req.getPartitionInterval());
        dataMap.put("authentication", SecurityContextHolder.getContext().getAuthentication());
        dataMap.put("schedulerJobService", schedulerJobService);
        dataMap.put("jobService", jobService);
        jobDetail.setJobDataMap(dataMap);

        CronExpression cronExp = new CronExpression(createCronExpression(req.getRepeatInterval(), req.getTriggerTime()));
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExp);
        CronTrigger trigger = TriggerBuilder.newTrigger().startAt(new Date(req.getTriggerTime())).withSchedule(cronScheduleBuilder).build();

        SchedulerJobInstance jobInstance;

        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        try {
            jobInstance = schedulerJobService.saveSchedulerJob(name, project, cubeName, req.getTriggerTime(), req.getStartTime(), req.getRepeatCount(), 0, req.getRepeatInterval(), req.getPartitionInterval());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        } catch (JobException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        return jobInstance;
    }

    /**
     * Remove a scheduler job
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{name}/delete", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public SchedulerJobInstance delete(@PathVariable String name) {
        SchedulerJobInstance jobInstance;

        try {
            jobInstance = schedulerJobService.getSchedulerJob(name);
            schedulerJobService.deleteSchedulerJob(jobInstance.getName());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

        try {
            JobKey  jobKey = JobKey.jobKey(name, scheduler.DEFAULT_GROUP);
            scheduler.deleteJob(jobKey);
        } catch (SchedulerException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

        return jobInstance;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public void setSchedulerJobService(SchedulerJobService schedulerJobService) {
        this.schedulerJobService = schedulerJobService;
    }

}
