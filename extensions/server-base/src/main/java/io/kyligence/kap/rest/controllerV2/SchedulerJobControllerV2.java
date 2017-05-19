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

package io.kyligence.kap.rest.controllerV2;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.ScheduleBuildJob;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.ScheduleJobRequest;
import io.kyligence.kap.rest.service.SchedulerJobServiceV2;

@Controller
@RequestMapping(value = "schedulers")
public class SchedulerJobControllerV2 extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobControllerV2.class);

    @Autowired
    @Qualifier("schedulerJobServiceV2")
    private SchedulerJobServiceV2 schedulerJobServiceV2;

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
    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJobs(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        KapMsgPicker.setMsg(lang);

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<SchedulerJobInstance> jobs = schedulerJobServiceV2.listAllSchedulerJobs(projectName, cubeName);

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (jobs.size() <= offset) {
            offset = jobs.size();
            limit = 0;
        }

        if ((jobs.size() - offset) < limit) {
            limit = jobs.size() - offset;
        }

        data.put("jobs", jobs.subList(offset, offset + limit));
        data.put("size", jobs.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * get scheduler job with name
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{schedulerName}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJob(@RequestHeader("Accept-Language") String lang, @PathVariable String schedulerName) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        SchedulerJobInstance job = schedulerJobServiceV2.getSchedulerJobManager().getSchedulerJob(schedulerName);

        if (job == null) {
            throw new BadRequestException(String.format(msg.getSCHEDULER_JOB_NOT_FOUND(), schedulerName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, job, "");
    }

    /** Schedule a cube build */
    @RequestMapping(value = "/{name}/{project}/{cubeName}/schedule", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse scheduleJob(@RequestHeader("Accept-Language") String lang, @PathVariable String name, @PathVariable String project, @PathVariable String cubeName, @RequestBody ScheduleJobRequest req) throws ParseException, SchedulerException, IOException {
        KapMsgPicker.setMsg(lang);

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
        dataMap.put("schedulerJobService", schedulerJobServiceV2);
        dataMap.put("jobService", jobService);
        jobDetail.setJobDataMap(dataMap);

        CronExpression cronExp = new CronExpression(createCronExpression(req.getRepeatInterval(), req.getTriggerTime()));
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExp);
        CronTrigger trigger = TriggerBuilder.newTrigger().startAt(new Date(req.getTriggerTime())).withSchedule(cronScheduleBuilder).build();

        SchedulerJobInstance jobInstance;

        scheduler.scheduleJob(jobDetail, trigger);

        jobInstance = schedulerJobServiceV2.saveSchedulerJob(name, project, cubeName, req.getTriggerTime(), req.getStartTime(), req.getRepeatCount(), 0, req.getRepeatInterval(), req.getPartitionInterval());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
    }

    /**
     * Remove a scheduler job
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{name}/delete", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse delete(@RequestHeader("Accept-Language") String lang, @PathVariable String name) throws IOException, SchedulerException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        SchedulerJobInstance jobInstance;

        jobInstance = schedulerJobServiceV2.getSchedulerJob(name);
        schedulerJobServiceV2.deleteSchedulerJob(jobInstance.getName());

        JobKey jobKey = JobKey.jobKey(name, scheduler.DEFAULT_GROUP);
        scheduler.deleteJob(jobKey);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
    }

    public void setJobService(JobService jobServiceV2) {
        this.jobService = jobServiceV2;
    }

    public void setSchedulerJobService(SchedulerJobServiceV2 schedulerJobServiceV2) {
        this.schedulerJobServiceV2 = schedulerJobServiceV2;
    }

}
