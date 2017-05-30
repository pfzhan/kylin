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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.CubeService;
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
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.ScheduleBuildJob;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KapCubeRequest;
import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.rest.service.KapCubeService;
import io.kyligence.kap.rest.service.RawTableService;
import io.kyligence.kap.rest.service.SchedulerJobService;
import io.kyligence.kap.storage.parquet.steps.ColumnarStorageUtils;

@Controller
@RequestMapping(value = "/cubes")
public class KapCubeController extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(KapCubeController.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("kapCubeService")
    private KapCubeService kapCubeService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Autowired
    @Qualifier("schedulerJobService")
    private SchedulerJobService schedulerJobService;

    @Autowired
    @Qualifier("rawTableService")
    private RawTableService rawTableService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    private Scheduler scheduler;

    private Map<String, TriggerKey> cubeTriggerKeyMap = new HashMap<>();

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

    /**
     * get Columnar Info
     *
     * @return true
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/columnar", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getColumnarInfo(@PathVariable String cubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        List<ColumnarResponse> columnar = new ArrayList<>();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        for (CubeSegment segment : cube.getSegments()) {
            final KylinConfig config = KylinConfig.getInstanceFromEnv();
            String storagePath = ColumnarStorageUtils.getSegmentDir(config, cube, segment);

            ColumnarResponse info;
            try {
                info = kapCubeService.getColumnarInfo(storagePath, segment);
            } catch (IOException ex) {
                logger.error("Can't get columnar info, cube {}, segment {}:", cube, segment);
                logger.error("{}", ex);
                continue;
            }

            columnar.add(info);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, columnar, "");
    }

    /**
     * Calculate Cuboid Combination based on the AggreationGroup definition.
     *
     * @param cubeDescStr
     * @return number of cuboid, -1 if failed
     */

    @RequestMapping(value = "aggregationgroups/{aggIndex}/cuboid", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse calculateCuboidCombinationV2(@PathVariable int aggIndex, @RequestBody String cubeDescStr)
            throws IOException {

        CubeDesc cubeDesc = deserializeCubeDescV2(cubeDescStr);
        AggregationGroup aggregationGroup = cubeDesc.getAggregationGroups().get(aggIndex);

        if (aggregationGroup != null) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggregationGroup.calculateCuboidCombination(), "");
        } else
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, -1, "");
    }

    private CubeDesc deserializeCubeDescV2(String cubeDescStr) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + cubeDescStr);
            desc = JsonUtil.readValue(cubeDescStr, CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }
        return desc;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescV2(@RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, ParseException, SchedulerException {

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        scheduler.pauseTrigger(cubeTriggerKeyMap.get(cubeDesc.getName()));
        cubeService.validateCubeDesc(cubeDesc, false);
        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        SchedulerJobInstance schedulerJobInstance = deserializeSchedulerJobInstance(kapCubeRequest);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            boolean createNewCube = cubeService.unifyCubeDesc(cubeDesc, false);
            cubeDesc = cubeService.updateCubeToResourceStore(cubeDesc, projectName, createNewCube);

            if (rawTableDesc != null) {
                rawTableService.validateRawTableDesc(rawTableDesc);
                rawTableDesc.setUuid(cubeDesc.getUuid());
                boolean createNewRawTable = rawTableService.unifyRawTableDesc(rawTableDesc, false);
                rawTableDesc = rawTableService.updateRawTableToResourceStore(rawTableDesc, projectName,
                        createNewRawTable);
            } else {
                rawTableService.deleteRawByUuid(cubeDesc.getUuid(), false);
            }

            schedulerJobService.unifySchedulerJobInstance(schedulerJobInstance);
            bindSchedulerJobWithCube(schedulerJobInstance, cubeDesc.getName(), cubeDesc.getUuid());
            validateSchedulerJobs(cubeDesc.getUuid());
            enableSchedulerJob(schedulerJobInstance);
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String cubeDescData = JsonUtil.writeValueAsIndentString(cubeDesc);
        String rawTableDescData = JsonUtil.writeValueAsIndentString(rawTableDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("cubeUuid", cubeDesc.getUuid());
        data.setProperty("cubeDescData", cubeDescData);
        if (rawTableDesc != null) {
            data.setProperty("rawTableUuid", rawTableDesc.getUuid());
            data.setProperty("rawTableDescData", rawTableDescData);
            data.setProperty("schedulerJobData", kapCubeRequest.getSchedulerJobData());
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescDraftV2(@RequestBody KapCubeRequest kapCubeRequest) throws IOException {

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, true);
        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        SchedulerJobInstance schedulerJobInstance = deserializeSchedulerJobInstance(kapCubeRequest);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            boolean createNewCube = cubeService.unifyCubeDesc(cubeDesc, true);
            cubeDesc = cubeService.updateCubeToResourceStore(cubeDesc, projectName, createNewCube);

            if (rawTableDesc != null) {
                rawTableService.validateRawTableDesc(rawTableDesc);
                rawTableDesc.setUuid(cubeDesc.getUuid());
                boolean createNewRawTable = rawTableService.unifyRawTableDesc(rawTableDesc, true);
                rawTableDesc = rawTableService.updateRawTableToResourceStore(rawTableDesc, projectName,
                        createNewRawTable);
            } else {
                rawTableService.deleteRawByUuid(cubeDesc.getUuid(), true);
            }

            schedulerJobService.unifySchedulerJobInstance(schedulerJobInstance);
            bindSchedulerJobWithCube(schedulerJobInstance, cubeDesc.getName(), cubeDesc.getUuid());
            validateSchedulerJobs(cubeDesc.getUuid());
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String cubeDescData = JsonUtil.writeValueAsIndentString(cubeDesc);
        String rawTableDescData = JsonUtil.writeValueAsIndentString(rawTableDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("cubeUuid", cubeDesc.getUuid());
        data.setProperty("cubeDescData", cubeDescData);
        if (rawTableDesc != null) {
            data.setProperty("rawTableUuid", rawTableDesc.getUuid());
            data.setProperty("rawTableDescData", rawTableDescData);
            data.setProperty("schedulerJobData", kapCubeRequest.getSchedulerJobData());
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "{cubeName}/scheduler_job", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJob(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) throws IOException {
        MsgPicker.setMsg(lang);
        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, job, "");
    }

    @RequestMapping(value = "{cubeName}/scheduler_job", method = RequestMethod.DELETE, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse deleteSchedulerJob(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) throws IOException, SchedulerException {
        MsgPicker.setMsg(lang);
        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        schedulerJobService.deleteSchedulerJob(job);
        if (cubeTriggerKeyMap.containsKey(job.getRelatedRealization())) {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(cubeTriggerKeyMap.get(job.getRelatedRealization()));
            JobKey jobKey = trigger.getJobKey();
            if (jobKey != null) {
                scheduler.deleteJob(jobKey);
            }
            cubeTriggerKeyMap.remove(job.getRelatedRealization());
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, 0, "");
    }

    private CubeDesc deserializeCubeDesc(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + kapCubeRequest.getCubeDescData());
            desc = JsonUtil.readValue(kapCubeRequest.getCubeDescData(), CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }
        return desc;
    }

    private RawTableDesc deserializeRawTableDesc(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableDesc desc = null;
        if (kapCubeRequest.getRawTableDescData() == null)
            return desc;

        try {
            logger.debug("Saving rawtable " + kapCubeRequest.getRawTableDescData());
            desc = JsonUtil.readValue(kapCubeRequest.getRawTableDescData(), RawTableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        }
        return desc;
    }

    private SchedulerJobInstance deserializeSchedulerJobInstance(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        SchedulerJobInstance schedulerJob = null;
        try {
            logger.debug("Saving scheduler job " + kapCubeRequest.getSchedulerJobData());
            schedulerJob = JsonUtil.readValue(kapCubeRequest.getSchedulerJobData(), SchedulerJobInstance.class);
        } catch (JsonParseException e) {
            logger.error("The SchedulerJobInstance definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The SchedulerJobInstance definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }
        return schedulerJob;
    }

    private void bindSchedulerJobWithCube(SchedulerJobInstance younger, String cubeName, String cubeUuid)
            throws IOException {
        SchedulerJobInstance older = getSchedulerJobByCubeName(cubeName);
        if (null != older)
            schedulerJobService.deleteSchedulerJob(older);
        younger.setRelatedRealization(cubeName);
        younger.setName(cubeName);
        younger.setRelatedRealizationUuid(cubeUuid);
        schedulerJobService.saveSchedulerJob(younger);
    }

    // Keep SchedulerJob consistent with cubes.
    private void validateSchedulerJobs(String uuid) throws IOException {
        for (SchedulerJobInstance jobInstance : schedulerJobService.listAllSchedulerJobs()) {
            if (!jobInstance.getRelatedRealizationUuid().equalsIgnoreCase(uuid))
                continue;

            for (String cubeName : kapCubeService.getCubesByUuid(uuid)) {
                if (!jobInstance.getRelatedRealization().equalsIgnoreCase(cubeName)) {
                    schedulerJobService.deleteSchedulerJob(jobInstance);
                }
            }
        }
    }

    private SchedulerJobInstance getSchedulerJobByCubeName(String cubeName) throws IOException {
        for (SchedulerJobInstance jobInstance : schedulerJobService.listAllSchedulerJobs()) {
            if (jobInstance.getRelatedRealization().equalsIgnoreCase(cubeName))
                return jobInstance;

        }
        return null;
    }

    // SchedulerJob will be triggered once its trigger_time is set.
    private void enableSchedulerJob(SchedulerJobInstance instance) throws ParseException, SchedulerException {

        if (instance.getScheduledRunTime() == 0)
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
        dataMap.put("schedulerJobService", schedulerJobService);
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
        trigger = TriggerBuilder.newTrigger().startAt(new Date(instance.getScheduledRunTime())).withSchedule(cronScheduleBuilder).build();
        cubeTriggerKeyMap.put(instance.getRelatedRealization(), trigger.getKey());
        scheduler.scheduleJob(jobDetail, trigger);
    }

    private void resumeSchedulers() throws ParseException, IOException, SchedulerException {
        List<SchedulerJobInstance> schedulerList = schedulerJobService.listAllSchedulerJobs();
        for (SchedulerJobInstance schedulerInstance : schedulerList) {
            enableSchedulerJob(schedulerInstance);
        }
    }
}
