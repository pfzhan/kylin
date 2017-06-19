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
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ProjectService;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
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
    @Qualifier("projectService")
    private ProjectService projectService;

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

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
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
     * @return number of cuboid, return -1 on agg not found
     */

    @RequestMapping(value = "aggregationgroups/{aggIndex}/cuboid", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse calculateAggCuboidCombination(@PathVariable int aggIndex, @RequestBody String cubeDescStr)
            throws IOException {

        CubeDesc cubeDesc = deserializeCubeDesc(cubeDescStr);
        cubeDesc.init(KylinConfig.getInstanceFromEnv());
        AggregationGroup aggregationGroup = cubeDesc.getAggregationGroups().get(aggIndex);

        if (aggregationGroup != null) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggregationGroup.calculateCuboidCombination(), "");
        } else
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, -1, "");
    }

    @RequestMapping(value = "/cuboid", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse calculateCubeCuboidCombination(@RequestBody String cubeDescStr) throws IOException {
        CubeDesc cubeDesc = deserializeCubeDesc(cubeDescStr);
        cubeDesc.init(KylinConfig.getInstanceFromEnv());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeDesc.getAllCuboids().size(), "");
    }

    private CubeDesc deserializeCubeDesc(String cubeDescStr) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.trace("Saving cube " + cubeDescStr);
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
    public EnvelopeResponse updateCubeDesc(@RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, ParseException, SchedulerException {

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, false);

        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        rawTableService.validateRawTableDesc(rawTableDesc);

        SchedulerJobInstance schedule = deserializeSchedulerJobInstance(kapCubeRequest);
        schedulerJobService.pauseScheduler(cubeDesc.getName());

        String project = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            cubeDesc = cubeService.updateCubeToResourceStore(cubeDesc, project);

            if (rawTableDesc != null) {
                rawTableDesc.setName(cubeDesc.getName());
                rawTableDesc = rawTableService.updateRawTableToResourceStore(rawTableDesc, project);
            } else {
                rawTableService.deleteRawByName(cubeDesc.getName());
            }

            if (schedule != null) {
                bindSchedulerJobWithCube(schedule, cubeDesc.getName(), cubeDesc.getUuid());
                schedulerJobService.enableSchedulerJob(schedule);
            }

            // remove any previous draft
            cubeService.getDraftManager().delete(cubeDesc.getUuid());

        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(project);
            throw ex;
        } finally {
            cp.close();
        }

        return buildUpdateCubeDescResponse(cubeDesc, rawTableDesc, schedule);
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescDraft(@RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, SchedulerException {

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, true);

        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        rawTableService.validateRawTableDesc(rawTableDesc);

        SchedulerJobInstance schedule = deserializeSchedulerJobInstance(kapCubeRequest);

        String project = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();

        if (cubeDesc.getUuid() == null)
            cubeDesc.updateRandomUuid();
        cubeDesc.setDraft(true);

        if (rawTableDesc != null) {
            rawTableDesc.setDraft(true);
        }

        cubeService.getDraftManager().save(project, cubeDesc.getUuid(), cubeDesc, rawTableDesc, schedule);

        return buildUpdateCubeDescResponse(cubeDesc, rawTableDesc, schedule);
    }

    private EnvelopeResponse buildUpdateCubeDescResponse(CubeDesc cubeDesc, RawTableDesc rawTableDesc,
            SchedulerJobInstance schedule) throws JsonProcessingException {
        GeneralResponse result = new GeneralResponse();
        result.setProperty("cubeUuid", cubeDesc.getUuid());
        result.setProperty("cubeDescData", JsonUtil.writeValueAsIndentString(cubeDesc));
        if (rawTableDesc != null)
            result.setProperty("rawTableDescData", JsonUtil.writeValueAsIndentString(rawTableDesc));
        if (schedule != null)
            result.setProperty("schedulerJobData", JsonUtil.writeValueAsIndentString(schedule));

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{cubeName}/scheduler_job", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJob(@PathVariable String cubeName) throws IOException {

        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        Draft draft = cubeService.getCubeDraft(cubeName);

        // skip checking job/draft being null, schedulerJob not exist is fine

        HashMap<String, SchedulerJobInstance> result = new HashMap<>();
        if (job != null) {
            result.put("schedulerJob", job);
        }
        if (draft != null) {
            SchedulerJobInstance djob = (SchedulerJobInstance) draft.getEntities()[2];
            result.put("draft", djob);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{cubeName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteCubeAndDraft(@PathVariable String cubeName) throws IOException, SchedulerException {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube != null) {
            String project = projectService.getProjectOfCube(cube.getName());

            ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            ResourceStore.Checkpoint cp = store.checkpoint();
            try {
                deleteCube(cubeName);
            } catch (Exception ex) {
                cp.rollback();
                cacheService.wipeProjectCache(project);
                throw ex;
            } finally {
                cp.close();
            }
        }

        // delete draft is not critical and can be out of checkpoint/rollback
        Draft draft = cubeService.getCubeDraft(cubeName);
        if (draft != null)
            cubeService.getDraftManager().delete(draft.getUuid());
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneCubeV2(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest)
            throws IOException {
        Message msg = MsgPicker.getMsg();
        String newCubeName = cubeRequest.getCubeName();
        String project = cubeRequest.getProject();

        CubeDesc oldCubeDesc = kapCubeService.getCubeDescManager().getCubeDesc(cubeName);
        CubeInstance oldCube = kapCubeService.getCubeManager().getCube(cubeName);
        if (oldCubeDesc == null || oldCube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        CubeInstance newCube = null;
        try {
            newCube = kapCubeService.cubeClone(oldCube, newCubeName, project);
            RawTableInstance oldRaw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (oldRaw != null) {
                logger.debug("Cube " + cubeName + " contains rawtable, clone it");
                rawTableService.cloneRaw(cubeName, newCubeName, project);
            }
            SchedulerJobInstance job = schedulerJobService.getSchedulerJob(cubeName);
            if (job != null) {
                schedulerJobService.cloneSchedulerJob(job, newCubeName, newCube.getUuid());
            }
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(project);
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, newCube, "");
    }

    private void deleteCube(String cubeName) throws IOException, SchedulerException {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube != null) {
            deleteScheduler(cubeName);
            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (null != raw) {
                rawTableService.deleteRaw(raw);
            }
            cubeService.deleteCube(cube);
        }
    }

    private void deleteScheduler(String cubeName) throws IOException, SchedulerException {
        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        if (job != null) {
            schedulerJobService.deleteSchedulerJob(job);
        }
    }

    private CubeDesc deserializeCubeDesc(KapCubeRequest kapCubeRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        CubeDesc desc = null;
        try {
            logger.trace("Saving cube " + kapCubeRequest.getCubeDescData());
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
            logger.trace("Saving rawtable " + kapCubeRequest.getRawTableDescData());
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
            logger.trace("Saving scheduler job " + kapCubeRequest.getSchedulerJobData());
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

    private void bindSchedulerJobWithCube(SchedulerJobInstance schedule, String cubeName, String cubeUuid)
            throws IOException, SchedulerException {
        SchedulerJobInstance older = getSchedulerJobByCubeName(cubeName);
        if (null != older)
            schedulerJobService.deleteSchedulerJob(older);

        schedule.setRelatedRealization(cubeName);
        schedule.setName(cubeName);
        schedule.setRelatedRealizationUuid(cubeUuid);
        schedule.setRealizationType("cube");

        long realScheduledRunTime = utc2Local(schedule.getScheduledRunTime());

        if (realScheduledRunTime < System.currentTimeMillis()) {
            schedule.setScheduledRunTime(System.currentTimeMillis());
        } else {
            schedule.setScheduledRunTime(realScheduledRunTime);
        }

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        Segments<CubeSegment> segments = cube.getSegments();

        if (schedule.getPartitionStartTime() < segments.getDateRangeEnd()) {
            schedule.setPartitionStartTime(segments.getDateRangeEnd());
        }
        schedulerJobService.saveSchedulerJob(schedule);
    }

    public long utc2Local(long utcTime) {
        Calendar calendar = Calendar.getInstance();

        int zoneOffset = calendar.get(java.util.Calendar.ZONE_OFFSET);
        int dstOffset = calendar.get(java.util.Calendar.DST_OFFSET);

        calendar.setTimeInMillis(utcTime);
        calendar.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));

        return calendar.getTimeInMillis();
    }

    private SchedulerJobInstance getSchedulerJobByCubeName(String cubeName) throws IOException {
        for (SchedulerJobInstance jobInstance : schedulerJobService.listAllSchedulerJobs()) {
            if (jobInstance.getRelatedRealization().equals(cubeName))
                return jobInstance;

        }
        return null;
    }
}
