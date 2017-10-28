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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
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
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KapCubeRequest;
import io.kyligence.kap.rest.response.KapCubeResponse;
import io.kyligence.kap.rest.service.KapCubeService;
import io.kyligence.kap.rest.service.RawTableService;
import io.kyligence.kap.rest.service.SchedulerJobService;

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

    @RequestMapping(value = "/validate/{cubeName}", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<Boolean> validateModelName(@PathVariable String cubeName) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, cubeService.isCubeNameVaildate(cubeName), "");
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

        List<Long> cuboidCnt = Lists.newArrayList();
        cuboidCnt.add((long) cubeDesc.getAllCuboids().size());
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            if (agg != null) {
                cuboidCnt.add(agg.calculateCuboidCombination());
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cuboidCnt, "");
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
        Message msg = MsgPicker.getMsg();

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, false);

        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        rawTableService.validateRawTableDesc(rawTableDesc);

        SchedulerJobInstance schedule = deserializeSchedulerJobInstance(kapCubeRequest);
        schedulerJobService.pauseScheduler(cubeDesc.getName());

        validateMPMaster(cubeDesc, rawTableDesc, schedule);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();
        ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
        if (project == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), projectName));
        }

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        try {
            if (cubeDesc.getLastModified() == 0) {
                cubeDesc = cubeService.saveCube(cubeDesc, project);
            } else {
                CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeDesc.getName());
                if (cubeInstance == null) {
                    throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeDesc.getName()));
                }
                cubeDesc = cubeService.updateCube(cubeInstance, cubeDesc, project);
            }

            CubeInstance cube = cubeService.getCubeManager().getCube(cubeDesc.getName());
            if (rawTableDesc != null) {
                rawTableDesc.setName(cubeDesc.getName());
                if (rawTableDesc.getLastModified() == 0) {
                    rawTableDesc = rawTableService.saveRaw(rawTableDesc, cube, project);
                } else {
                    RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeDesc.getName());
                    rawTableDesc = rawTableService.updateRaw(raw, rawTableDesc, cube, project);
                }
            } else {
                RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeDesc.getName());
                if (raw != null) {
                    rawTableService.deleteRaw(raw, cube);
                }
            }

            if (schedule != null) {
                schedule.setPartitionStartTime(cubeDesc.getPartitionDateStart());
                bindSchedulerJobWithCube(schedule, cubeDesc.getName(), cubeDesc.getUuid(), project);
                if (schedule.isEnabled()) {
                    schedulerJobService.enableSchedulerJob(schedule, cube);
                } else {
                    schedulerJobService.disableSchedulerJob(schedule, cube);
                }
            } else {
                //                SchedulerJobInstance localSchedule = schedulerJobService.getSchedulerJobManager()
                //                        .getSchedulerJob(cubeDesc.getName());
                //                if (localSchedule != null) {
                //                    schedulerJobService.deleteSchedulerJob(localSchedule, cube);
                //                }
            }

            // remove any previous draft
            cubeService.getDraftManager().delete(cubeDesc.getUuid());

        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(projectName);
            throw ex;
        } finally {
            cp.close();
        }

        return buildUpdateCubeDescResponse(cubeDesc, rawTableDesc, schedule);
    }

    private void validateMPMaster(CubeDesc cubeDesc, RawTableDesc rawTableDesc, SchedulerJobInstance schedule) {
        KapModel model = (KapModel) cubeService.getDataModelManager().getDataModelDesc(cubeDesc.getModelName());
        if (model.isMultiLevelPartitioned() == false)
            return;

        // note cubeDesc.getModel() is not inited yet
        if (rawTableDesc != null || (schedule != null && schedule.isEnabled())) {
            KapMessage msg = KapMsgPicker.getMsg();
            throw new BadRequestException(msg.getMPCUBE_HATES_RAW_AND_SCHEDULER());
        }

        // make MP columns mandatory in aggregation groups
        kapCubeService.validateMPDimensions(cubeDesc, model);
        TblColRef[] mpCols = model.getMutiLevelPartitionCols();
        for (AggregationGroup aggr : cubeDesc.getAggregationGroups()) {
            aggr.setIncludes(addColsToStrs(aggr.getIncludes(), mpCols));
            SelectRule selectRule = aggr.getSelectRule();
            selectRule.mandatoryDims = addColsToStrs(selectRule.mandatoryDims, mpCols);
        }
    }

    private String[] addColsToStrs(String[] strs, TblColRef[] mpCols) {
        if (strs == null)
            strs = new String[0];

        for (TblColRef col : mpCols) {
            String colStr = col.getIdentity();
            if (ArrayUtils.contains(strs, colStr) == false) {
                strs = (String[]) ArrayUtils.add(strs, colStr);
            }
        }
        return strs;
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDescDraft(@RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, SchedulerException {
        Message msg = MsgPicker.getMsg();

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, true);

        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        rawTableService.validateRawTableDesc(rawTableDesc);

        SchedulerJobInstance schedule = deserializeSchedulerJobInstance(kapCubeRequest);

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();

        ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
        if (project == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), projectName));
        }

        if (cubeDesc.getUuid() == null)
            cubeDesc.updateRandomUuid();
        cubeDesc.setDraft(true);

        if (rawTableDesc != null) {
            rawTableDesc.setDraft(true);
        }

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeDesc.getName());
        if (cube != null) {
            // Official Cube exist
            cubeService.saveDraft(project, cube, cubeDesc.getUuid(), cubeDesc, rawTableDesc, schedule);
        } else {
            // Official Cube absent
            cubeService.saveDraft(project, cubeDesc.getUuid(), cubeDesc, rawTableDesc, schedule);
        }

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

    @RequestMapping(value = "/{projectName}/{cubeName}/scheduler_job", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJob(@PathVariable String projectName, @PathVariable String cubeName)
            throws IOException {
        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        Draft draft = cubeService.getCubeDraft(cubeName, projectName);

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

    @RequestMapping(value = "/{projectName}/{cubeName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteCubeAndDraft(@PathVariable String projectName, @PathVariable String cubeName)
            throws IOException, SchedulerException {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube != null) {

            checkCubeIsEmpty(cubeName);

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
        Draft draft = cubeService.getCubeDraft(cubeName, projectName);
        if (draft != null) {
            cubeService.deleteDraft(draft);
            kapCubeService.deleteCubeOptLog(cubeName);
        }
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneCubeV2(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest)
            throws IOException, ParseException, SchedulerException {
        Message msg = MsgPicker.getMsg();
        String newCubeName = cubeRequest.getCubeName();
        String projectName = cubeRequest.getProject();

        CubeDesc oldCubeDesc = kapCubeService.getCubeDescManager().getCubeDesc(cubeName);
        CubeInstance oldCube = kapCubeService.getCubeManager().getCube(cubeName);
        if (oldCubeDesc == null || oldCube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        CubeInstance newCube = null;
        try {
            ProjectInstance project = kapCubeService.getProjectManager().getProject(projectName);
            if (project == null) {
                throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), projectName));
            }

            newCube = kapCubeService.cubeClone(oldCube, newCubeName, project);
            RawTableInstance oldRaw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (oldRaw != null) {
                logger.debug("Cube " + cubeName + " contains rawtable, clone it");
                rawTableService.cloneRaw(oldRaw, newCubeName, newCube, project);
            }
            SchedulerJobInstance job = schedulerJobService.getSchedulerJob(cubeName);
            if (job != null) {
                schedulerJobService.cloneSchedulerJob(job, newCubeName, newCube.getUuid(),
                        newCube.getDescriptor().getPartitionDateStart(), newCube, project);
            }
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(projectName);
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createCubeResponse(newCube), "");
    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse enableCubeV2(@PathVariable String cubeName) throws IOException {
        Message msg = MsgPicker.getMsg();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        CubeInstance cube = null;
        try {
            cube = cubeService.getCubeManager().getCube(cubeName);
            if (cube == null) {
                throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
            }

            kapCubeService.checkEnableCubeCondition(cube);
            cube = cubeService.enableCube(cube);

            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (raw != null) {
                rawTableService.enableRaw(raw, cube);
            }
        } catch (Exception ex) {
            cp.rollback();
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createCubeResponse(cube), "");
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse disableCubeV2(@PathVariable String cubeName) throws IOException, SchedulerException {
        Message msg = MsgPicker.getMsg();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        CubeInstance cube = null;
        try {
            cube = cubeService.getCubeManager().getCube(cubeName);

            if (cube == null) {
                throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
            }

            cube = cubeService.disableCube(cube);

            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (raw != null) {
                rawTableService.disableRaw(raw, cube);
            }

            SchedulerJobInstance schedulerJobInstance = schedulerJobService.getSchedulerJobManager()
                    .getSchedulerJob(cubeName);
            if (schedulerJobInstance != null) {
                schedulerJobService.disableSchedulerJob(schedulerJobInstance, cube);
            }
        } catch (Exception ex) {
            cp.rollback();
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createCubeResponse(cube), "");

    }

    private KapCubeResponse createCubeResponse(CubeInstance cube) {
        return KapCubeResponse.create(cube, kapCubeService);
    }

    private void deleteCube(String cubeName) throws IOException, SchedulerException {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube != null) {
            deleteScheduler(cubeName, cube);
            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (null != raw) {
                rawTableService.deleteRaw(raw, cube);
            }
            cubeService.deleteCube(cube);
        }
    }

    private void deleteScheduler(String cubeName, CubeInstance cube) throws IOException, SchedulerException {
        SchedulerJobInstance job = getSchedulerJobByCubeName(cubeName);
        if (job != null) {
            schedulerJobService.deleteSchedulerJob(job, cube);
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

        if (kapCubeRequest.getSchedulerJobData() == null)
            return schedulerJob;

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

    private void bindSchedulerJobWithCube(SchedulerJobInstance schedule, String cubeName, String cubeUuid,
            ProjectInstance project) throws IOException, SchedulerException {
        SchedulerJobInstance older = getSchedulerJobByCubeName(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null != older)
            schedulerJobService.deleteSchedulerJob(older, cube);

        schedule.setRelatedRealization(cubeName);
        schedule.setName(cubeName);
        schedule.setRelatedRealizationUuid(cubeUuid);
        schedule.setRealizationType("cube");

        long localRunTime = schedulerJobService.utcLocalConvert(schedule.getScheduledRunTime(), true);

        if (localRunTime < System.currentTimeMillis()) {
            schedule.setScheduledRunTime(schedulerJobService.utcLocalConvert(System.currentTimeMillis(), false));
        } else {
            schedule.setScheduledRunTime(schedule.getScheduledRunTime());
        }

        Segments<CubeSegment> segments = cube.getSegments();

        if (schedule.getPartitionStartTime() < segments.getTSEnd()) {
            schedule.setPartitionStartTime(segments.getTSEnd());
        }
        schedulerJobService.saveSchedulerJob(schedule, cube, project);
    }

    private SchedulerJobInstance getSchedulerJobByCubeName(String cubeName) throws IOException {
        for (SchedulerJobInstance jobInstance : schedulerJobService.listAllSchedulerJobs()) {
            if (jobInstance.getRelatedRealization().equals(cubeName))
                return jobInstance;

        }
        return null;
    }

    private void checkCubeIsEmpty(String cubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        CubeManager cmmgr = CubeManager.getInstance(cubeService.getConfig());
        MPCubeManager mpmgr = MPCubeManager.getInstance(cubeService.getConfig());
        CubeInstance cubeInstance = cmmgr.getCube(cubeName);

        if (mpmgr.isCommonCube(cubeInstance)) {
            Segments<CubeSegment> segments = cubeInstance.getSegments();
            if (segments.isEmpty() == false) {
                throw new BadRequestException(msg.getRAW_SEG_SIZE_NOT_NULL());
            }
        } else if (mpmgr.isMPMaster(cubeInstance)) {
            List<CubeInstance> mpcubeList = mpmgr.listMPCubes(cubeInstance);
            if (mpcubeList.isEmpty() == false) {
                throw new BadRequestException(msg.getRAW_SEG_SIZE_NOT_NULL());
            }
        } else {
            throw new IllegalArgumentException(cubeName + " must not be a MPCube");
        }
    }
}
