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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.rest.PagingUtil;
import io.kyligence.kap.rest.request.KapBuildRequest;
import io.kyligence.kap.rest.request.KapStreamingBuildRequest;
import io.kyligence.kap.rest.request.SegmentMgmtRequest;
import io.kyligence.kap.rest.response.KapCubeResponse;
import io.kyligence.kap.rest.service.KapCubeService;

/**
 * CubeController is defined as Restful API entrance for UI.
 */
@Controller
@RequestMapping(value = "/cubes")
public class CubeControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CubeControllerV2.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("kapCubeService")
    private KapCubeService kapCubeService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubesPaging(@RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "exactMatch", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<CubeInstanceResponse> response = new ArrayList<CubeInstanceResponse>();
        List<CubeInstance> cubes = cubeService.listAllCubes(cubeName, projectName, modelName, exactMatch);
        MPCubeManager mpCubeMgr = MPCubeManager.getInstance(cubeService.getConfig());

        // official cubes
        for (CubeInstance cube : cubes) {
            if (mpCubeMgr.isMPCube(cube))
                continue;

            try {
                response.add(createCubeResponse(cube));
            } catch (Exception e) {
                logger.error("Error creating cube instance response, skipping.", e);
            }
        }

        // draft cubes
        for (Draft d : cubeService.listCubeDrafts(cubeName, modelName, projectName, exactMatch)) {
            CubeDesc c = (CubeDesc) d.getEntity();
            if (contains(response, c.getName()) == false) {
                response.add(KapCubeResponse.create(d));
            }
        }

        data.put("cubes", PagingUtil.cutPage(response, pageOffset, pageSize));
        data.put("size", response.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private boolean contains(List<CubeInstanceResponse> response, String name) {
        for (CubeInstanceResponse r : response) {
            if (r.getName().equals(name))
                return true;
        }
        return false;
    }

    @RequestMapping(value = "validEncodings", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getValidEncodings() {

        Map<String, Integer> encodings = DimensionEncodingFactory.getValidEncodings();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, encodings, "");
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCube(@PathVariable String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        KapCubeResponse r;
        try {
            r = createCubeResponse(cube);
        } catch (Exception e) {
            throw new BadRequestException("Error getting cube instance response.", ResponseCode.CODE_UNDEFINED, e);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, r, "");
    }

    private KapCubeResponse createCubeResponse(CubeInstance cube) {
        return KapCubeResponse.create(cube, kapCubeService);
    }

    /**
     * Get hive SQL of the cube
     *
     * @param cubeName Cube Name
     * @return
     * @throws UnknownHostException
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/sql", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSql(@PathVariable String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse response = new GeneralResponse();
        response.setProperty("sql", sql);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    /**
     * Update cube notify list
     *
     * @param cubeName
     * @param notifyList
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/notify_list", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateNotifyList(@PathVariable String cubeName, @RequestBody List<String> notifyList)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        cubeService.updateCubeNotifyList(cube, notifyList);

    }

    @RequestMapping(value = "/{cubeName}/cost", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeCost(@PathVariable String cubeName, @RequestBody Integer cost)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.updateCubeCost(cube, cost), "");
    }

    /**
     * Force rebuild a cube's lookup table snapshot
     *
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/refresh_lookup", method = {
            RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuildLookupSnapshot(@PathVariable String cubeName, @PathVariable String segmentName,
            @RequestBody String lookupTable) throws IOException {
        Message msg = MsgPicker.getMsg();

        final CubeManager cubeMgr = cubeService.getCubeManager();
        final CubeInstance cube = cubeMgr.getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                cubeService.rebuildLookupSnapshot(cube, segmentName, lookupTable), "");
    }

    @RequestMapping(value = "{cubeName}/mp_cubes", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listMPValues(@PathVariable String cubeName) throws IOException {
        Message msg = MsgPicker.getMsg();

        final CubeManager cubeMgr = cubeService.getCubeManager();
        final MPCubeManager mpCubeMgr = kapCubeService.getMPCubeManager();
        final CubeInstance cube = cubeMgr.getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        List<Map<String, Object>> cubeList = mpCubeMgr.listMPValuesAndCubes(cube);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, cubeList, "");
    }

    @RequestMapping(value = "{cubeName}/segments", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listSegments(@PathVariable String cubeName,
            @RequestParam(value = "mpValues", required = false) String mpValues,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        checkCubeExists(cubeName);

        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { mpValues });
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        List<CubeSegment> all = new ArrayList<>(cube.getSegments());
        Collections.sort(all, new Comparator<CubeSegment>() {
            @Override
            public int compare(CubeSegment seg1, CubeSegment seg2) {
                return (int) (seg2.getLastBuildTime() - seg1.getLastBuildTime());
            }
        });
        List<CubeSegment> segments = PagingUtil.cutPage(all, pageOffset, pageSize);

        long totalSizeKB = kapCubeService.computeSegmentsStorage(cube, segments);
        kapCubeService.computeSegmentsOperativeFlags(cube, segments);

        HashMap<String, Object> data = new HashMap<>();
        data.put("segments", segments);
        data.put("size", all.size());
        data.put("total_storage_size_kb", totalSizeKB);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{cubeName}/segments", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse manageSegments(@PathVariable String cubeName, @RequestBody SegmentMgmtRequest req)
            throws IOException {

        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        switch (req.getBuildType()) {
        case "MERGE": {
            JobInstance jobInstance = kapCubeService.mergeSegment(cubeName, req.getSegments(), req.isForce());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobInstance, "");
        }
        case "REFRESH":
            List<JobInstance> jobList = kapCubeService.refreshSegments(cubeName, req.getSegments());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobList, "");
        case "DROP":
            kapCubeService.dropSegments(cubeName, req.getSegments());

            CubeInstance cube = convertToMPMasterIfNeeded(cubeName);
            if (hasNoReadySegments(cube)) {
                cubeService.disableCube(cube);
            }

            dropMPCubeIfNeeded(cubeName);

            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, createCubeResponse(cube), "");
        default:
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, "Invalid build type.", "");
        }
    }

    /** Build/Rebuild a cube segment */
    @RequestMapping(value = "/{cubeName}/build", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse build(@PathVariable String cubeName, @RequestBody KapBuildRequest req) throws IOException {
        return rebuild(cubeName, req);
    }

    /** Build/Rebuild a cube segment */
    @RequestMapping(value = "/{cubeName}/rebuild", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuild(@PathVariable String cubeName, @RequestBody KapBuildRequest req)
            throws IOException {

        checkCubeExists(cubeName);

        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        TSRange range = new TSRange(req.getStartTime(), req.getEndTime());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                buildInternal(cubeName, range, null, null, null, req.getBuildType(), req.isForce()), "");
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/build_streaming", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse buildStreaming(@PathVariable String cubeName, @RequestBody KapStreamingBuildRequest req)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        boolean existKafkaClient = false;
        try {
            Class<?> clazz = Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
            if (clazz != null) {
                existKafkaClient = true;
            }
        } catch (ClassNotFoundException e) {
            existKafkaClient = false;
        }
        if (!existKafkaClient) {
            throw new BadRequestException(msg.getKAFKA_DEP_NOT_FOUND());
        }
        return rebuildStreaming(cubeName, req);
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/rebuild_streaming", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuildStreaming(@PathVariable String cubeName, @RequestBody KapStreamingBuildRequest req)
            throws IOException {

        checkCubeExists(cubeName);

        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        SegmentRange range = new SegmentRange(req.getSourceOffsetStart(), req.getSourceOffsetEnd());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                buildInternal(cubeName, null, range, req.getSourcePartitionOffsetStart(),
                        req.getSourcePartitionOffsetEnd(), req.getBuildType(), req.isForce()),
                "");
    }

    private JobInstance buildInternal(String cubeName, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd,
            String buildType, boolean force) throws IOException {
        Message msg = MsgPicker.getMsg();

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        if (cube.getDescriptor().isDraft()) {
            throw new BadRequestException(msg.getBUILD_DRAFT_CUBE());
        }
        return jobService.submitJob(cube, tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd,
                CubeBuildTypeEnum.valueOf(buildType), force, submitter);
    }

    @RequestMapping(value = "/{cubeName}/purge", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse purgeCube(@PathVariable String cubeName, @RequestBody KapBuildRequest req)
            throws IOException {

        checkCubeExists(cubeName);
        String originCubeName = cubeName;

        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        CubeInstance purgeCube = cubeService.purgeCube(cube);

        dropMPCubeIfNeeded(purgeCube.getName());

        CubeInstance originCube = cubeService.getCubeManager().getCube(originCubeName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createCubeResponse(originCube), "");
    }

    /**
     * get cube segment holes
     *
     * @return a list of CubeSegment, each representing a hole
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHoles(@PathVariable String cubeName, @RequestBody KapBuildRequest req)
            throws IOException {

        checkCubeExists(cubeName);
        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.getCubeManager().calculateHoles(cubeName),
                "");
    }

    /**
     * fill cube segment holes
     *
     * @return a list of JobInstances to fill the holes
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse fillHoles(@PathVariable String cubeName, @RequestBody KapBuildRequest req)
            throws IOException {

        checkCubeExists(cubeName);
        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        List<JobInstance> jobs = Lists.newArrayList();
        List<CubeSegment> holes = cubeService.getCubeManager().calculateHoles(cubeName);

        if (holes.size() == 0) {
            logger.info("No hole detected for cube '" + cubeName + "'");
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobs, "");
        }

        for (CubeSegment hole : holes) {
            if (hole.isOffsetCube()) {
                KapStreamingBuildRequest request = new KapStreamingBuildRequest();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setSourceOffsetStart((Long) hole.getSegRange().start.v);
                request.setSourceOffsetEnd((Long) hole.getSegRange().end.v);
                request.setSourcePartitionOffsetStart(hole.getSourcePartitionOffsetStart());
                request.setSourcePartitionOffsetEnd(hole.getSourcePartitionOffsetEnd());
                try {
                    JobInstance job = (JobInstance) buildStreaming(cubeName, request).data;
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            } else {
                KapBuildRequest request = new KapBuildRequest();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setStartTime(hole.getTSRange().start.v);
                request.setEndTime(hole.getTSRange().end.v);

                try {
                    JobInstance job = (JobInstance) build(cubeName, request).data;
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobs, "");
    }

    /**
     * Initiate the very beginning of a streaming cube. Will seek the latest offests of each partition from streaming
     * source (kafka) and record in the cube descriptor; In the first build job, it will use these offests as the start point.
     * @param cubeName
     * @return
     */

    @RequestMapping(value = "/{cubeName}/init_start_offsets", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse initStartOffsets(@PathVariable String cubeName, @RequestBody KapBuildRequest req)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        checkCubeExists(cubeName);
        cubeName = convertToMPCubeIfNeeded(cubeName, new String[] { req.getMpValues() });

        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance.getSourceType() != ISourceAware.ID_STREAMING) {
            throw new BadRequestException(String.format(msg.getNOT_STREAMING_CUBE(), cubeName));
        }

        final GeneralResponse response = new GeneralResponse();
        final Map<Integer, Long> startOffsets = KafkaClient.getLatestOffsets(cubeInstance);
        CubeDesc desc = cubeInstance.getDescriptor();
        desc.setPartitionOffsetStart(startOffsets);
        cubeService.getCubeDescManager().updateCubeDesc(desc);
        response.setProperty("result", "success");
        response.setProperty("offsets", startOffsets.toString());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    private void checkCubeExists(String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);

        if (cubeInstance == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
    }

    private String convertToMPCubeIfNeeded(String cubeName, String[] multiLevelPartitionValues) throws IOException {
        MPCubeManager mpCubeMgr = MPCubeManager.getInstance(cubeService.getConfig());
        return mpCubeMgr.convertToMPCubeIfNeeded(cubeName, multiLevelPartitionValues).getName();
    }

    private boolean hasNoReadySegments(CubeInstance cube) throws IOException {
        KylinConfig config = cubeService.getConfig();
        MPCubeManager mgr = MPCubeManager.getInstance(config);

        Segments<CubeSegment> segments = new Segments<CubeSegment>();
        if (mgr.isMPMaster(cube)) {
            for (CubeInstance mpCube : mgr.listMPCubes(cube)) {
                segments.addAll(mpCube.getSegments());
            }
        } else {
            segments.addAll(cube.getSegments());
        }

        return (segments.getSegments(SegmentStatusEnum.READY).size() == 0);
    }

    private void dropMPCubeIfNeeded(String cubeName) throws IOException {
        MPCubeManager mpCubeMgr = MPCubeManager.getInstance(cubeService.getConfig());
        mpCubeMgr.dropMPCubeIfNeeded(cubeName);
    }

    private CubeInstance convertToMPMasterIfNeeded(String cubeName) {
        MPCubeManager mpCubeMgr = MPCubeManager.getInstance(cubeService.getConfig());
        return mpCubeMgr.convertToMPMasterIfNeeded(cubeName);
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
