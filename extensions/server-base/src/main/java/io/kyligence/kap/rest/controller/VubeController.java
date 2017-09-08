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

import static io.kyligence.kap.metadata.model.IKapStorageAware.ID_SHARDED_PARQUET;
import static io.kyligence.kap.metadata.model.IKapStorageAware.ID_SPLICE_PARQUET;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
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
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.HBaseResponse;
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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KapCubeRequest;
import io.kyligence.kap.rest.request.NotificationSettingRequest;
import io.kyligence.kap.rest.request.ScheduleJobRequest;
import io.kyligence.kap.rest.request.VubeBuildRequest;
import io.kyligence.kap.rest.request.VubeManageRequest;
import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.rest.response.CubeVersionResponse;
import io.kyligence.kap.rest.response.KapCubeInstanceResponse;
import io.kyligence.kap.rest.service.KapCubeService;
import io.kyligence.kap.rest.service.KapSuggestionService;
import io.kyligence.kap.rest.service.RawTableService;
import io.kyligence.kap.rest.service.SchedulerJobService;
import io.kyligence.kap.rest.service.VubeService;
import io.kyligence.kap.vube.VubeInstance;
import io.kyligence.kap.vube.VubeUpdate;
@Controller
@Component("vubeController")
@RequestMapping(value = "/vubes")
public class VubeController extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(VubeController.class);

    @Autowired
    @Qualifier("vubeService")
    private VubeService vubeService;

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

    @Autowired
    @Qualifier("kapSuggestionService")
    private KapSuggestionService kapSuggestionService;

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() {

    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getVubesPaging(@RequestParam(value = "vubeName", required = false) String vubeName,
            @RequestParam(value = "exactMatch", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        HashMap<String, Object> data = new HashMap<String, Object>();
        List<KapCubeInstanceResponse> response = new ArrayList<KapCubeInstanceResponse>();

        List<VubeInstance> vubes;
        vubes = vubeService.listAllVubes(vubeName, projectName, modelName, true);

        // official vubes
        for (VubeInstance vube : vubes) {
            try {
                response.add(createCubeInstanceResponse(vube));
            } catch (Exception e) {
                logger.error("Error creating vube instance response, skipping.", e);
            }
        }

        // draft cubes
        for (Draft d : cubeService.listCubeDrafts(vubeName, modelName, projectName, exactMatch)) {
            CubeDesc c = (CubeDesc) d.getEntity();

            if (contains(response, c.getName()) == false) {
                KapCubeInstanceResponse r = createCubeInstanceResponseFromDraft(d);
                r.setProject(d.getProject());
                response.add(r);
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;
        int size = response.size();

        if (size <= offset) {
            offset = size;
            limit = 0;
        }

        if ((size - offset) < limit) {
            limit = size - offset;
        }

        data.put("cubes", response.subList(offset, offset + limit));
        data.put("size", size);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private boolean contains(List<KapCubeInstanceResponse> response, String name) {
        for (CubeInstanceResponse r : response) {
            if (r != null && r.getName().equals(name))
                return true;
        }
        return false;
    }

    private KapCubeInstanceResponse createCubeInstanceResponseFromDraft(Draft d) {
        CubeDesc desc = (CubeDesc) d.getEntity();
        Preconditions.checkState(desc.isDraft());

        CubeInstance mock = new CubeInstance();
        mock.setName(desc.getName());
        mock.setDescName(desc.getName());
        mock.setStatus(RealizationStatusEnum.DISABLED);

        KapCubeInstanceResponse r = new KapCubeInstanceResponse(mock);

        r.setModel(desc.getModelName());
        r.setProject(d.getProject());
        r.setDraft(true);

        return r;
    }

    private KapCubeInstanceResponse createCubeInstanceResponse(VubeInstance vube) {
        CubeInstance cube = cubeService.getCubeManager().getCube(vube.getLatestCube().getName());

        if (cube == null) {
            return null;
        }

        RealizationStatusEnum status = cube.getStatus();

        if (vube.getVersionedCubes().size() > 1) {
            status = vube.getStatus();
        }

        Preconditions.checkState(!cube.getDescriptor().isDraft());

        KapCubeInstanceResponse r = new KapCubeInstanceResponse(cube);

        Segments<CubeSegment> segments = vube.getAllSegments();

        long cubeSize = 0L;
        long sizeRecordCount = 0L;
        long lastBuildTime = 0L;
        long inputRecordsSize = 0L;

        for (CubeSegment segment : segments) {
            if (segment.getStatus() == SegmentStatusEnum.READY) {
                sizeRecordCount += segment.getInputRecords();
                inputRecordsSize += segment.getInputRecordsSize();
            }

            if (segment.getLastBuildTime() > lastBuildTime) {
                lastBuildTime = segment.getLastBuildTime();
            }

            int storageType = vubeService.getSegmentStorageType(segment);

            if (storageType == ID_SHARDED_PARQUET || storageType == ID_SPLICE_PARQUET) {
                ColumnarResponse info = kapCubeService.getColumnarInfo(segment);
                cubeSize += info.getStorageSize();
                cubeSize += info.getRawTableStorageSize();
            } else {
                HBaseResponse info = getHBaseResponse(segment);
                cubeSize += info.getTableSize();
            }
            inputRecordsSize += segment.getInputRecordsSize();
        }

        r.setSourceRecords(sizeRecordCount);
        r.setInputRecordsSize(inputRecordsSize);
        r.setLastBuildTime(lastBuildTime);
        r.setCubeSize((float) cubeSize / 1024);

        r.setStatus(status);
        r.setModel(cube.getDescriptor().getModelName());
        r.setPartitionDateStart(cube.getDescriptor().getPartitionDateStart());

        if (cube.getModel() != null) {
            r.setPartitionDateColumn(cube.getModel().getPartitionDesc().getPartitionDateColumn());
            r.setIs_streaming(
                    cube.getModel().getRootFactTable().getTableDesc().getSourceType() == ISourceAware.ID_STREAMING);
        }
        r.setProject(projectService.getProjectOfCube(cube.getName()));
        r.setName(vube.getName());

        return r;
    }

    @RequestMapping(value = "validEncodings", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getValidEncodings() {

        Map<String, Integer> encodings = DimensionEncodingFactory.getValidEncodings();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, encodings, "");
    }

    @RequestMapping(value = "/{vubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCube(@PathVariable String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube, "");
    }

    @RequestMapping(value = "/{vubeName}/latest", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getLatestCubeName(@PathVariable String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube.getLatestCube(), "");
    }

    /**
     * get Hbase Info
     *
     * @return true
     * @throws IOException
     */

    @RequestMapping(value = "/{vubeName}/hbase", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHBaseInfo(@PathVariable String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        List<HBaseResponse> hbase = new ArrayList<HBaseResponse>();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        List<CubeSegment> segments = vube.getAllSegments();

        for (CubeSegment segment : segments) {
            HBaseResponse hr = getHBaseResponse(segment);
            hbase.add(hr);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hbase, "");
    }

    private HBaseResponse getHBaseResponse(CubeSegment segment) {
        String tableName = segment.getStorageLocationIdentifier();
        String cubeName = segment.getCubeInstance().getName();
        HBaseResponse hr = null;

        // Get info of given table.
        try {
            hr = cubeService.getHTableInfo(cubeName, tableName);
        } catch (IOException e) {
            logger.error("Failed to calculate size of HTable \"" + tableName + "\".", e);
        }

        if (null == hr) {
            logger.info("Failed to calculate size of HTable \"" + tableName + "\".");
            hr = new HBaseResponse();
        }

        hr.setTableName(tableName);
        hr.setDateRangeStart(segment.getTSRange().start.v);
        hr.setDateRangeEnd(segment.getTSRange().end.v);
        hr.setSegmentName(segment.getName());
        hr.setSegmentUUID(segment.getUuid());
        hr.setSegmentStatus(segment.getStatus().toString());
        hr.setSourceCount(segment.getInputRecords());
        if (segment.isOffsetCube()) {
            hr.setSourceOffsetStart((Long) segment.getSegRange().start.v);
            hr.setSourceOffsetEnd((Long) segment.getSegRange().end.v);
        }

        return hr;
    }

    private long getHbaseSize(String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        long sizeSum = 0;

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        for (CubeSegment segment : vube.getAllSegments()) {
            sizeSum += segment.getInputRecordsSize();
        }

        return sizeSum;
    }

    @RequestMapping(value = "/{vubeName}/columnar", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getColumnarInfo(@PathVariable String vubeName) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, getColumnarList(vubeName), "");
    }

    private long getColumnarSize(String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        long sizeSum = 0;

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        for (CubeInstance cube : vube.getVersionedCubes()) {
            CubeInstance realCube = cubeService.getCubeManager().getCube(cube.getName());
            List<ColumnarResponse> columnarList = kapCubeService.getAllColumnarInfo(realCube);

            for (ColumnarResponse response : columnarList) {
                sizeSum += response.getRawTableStorageSize();
                sizeSum += response.getStorageSize();
            }
        }

        return sizeSum;
    }

    private List<List<ColumnarResponse>> getColumnarList(String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();
        List<List<ColumnarResponse>> columnarList = Lists.newArrayList();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        for (CubeInstance cube : vube.getVersionedCubes()) {
            CubeInstance realCube = cubeService.getCubeManager().getCube(cube.getName());
            columnarList.add(kapCubeService.getAllColumnarInfo(realCube));
        }

        return columnarList;
    }

    @RequestMapping(value = "/{vubeName}/holes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHoles(@PathVariable String vubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        List<CubeSegment> holes = new ArrayList<CubeSegment>();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        List<CubeInstance> cubes = vube.getVersionedCubes();
        for (CubeInstance cube : cubes) {
            holes.addAll(cubeService.getCubeManager().calculateHoles(cube.getName()));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, holes, "");
    }

    @RequestMapping(value = "/{projectName}/{vubeName}/scheduler_job", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSchedulerJob(@PathVariable String projectName, @PathVariable String vubeName)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        SchedulerJobInstance job = getSchedulerJobByVubeName(vubeName);
        Draft draft = cubeService.getCubeDraft(vubeName, projectName);

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

    private SchedulerJobInstance getSchedulerJobByVubeName(String vubeName) throws IOException {
        for (SchedulerJobInstance jobInstance : schedulerJobService.listAllSchedulerJobs()) {
            if (jobInstance.getRelatedRealization().equals(vubeName))
                return jobInstance;
        }

        return null;
    }

    @RequestMapping(value = "{vubeName}/versions", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getVersions(@PathVariable String vubeName) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        List<CubeVersionResponse> result = new ArrayList<>();
        List<CubeInstance> cubes = vube.getVersionedCubes();

        for (CubeInstance cube : cubes) {
            String version = VubeInstance.cubeNameToVersion(vube, cube.getName());
            CubeVersionResponse response = new CubeVersionResponse(version, cube.getCreateTimeUTC());

            result.add(response);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{vubeName}/segments", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSegments(@PathVariable String vubeName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        HashMap<String, Object> data = new HashMap<>();
        List<CubeVersionResponse> versions = new ArrayList<>();
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        long totalSize = 0;

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        Segments<CubeSegment> segments = vubeService.listSegments(vubeName, projectName, version);
        List<Object> responses = Lists.newArrayList();
        int offset = pageOffset * pageSize;
        int limit = pageSize;
        int size = segments.size();

        if (size <= offset) {
            offset = size;
            limit = 0;
        }

        if ((size - offset) < limit) {
            limit = size - offset;
        }

        Collections.sort(segments, new Comparator<CubeSegment>() {
            @Override
            public int compare(CubeSegment seg1, CubeSegment seg2) {
                return (int) (seg2.getLastBuildTime() - seg1.getLastBuildTime());
            }
        });

        Set<String> mergingSegs = new HashSet<>();
        Segments<CubeSegment> buildingSegments = segments.getBuildingSegments();

        for (CubeSegment buildingSeg : buildingSegments) {
            Segments<CubeSegment> mergingSegments = segments.getMergingSegments(buildingSeg);

            for (CubeSegment mergingSegment : mergingSegments) {
                mergingSegs.add(mergingSegment.getName());
            }
        }

        for (CubeSegment segment : segments) {
            int storageType = vubeService.getSegmentStorageType(segment);
            String segVersion = VubeInstance.cubeNameToVersion(vube, segment.getCubeInstance().getName());

            if (segment.getStatus() == SegmentStatusEnum.NEW) {
                Segments<CubeSegment> mergingSegments = segments.getMergingSegments(segment);

                for (CubeSegment mSeg : mergingSegments) {
                    mergingSegs.add(mSeg.getName());
                }
            }

            if (storageType == ID_SHARDED_PARQUET || storageType == ID_SPLICE_PARQUET) {
                ColumnarResponse info = kapCubeService.getColumnarInfo(segment);

                if (mergingSegs.contains(segment.getName())) {
                    info.setSegmentStatus("READY_PENDING");
                }
                if (info != null) {
                    responses.add(info);
                    totalSize += info.getStorageSize();
                    totalSize += info.getRawTableStorageSize();
                }
            } else {
                HBaseResponse info = getHBaseResponse(segment);

                if (mergingSegs.contains(segment.getName())) {
                    info.setSegmentStatus("READY_PENDING");
                }
                responses.add(info);
                totalSize += info.getTableSize();
            }
            versions.add(new CubeVersionResponse(segVersion, segment.getCubeInstance().getCreateTimeUTC()));
        }

        data.put("segments", responses.subList(offset, offset + limit));
        data.put("versions", versions.subList(offset, offset + limit));
        data.put("size", size);
        data.put("total_size", totalSize);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "{vubeName}/segments_end", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSegmentsEnd(@PathVariable String vubeName) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        Segments<CubeSegment> segments = vube.getAllSegments();
        long end = 0;

        if (segments.size() > 0) {
            end = segments.getTSEnd();
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, end == Long.MIN_VALUE ? 0 : end, "");
    }

    @RequestMapping(value = "{projectName}/{vubeName}/cube_desc", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubeDesc(@PathVariable String projectName, @PathVariable String vubeName,
            @RequestParam(value = "version", required = false) String version) throws IOException {
        CubeInstance cube = vubeService.getCubeWithVersion(projectName, vubeName, version);
        HashMap<String, CubeDesc> result = new HashMap<>();
        Draft draft = cubeService.getCubeDraft(vubeName, projectName);

        if (cube != null) {
            Preconditions.checkState(!cube.getDescriptor().isDraft());
            result.put("cube", cube.getDescriptor());
        }
        if (draft != null) {
            CubeDesc dc = (CubeDesc) draft.getEntity();
            Preconditions.checkState(dc.isDraft());
            result.put("draft", dc);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{vubeName}/sample_sqls", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSampleSqls(@PathVariable String vubeName,
            @RequestParam(value = "version", required = false) String version) throws IOException {
        VubeInstance vube = vubeService.getVubeInstance(vubeName);
        HashMap<String, List<String>> result = new HashMap<>();
        result.put("suggested", kapSuggestionService.getCubeOptLog(vubeName).getSampleSqls());

        if (vube != null) {
            result.put("versioned", vube.getSampleSqlsWithVersion(version));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/{vubeName}/sql", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSql(@PathVariable String vubeName) {
        CubeInstance cube = vubeService.getLatestCube(vubeName);

        if (cube == null) {
            throw new InternalErrorException("Cannot find latest cube of " + vubeName);
        }

        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse response = new GeneralResponse();
        response.setProperty("sql", sql);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/{vubeName}/rebuild", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuild(@PathVariable String vubeName, @RequestBody VubeBuildRequest req)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }

        if (req.getBuildType().equals("BUILD")) {
            JobInstance jobInstance = doBuild(vube, req);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
        } else if (req.getBuildType().equals("MERGE")) {
            JobInstance jobInstance = doMerge(vube, req);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
        } else if (req.getBuildType().equals("REFRESH")) {
            List<JobInstance> jobList = doRefresh(vube, req);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobList, "");
        } else if (req.getBuildType().equals("DROP")) {
            vube = doDrop(vube, req);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube, "");
        } else {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, "Invalid build type.", "");
        }
    }

    private JobInstance doBuild(VubeInstance vube, VubeBuildRequest req) throws IOException {
        String cubeName = vubeService.getLatestCube(vube.getName()).getName();

        JobInstance jobInstance = buildInternal(cubeName, new TSRange(req.getStartTime(), req.getEndTime()), null, null,
                null, req.getBuildType(), req.isForce());
        vubeService.updateCube(vube, cubeName);
        vubeService.addPendingCube(vube.getName(), cubeName);
        return jobInstance;
    }

    private JobInstance doMerge(VubeInstance vube, VubeBuildRequest req) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        if (req.getSegments() == null || req.getSegments().size() < 2) {
            throw new BadRequestException(String.format(msg.getVUBE_SEGMENTS_MERGE_LESS_THAN_TWO()));
        }

        Segments<CubeSegment> segments = vube.getSegments(req.getSegments());
        Collections.sort(segments);

        CubeSegment pre = null;
        String version = null;

        for (CubeSegment seg : segments) {
            if (pre != null) {
                if (pre.getTSRange().start.v < seg.getTSRange().end.v
                        && seg.getTSRange().start.v < pre.getTSRange().end.v)
                    throw new BadRequestException(String.format(msg.getVUBE_SEGMENTS_MERGE_OVERLAPED()));

                if (!VubeInstance.cubeNameToVersion(vube, seg.getCubeInstance().getName()).equals(version)) {
                    throw new BadRequestException(String.format(msg.getVUBE_SEGMENTS_DIFFERENT_VERSION()));
                }
            } else if (seg != null) {
                version = VubeInstance.cubeNameToVersion(vube, seg.getCubeInstance().getName());
            }
            pre = seg;
        }

        String cubeName = pre.getCubeInstance().getName();

        JobInstance jobInstance = buildInternal(cubeName, new TSRange(segments.getTSStart(), segments.getTSEnd()), null,
                null, null, req.getBuildType(), true);
        vubeService.updateCube(vube, cubeName);
        vubeService.addPendingCube(vube.getName(), cubeName);
        return jobInstance;
    }

    private List<JobInstance> doRefresh(VubeInstance vube, VubeBuildRequest req) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        Segments<CubeSegment> segments = vube.getSegments(req.getSegments());
        List<JobInstance> jobList = new LinkedList<>();

        if (req.isUpgrade()) {
            String latestCubeName = vube.getLatestCube().getName();
            String olderCubeName = null;
            boolean latestRefreshed = false;
            boolean olderRefreshed = false;

            if (vube.getVersionedCubes().size() > 1) {
                List<CubeInstance> cubeList = vube.getVersionedCubes();

                for (int i = cubeList.size() - 2; i >= 0; i--) {
                    if (cubeList.get(i).getSegments().size() > 0) {
                        olderCubeName = cubeList.get(i).getName();
                        break;
                    }
                }
            }

            for (CubeSegment segment : segments) {
                if ((!segment.getCubeInstance().getName().equals(latestCubeName))
                        && (!segment.getCubeInstance().getName().equals(olderCubeName))) {
                    continue;
                } else if (segment.getCubeInstance().getName().equals(latestCubeName)) {
                    jobList.add(buildInternal(latestCubeName, segment.getTSRange(), segment.getSegRange(), null, null,
                            req.getBuildType(), true));
                    latestRefreshed = true;
                } else {
                    jobList.add(buildInternal(latestCubeName, segment.getTSRange(), segment.getSegRange(), null, null,
                            "BUILD", true));
                    cubeService.deleteSegment(segment.getCubeInstance(), segment.getName());
                    olderRefreshed = true;
                }
            }

            if (latestRefreshed) {
                vubeService.updateCube(vube, latestCubeName);
                vubeService.addPendingCube(vube.getName(), latestCubeName);
            }

            if (olderRefreshed) {
                vubeService.updateCube(vube, olderCubeName);
                vubeService.addPendingCube(vube.getName(), olderCubeName);
            }
        } else {
            for (CubeSegment segment : segments) {
                jobList.add(buildInternal(segment.getCubeInstance().getName(), segment.getTSRange(),
                        segment.getSegRange(), null, null, req.getBuildType(), true));
                vubeService.addPendingCube(vube.getName(), segment.getCubeInstance().getName());
            }

            for (String cubeName : vubeService.getPendingCubes(vube.getName())) {
                vubeService.updateCube(vube, cubeName);
            }
        }
        return jobList;
    }

    private VubeInstance doDrop(VubeInstance vube, VubeBuildRequest req) throws IOException {
        Segments<CubeSegment> segments = vube.getAllSegments();
        Set<CubeInstance> cubesSet = new HashSet<>();
        List<String> segsToDrop = req.getSegments();

        for (CubeSegment segment : segments) {
            if (segsToDrop.contains(segment.getName())) {
                cubesSet.add(segment.getCubeInstance());
                cubeService.deleteSegment(cubeService.getCubeManager().getCube(segment.getCubeInstance().getName()),
                        segment.getName());
                vubeService.addPendingCube(vube.getName(), segment.getCubeInstance().getName());
            }
        }

        VubeUpdate update = new VubeUpdate(vube);

        update.setCubesToUpdate(cubesSet.toArray(new CubeInstance[cubesSet.size()]));
        vube = vubeService.updateVube(vube, update);

        return vube;
    }

    @RequestMapping(value = "/{vubeName}/enable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse enableVube(@PathVariable String vubeName) throws IOException {
        KapMessage kapMsg = KapMsgPicker.getMsg();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        VubeInstance vube = null;
        try {
            vube = vubeService.getVubeInstance(vubeName);

            if (vube == null) {
                throw new BadRequestException(String.format(kapMsg.getVUBE_NOT_FOUND(), vubeName));
            }

            VubeUpdate update = new VubeUpdate(vube);

            // Set vube status to DESCBROKEN to avoid cube being disabled again by broadcast
            update.setStatus(RealizationStatusEnum.DESCBROKEN);
            vubeService.updateVube(vube, update);

            List<CubeInstance> enabledCubeList = new ArrayList<>();
            for (CubeInstance cube : vube.getVersionedCubes()) {
                CubeInstance realCube = cubeService.getCubeManager().getCube(cube.getName());

                if (realCube.getStatus().equals(RealizationStatusEnum.DISABLED)
                        && realCube.getSegments(SegmentStatusEnum.READY).size() > 0) {
                    cube = cubeService.enableCube(realCube);
                    enabledCubeList.add(cube);
                }
                RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cube.getName());

                if (raw != null) {
                    rawTableService.enableRaw(raw, realCube);
                }
            }

            update.setCubesToUpdate(enabledCubeList.toArray(new CubeInstance[enabledCubeList.size()]));
            update.setStatus(RealizationStatusEnum.READY);
            vubeService.updateVube(vube, update);

        } catch (Exception ex) {
            cp.rollback();
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube, "");
    }

    @RequestMapping(value = "/{vubeName}/disable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse disableVube(@PathVariable String vubeName) throws IOException, SchedulerException {
        KapMessage kapMsg = KapMsgPicker.getMsg();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        VubeInstance vube = null;
        try {
            vube = vubeService.getVubeInstance(vubeName);

            if (vube == null) {
                throw new BadRequestException(String.format(kapMsg.getVUBE_NOT_FOUND(), vubeName));
            }

            VubeUpdate update = new VubeUpdate(vube);
            List<CubeInstance> disabledCubeList = new ArrayList<>();

            for (CubeInstance cube : vube.getVersionedCubes()) {
                CubeInstance realCube = cubeService.getCubeManager().getCube(cube.getName());

                if (realCube.getStatus() == RealizationStatusEnum.READY) {
                    cube = cubeService.disableCube(realCube);
                    disabledCubeList.add(cube);
                }
                RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cube.getName());

                if (raw != null && raw.getStatus() == RealizationStatusEnum.READY) {
                    rawTableService.disableRaw(raw, cube);
                }
            }

            SchedulerJobInstance schedulerJobInstance = schedulerJobService.getSchedulerJobManager()
                    .getSchedulerJob(vubeName);
            ProjectInstance project = cubeService.getProjectManager()
                    .getProject(cubeService.getCubeManager().getCube(vube.getLatestCube().getName()).getProject());

            if (schedulerJobInstance != null) {
                schedulerJobService.disableSchedulerJob(schedulerJobInstance, project);
            }
            update.setCubesToUpdate(disabledCubeList.toArray(new CubeInstance[disabledCubeList.size()]));
            update.setStatus(RealizationStatusEnum.DISABLED);
            vubeService.updateVube(vube, update);
        } catch (Exception ex) {
            cp.rollback();
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube, "");
    }

    @RequestMapping(value = "/{vubeName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneVube(@PathVariable String vubeName, @RequestBody CubeRequest cubeRequest)
            throws IOException, ParseException, SchedulerException {
        KapMessage kapMsg = KapMsgPicker.getMsg();
        Message msg = MsgPicker.getMsg();
        String newVubeName = cubeRequest.getCubeName();
        String projectName = cubeRequest.getProject();

        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(kapMsg.getVUBE_NOT_FOUND(), vubeName));
        }

        String cubeName = vube.getLatestCube().getName();
        CubeDesc oldCubeDesc = kapCubeService.getCubeDescManager().getCubeDesc(cubeName);
        CubeInstance oldCube = kapCubeService.getCubeManager().getCube(cubeName);
        VubeInstance newVube = null;

        if (oldCubeDesc == null || oldCube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), vubeName));
        }

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();

        CubeInstance newCube = null;
        try {
            ProjectInstance project = kapCubeService.getProjectManager().getProject(projectName);
            if (project == null) {
                throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), projectName));
            }

            newCube = kapCubeService.cubeClone(oldCube, newVubeName + "_version_1", project);
            RawTableInstance oldRaw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            RawTableInstance newRaw = null;

            if (oldRaw != null) {
                logger.debug("Cube " + cubeName + " contains rawtable, clone it");
                newRaw = rawTableService.cloneRaw(oldRaw, newVubeName + "_version_1", newCube, project);
            }
            SchedulerJobInstance job = schedulerJobService.getSchedulerJob(vubeName);
            if (job != null) {
                schedulerJobService.cloneSchedulerJob(job, newVubeName, newCube.getUuid(),
                        newCube.getDescriptor().getPartitionDateStart(), newCube, project);
            }

            newVube = vubeService.createVube(newVubeName, project, newCube);
            VubeUpdate update = new VubeUpdate(newVube);

            update.setRawTableToAdd(newRaw);
            update.setSampleSqls(vube.getSampleSqls().get(vube.getVersionedCubes().size() - 1));
            vubeService.updateVube(newVube, update);

        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(projectName);
            throw ex;
        } finally {
            cp.close();
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, newVube, "");
    }

    @RequestMapping(value = "/{vubeName}/drop_segs", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropSegments(@PathVariable String vubeName, @RequestBody VubeBuildRequest req)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        VubeInstance vube = vubeService.getVubeInstance(vubeName);

        if (vube == null) {
            throw new BadRequestException(String.format(msg.getVUBE_NOT_FOUND(), vubeName));
        }
        Segments<CubeSegment> segments = vube.getAllSegments();
        Set<CubeInstance> cubesSet = new HashSet<>();
        List<String> segsToDrop = req.getSegments();

        for (CubeSegment segment : segments) {
            if (segsToDrop.contains(segment.getName())) {
                cubesSet.add(segment.getCubeInstance());
                cubeService.deleteSegment(segment.getCubeInstance(), segment.getName());
            }
        }

        VubeUpdate update = new VubeUpdate(vube);
        // All segments deleted and no raw table
        if (segments.size() == segsToDrop.size() && vube.getVersionedRawTables().size() == 0) {
            update.setStatus(RealizationStatusEnum.DISABLED);
        }

        update.setCubesToUpdate(cubesSet.toArray(new CubeInstance[cubesSet.size()]));
        vube = vubeService.updateVube(vube, update);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, vube, "");
    }

    private JobInstance buildInternal(String cubeName, TSRange tsRange, SegmentRange segRange,
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

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeDesc(@RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, ParseException, SchedulerException {
        Message msg = MsgPicker.getMsg();
        KapMessage kapMessage = KapMsgPicker.getMsg();

        CubeDesc cubeDesc = deserializeCubeDesc(kapCubeRequest);
        cubeService.validateCubeDesc(cubeDesc, false);

        RawTableDesc rawTableDesc = deserializeRawTableDesc(kapCubeRequest);
        rawTableService.validateRawTableDesc(rawTableDesc);

        SchedulerJobInstance schedule = deserializeSchedulerJobInstance(kapCubeRequest);
        schedulerJobService.pauseScheduler(cubeDesc.getName());

        String projectName = (null == kapCubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : kapCubeRequest.getProject();
        ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
        if (project == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), projectName));
        }

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore.Checkpoint cp = store.checkpoint();
        String version = null;

        try {
            boolean firstVersion = false;
            String vubeName = cubeDesc.getName();
            VubeInstance vube = vubeService.getVubeInstance(vubeName);

            if (vube == null) {
                firstVersion = true;
            }

            String cubeName = null;

            if (firstVersion) {
                cubeName = cubeDesc.getName() + "_version_1";
                version = "version_1";
            } else {
                int currentVersionNum = vube.getVersionedCubes().size() + 1;
                cubeName = cubeDesc.getName() + "_version_" + currentVersionNum;
                version = "version_" + currentVersionNum;
            }

            cubeDesc.setName(cubeName);
            cubeDesc.setLastModified(0);
            cubeDesc = cubeService.saveCube(cubeDesc, project);

            CubeInstance cube = cubeService.getCubeManager().getCube(cubeDesc.getName());

            VubeUpdate update = new VubeUpdate(vube);

            if (firstVersion) {
                vube = vubeService.createVube(vubeName, project, cube);

                if (schedule == null) {
                    schedule = new SchedulerJobInstance(vubeName, projectName, "vube", vubeName, false, 0, 0, 65535, 0,
                            0, 0);

                    schedule.setPartitionStartTime(cubeDesc.getPartitionDateStart());
                    bindSchedulerJobWithVube(schedule, vubeName, project);
                }

                if (schedule.isEnabled()) {
                    schedulerJobService.enableSchedulerJob(schedule, project);
                } else {
                    schedulerJobService.disableSchedulerJob(schedule, project);
                }
            } else {
                if (vube == null) {
                    throw new BadRequestException(String.format(kapMessage.getVUBE_NOT_FOUND(), vubeName));
                }

                update.setCubeToAdd(cube);
                update.setVersion(version);
                vubeService.updateVube(vube, update);
            }

            RawTableInstance raw = null;

            if (rawTableDesc != null) {
                rawTableDesc.setName(cubeDesc.getName());
                rawTableDesc = rawTableService.saveRaw(rawTableDesc, cube, project);
                raw = rawTableService.getRawTableManager().getRawTableInstance(cubeDesc.getName());
            }

            update.setVubeInstance(vube);
            update.setRawTableToAdd(raw);
            update.setSampleSqls(kapSuggestionService.getCubeOptLog(vubeName).getSampleSqls());
            vubeService.updateVube(vube, update);

            // remove any previous draft
            cubeService.getDraftManager().delete(cubeDesc.getUuid());

        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeProjectCache(projectName);
            throw ex;
        } finally {
            cp.close();
        }

        Date now = new Date();
        return buildUpdateCubeDescResponse(cubeDesc, rawTableDesc, schedule, version, now.getTime());
    }

    @RequestMapping(value = "/{vubeName}/manage/partition_start_date", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updatePartitionStartDate(@PathVariable String vubeName,
            @RequestBody VubeManageRequest vubeManageRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        return vubeService.updatePartitionDateStart(vube, vubeManageRequest.getPartitionDateStart());
    }

    @RequestMapping(value = "/{vubeName}/manage/notification_setting", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateNotificationSetting(@PathVariable String vubeName,
            @RequestBody NotificationSettingRequest notificationSettingRequest)
            throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        return vubeService.updateNotificationSetting(vube, notificationSettingRequest.getNotifyList(),
                notificationSettingRequest.getStatusNeedNotify());
    }

    @RequestMapping(value = "/{vubeName}/manage/merge_setting", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateMergeSetting(@PathVariable String vubeName,
            @RequestBody VubeManageRequest vubeManageRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        Long[] ranges = new Long[vubeManageRequest.getAutoMergeTimeRanges().size()];
        vubeManageRequest.getAutoMergeTimeRanges().toArray(ranges);

        return vubeService.updateMergeRanges(vube, ranges);
    }

    @RequestMapping(value = "/{vubeName}/manage/scheduler_setting", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateSchedulerSetting(@PathVariable String vubeName,
            @RequestBody ScheduleJobRequest scheduleJobRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(vube.getLatestCube().getName());

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        SchedulerJobInstance scheduler = new SchedulerJobInstance(vubeName, cube.getProject(), "vube", vubeName,
                scheduleJobRequest.isEnabled(), cube.getDescriptor().getPartitionDateStart(),
                scheduleJobRequest.getTriggerTime(), Integer.MAX_VALUE, 0, scheduleJobRequest.getRepeatInterval(),
                scheduleJobRequest.getRepeatInterval());

        schedulerJobService.pauseScheduler(vube.getName());
        ProjectInstance project = cubeService.getProjectManager().getProject(cube.getProject());
        scheduler.setPartitionStartTime(cube.getDescriptor().getPartitionDateStart());
        bindSchedulerJobWithVube(scheduler, vubeName, project);

        if (scheduler.isEnabled()) {
            schedulerJobService.enableSchedulerJob(scheduler, project);
        } else {
            schedulerJobService.disableSchedulerJob(scheduler, project);
        }

        return vube;
    }

    @RequestMapping(value = "/{vubeName}/manage/cube_engine", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateCubeEngine(@PathVariable String vubeName,
            @RequestBody VubeManageRequest vubeManageRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        return vubeService.updateCubeEngine(vube, vubeManageRequest.getEngineType());
    }

    @RequestMapping(value = "/{vubeName}/manage/retention_setting", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateRetentionSetting(@PathVariable String vubeName,
            @RequestBody VubeManageRequest vubeManageRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        return vubeService.updateRetentionThreshold(vube, vubeManageRequest.getRetentionRange());
    }

    @RequestMapping(value = "/{vubeName}/manage/cube_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateCubeConfig(@PathVariable String vubeName,
            @RequestBody VubeManageRequest vubeManageRequest) throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        return vubeService.updateCubeConfig(vube, vubeManageRequest.getOverrideKylinProperties());
    }

    @RequestMapping(value = "/{vubeName}/manage/", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public VubeInstance updateVubeManage(@PathVariable String vubeName, @RequestBody KapCubeRequest kapCubeRequest)
            throws IOException, SchedulerException, ParseException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube == null) {
            throw new InternalErrorException("Cannot find versioned cube " + vubeName);
        }

        SchedulerJobInstance scheduler = deserializeSchedulerJobInstance(kapCubeRequest);
        CubeInstance cube = cubeService.getCubeManager().getCube(vube.getLatestCube().getName());

        CubeDesc managerDesc = deserializeCubeDesc(kapCubeRequest);

        if (cube != null) {
            try {
                CubeDesc desc = cube.getDescriptor();
                desc.setPartitionDateStart(managerDesc.getPartitionDateStart());
                desc.setAutoMergeTimeRanges(managerDesc.getAutoMergeTimeRanges());
                desc.setRetentionRange(managerDesc.getRetentionRange());
                desc.setNotifyList(managerDesc.getNotifyList());
                desc.setStatusNeedNotify(managerDesc.getStatusNeedNotify());
                desc.setEngineType(managerDesc.getEngineType());

                Map<String, String> overrideKylinProps = desc.getOverrideKylinProps();
                Map<String, String> cubeConfig = managerDesc.getOverrideKylinProps();

                for (String key : cubeConfig.keySet()) {
                    if (overrideKylinProps.containsKey(key)) {
                        overrideKylinProps.put(key, cubeConfig.get(key));
                    }
                }

                vubeService.getCubeDescManager().updateCubeDesc(desc);
            } catch (Exception e) {
                String message = "Failed to update vube manager date: " + vubeName + " : " + managerDesc.toString();
                logger.error(message, e);
                throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
            }
        }

        if (scheduler != null) {
            schedulerJobService.pauseScheduler(vube.getName());
            ProjectInstance project = cubeService.getProjectManager().getProject(cube.getProject());
            scheduler.setPartitionStartTime(cube.getDescriptor().getPartitionDateStart());
            bindSchedulerJobWithVube(scheduler, vubeName, project);

            if (scheduler.isEnabled()) {
                schedulerJobService.enableSchedulerJob(scheduler, project);
            } else {
                schedulerJobService.disableSchedulerJob(scheduler, project);
            }
        }

        return vube;
    }

    private EnvelopeResponse buildUpdateCubeDescResponse(CubeDesc cubeDesc, RawTableDesc rawTableDesc,
            SchedulerJobInstance schedule, String version, long createTime) throws JsonProcessingException {
        GeneralResponse result = new GeneralResponse();
        result.setProperty("version", version);
        result.setProperty("createTime", String.valueOf(createTime));
        result.setProperty("cubeUuid", cubeDesc.getUuid());
        result.setProperty("cubeDescData", JsonUtil.writeValueAsIndentString(cubeDesc));
        if (rawTableDesc != null)
            result.setProperty("rawTableDescData", JsonUtil.writeValueAsIndentString(rawTableDesc));
        if (schedule != null)
            result.setProperty("schedulerJobData", JsonUtil.writeValueAsIndentString(schedule));

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/{projectName}/{vubeName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteVubeAndDraft(@PathVariable String projectName, @PathVariable String vubeName)
            throws IOException, SchedulerException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube != null) {
            String project = projectService.getProjectOfCube(vube.getLatestCube().getName());

            ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            ResourceStore.Checkpoint cp = store.checkpoint();
            try {
                deleteVube(vubeName);
            } catch (Exception ex) {
                cp.rollback();
                cacheService.wipeProjectCache(project);
                throw ex;
            } finally {
                cp.close();
            }
        }

        // delete draft is not critical and can be out of checkpoint/rollback
        Draft draft = cubeService.getCubeDraft(vubeName, projectName);
        if (draft != null) {
            cubeService.deleteDraft(draft);
            kapCubeService.deleteCubeOptLog(vubeName);
        }
    }

    private void deleteVube(String vubeName) throws IOException, SchedulerException {
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);

        if (vube != null) {
            deleteScheduler(vubeName);

            for (CubeInstance cube : vube.getVersionedCubes()) {
                CubeInstance realCube = cubeService.getCubeManager().getCube(cube.getName());
                RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cube.getName());
                if (null != raw) {
                    rawTableService.deleteRaw(raw, realCube);
                }
                cubeService.deleteCube(realCube);
            }
        }

        try {
            vubeService.deleteVube(vube);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete vube. " + " Caused by: " + e.getMessage(), e);
        }
    }

    private void deleteScheduler(String vubeName) throws IOException, SchedulerException {
        SchedulerJobInstance job = getSchedulerJobByVubeName(vubeName);

        if (job != null) {
            ProjectInstance project = projectService.getProjectManager().getProject(job.getProject());
            schedulerJobService.deleteSchedulerJob(job, project);
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

    private void bindSchedulerJobWithVube(SchedulerJobInstance schedule, String vubeName, ProjectInstance project)
            throws IOException, SchedulerException {
        SchedulerJobInstance older = getSchedulerJobByVubeName(vubeName);
        VubeInstance vube = vubeService.getVubeManager().getVubeInstance(vubeName);
        if (null != older)
            schedulerJobService.deleteSchedulerJob(older, project);

        schedule.setRelatedRealization(vubeName);
        schedule.setName(vubeName);
        schedule.setRelatedRealizationUuid(vube.getUuid());
        schedule.setRealizationType("vube");

        long localRunTime = schedulerJobService.utcLocalConvert(schedule.getScheduledRunTime(), true);

        if (localRunTime < System.currentTimeMillis()) {
            schedule.setScheduledRunTime(schedulerJobService.utcLocalConvert(System.currentTimeMillis(), false));
        } else {
            schedule.setScheduledRunTime(schedule.getScheduledRunTime());
        }

        Segments<CubeSegment> segments = vube.getAllSegments();

        if (schedule.getPartitionStartTime() < segments.getTSEnd()) {
            schedule.setPartitionStartTime(segments.getTSEnd());
        }
        schedulerJobService.saveSchedulerJob(schedule, vube, project);
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public void setRawTableService(RawTableService rawTableService) {
        this.rawTableService = rawTableService;
    }

    public void setSchedulerJobService(SchedulerJobService schedulerJobService) {
        this.schedulerJobService = schedulerJobService;
    }

    public void setVubeService(VubeService vubeService) {
        this.vubeService = vubeService;
    }

    public void setKapSuggestionService(KapSuggestionService kapSuggestionService) {
        this.kapSuggestionService = kapSuggestionService;
    }

    public void setProjectService(ProjectService projectService) {
        this.projectService = projectService;
    }
}
