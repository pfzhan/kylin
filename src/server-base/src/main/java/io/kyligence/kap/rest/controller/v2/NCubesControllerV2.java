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
package io.kyligence.kap.rest.controller.v2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.CubeRebuildRequest;
import io.kyligence.kap.rest.request.SegmentMgmtRequest;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/cubes")
public class NCubesControllerV2 extends NBasicController {

    private static final Message msg = MsgPicker.getMsg();
    private static final String FAILED_CUBE_MSG = "Can not find the cube.";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private ModelSemanticHelper semanticService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubes(@RequestParam(value = "projectName", required = false) String project,
            @RequestParam(value = "modelName", required = false) String modelAlias,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>();
        for (NDataModelResponse modelDesc : modelService.getCubes(modelAlias, project)) {
            Preconditions.checkState(!modelDesc.isDraft());
            models.add(modelDesc);
        }

        HashMap<String, Object> modelResponse = getDataResponse("cubes", models, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCube(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project) {
        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new BadRequestException(FAILED_CUBE_MSG, ResponseCode.CODE_UNDEFINED);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, dataModelResponse, "");
    }

    @RequestMapping(value = "/{cubeName}/rebuild", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuild(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project, @RequestBody CubeRebuildRequest request)
            throws Exception {
        String startTime = String.valueOf(request.getStartTime());
        String endTime = String.valueOf(request.getEndTime());
        validateDataRange(startTime, endTime);

        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new BadRequestException(FAILED_CUBE_MSG, ResponseCode.CODE_UNDEFINED);
        }

        switch (request.getBuildType()) {
        case "BUILD":
            modelService.buildSegmentsManually(dataModelResponse.getProject(), dataModelResponse.getId(), startTime,
                    endTime);
            break;
        case "REFRESH":
            List<String> idList = dataModelResponse.getSegments().stream()
                    .filter(segment -> segment.getStartTime() >= request.getStartTime()
                            && segment.getEndTime() <= request.getEndTime())
                    .map(NDataSegmentResponse::getId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(idList)) {
                throw new BadRequestException("You should choose at least one segment to refresh!");
            }
            modelService.refreshSegmentById(dataModelResponse.getId(), dataModelResponse.getProject(),
                    idList.toArray(new String[0]));
            break;
        default:
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, "Invalid build type.", "");
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{cubeName}/segments", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse manageSegments(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project,
            @RequestBody SegmentMgmtRequest request) {
        if (CollectionUtils.isEmpty(request.getSegments())) {
            throw new BadRequestException("You should choose at least one segment!");
        }

        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new BadRequestException(FAILED_CUBE_MSG, ResponseCode.CODE_UNDEFINED);
        }

        Set<String> idList = dataModelResponse.getSegments().stream()
                .filter(segment -> request.getSegments().contains(segment.getName())).map(NDataSegmentResponse::getId)
                .collect(Collectors.toSet());

        switch (request.getBuildType()) {
        case "MERGE":
            if (idList.size() < 2) {
                throw new BadRequestException("You should choose at least two segments to merge!");
            }

            modelService.mergeSegmentsManually(dataModelResponse.getId(), dataModelResponse.getProject(),
                    idList.toArray(new String[0]));
            break;
        case "REFRESH":
            if (CollectionUtils.isEmpty(idList)) {
                throw new BadRequestException("You should choose at least one segment to refresh!");
            }
            modelService.refreshSegmentById(dataModelResponse.getId(), dataModelResponse.getProject(),
                    idList.toArray(new String[0]));
            break;
        default:
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, "Invalid build type.", "");
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHoles(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project) {
        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new BadRequestException(FAILED_CUBE_MSG, ResponseCode.CODE_UNDEFINED);
        }

        List<NDataSegment> holes = Lists.newArrayList();
        List<NDataSegmentResponse> segments = dataModelResponse.getSegments();

        Collections.sort(segments);
        for (int i = 0; i < segments.size() - 1; ++i) {
            NDataSegment first = segments.get(i);
            NDataSegment second = segments.get(i + 1);
            if (first.getSegRange().connects(second.getSegRange()))
                continue;

            if (first.getSegRange().apartBefore(second.getSegRange())) {
                NDataSegmentResponse hole = new NDataSegmentResponse();
                hole.setSegmentRange(first.getSegRange().gapTill(second.getSegRange()));
                hole.setTimeRange(new TimeRange(first.getTSRange().getEnd(), second.getTSRange().getStart()));
                hole.setName(Segments.makeSegmentName(hole.getSegRange()));

                holes.add(hole);
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, holes, "");
    }

}
