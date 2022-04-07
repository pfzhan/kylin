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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_PARSE_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SEGMENT_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_SEGMENT_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SEGMENT_NOT_EXIST;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.rest.controller.BaseController;
import io.kyligence.kap.rest.request.CubeRebuildRequest;
import io.kyligence.kap.rest.request.SegmentMgmtRequest;
import io.kyligence.kap.rest.response.JobInfoResponseV2;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataModelResponse3X;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@RestController
@RequestMapping(value = "/api/cubes", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class SegmentControllerV2 extends BaseController {

    private static final String FAILED_CUBE_MSG = "Can not find the cube.";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @ApiOperation(value = "getCubes", tags = { "AI" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse getCubes(@RequestParam(value = "projectName") String project,
            @RequestParam(value = "modelName", required = false) String modelAlias,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<NDataModelResponse> modelsResponse = new ArrayList<>(modelService.getCubes(modelAlias, project));

        List<NDataModelResponse3X> result = Lists.newArrayList();
        try {
            for (NDataModelResponse response : modelsResponse) {
                result.add(NDataModelResponse3X.convert(response));
            }
        } catch (Exception e) {
            throw new KylinException(FAILED_PARSE_JSON, e);
        }

        HashMap<String, Object> modelResponse = getDataResponse("cubes", result, offset, limit);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelResponse, "");
    }

    @ApiOperation(value = "getCube", tags = { "AI" })
    @GetMapping(value = "/{cubeName}")
    @ResponseBody
    public EnvelopeResponse getCube(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project) {
        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new KylinException(MODEL_NOT_EXIST, FAILED_CUBE_MSG);
        }

        NDataModelResponse3X result;
        try {
            result = NDataModelResponse3X.convert(dataModelResponse);
        } catch (Exception e) {
            throw new KylinException(FAILED_PARSE_JSON, e);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "rebuild", tags = { "DW" })
    @PutMapping(value = "/{cubeName}/rebuild")
    @ResponseBody
    public EnvelopeResponse rebuild(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project, @RequestBody CubeRebuildRequest request)
            throws Exception {
        String startTime = String.valueOf(request.getStartTime());
        String endTime = String.valueOf(request.getEndTime());

        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new KylinException(MODEL_NOT_EXIST, FAILED_CUBE_MSG);
        }
        String partitionColumnFormat = modelService.getPartitionColumnFormatByAlias(dataModelResponse.getProject(),
                modelAlias);
        validateDataRange(startTime, endTime, partitionColumnFormat);
        JobInfoResponseV2 result = null;
        switch (request.getBuildType()) {
        case "BUILD":
            val buildResponse = modelBuildService.buildSegmentsManually(dataModelResponse.getProject(),
                    dataModelResponse.getId(), startTime, endTime);
            if (CollectionUtils.isNotEmpty(buildResponse.getJobs())) {
                result = JobInfoResponseV2
                        .convert(buildResponse.getJobs().stream()
                                .filter(job -> JobTypeEnum.INC_BUILD.name().equals(job.getJobName())
                                        || JobTypeEnum.INDEX_REFRESH.name().equals(job.getJobName()))
                                .findFirst().orElse(null));
            }
            break;
        case "REFRESH":
            List<String> idList = dataModelResponse.getSegments().stream()
                    .filter(segment -> segment.getStartTime() >= request.getStartTime()
                            && segment.getEndTime() <= request.getEndTime())
                    .map(NDataSegmentResponse::getId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(idList)) {
                throw new KylinException(INVALID_SEGMENT_PARAMETER,
                        "You should choose at least one segment to refresh!");
            }
            if (idList.size() > 1) {
                throw new KylinException(INVALID_SEGMENT_PARAMETER,
                        "You should choose at most one segment to refresh!");
            }
            val refreshResponse = modelBuildService.refreshSegmentById(new RefreshSegmentParams(
                    dataModelResponse.getProject(), dataModelResponse.getId(), idList.toArray(new String[0])));
            if (CollectionUtils.isNotEmpty(refreshResponse)) {
                result = JobInfoResponseV2.convert(refreshResponse.stream()
                        .filter(job -> JobTypeEnum.INDEX_REFRESH.name().equals(job.getJobName())).findFirst()
                        .orElse(null));
            }
            break;
        default:
            return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, null, "Invalid build type.");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "manageSegments", tags = { "DW" })
    @PutMapping(value = "/{cubeName}/segments")
    @ResponseBody
    public EnvelopeResponse manageSegments(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project,
            @RequestBody SegmentMgmtRequest request) {
        if (CollectionUtils.isEmpty(request.getSegments())) {
            throw new KylinException(EMPTY_SEGMENT_ID, "You should choose at least one segment!");
        }

        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new KylinException(MODEL_NOT_EXIST, FAILED_CUBE_MSG);
        }

        List<NDataSegmentResponse> segList = dataModelResponse.getSegments().stream()
                .filter(segment -> request.getSegments().contains(segment.getName())).collect(Collectors.toList());
        Set<String> segNameSet = segList.stream().map(NDataSegmentResponse::getName).collect(Collectors.toSet());
        Set<String> notExistSegList = request.getSegments().stream().filter(name -> !segNameSet.contains(name))
                .collect(Collectors.toSet());

        if (CollectionUtils.isNotEmpty(notExistSegList)) {
            throw new KylinException(SEGMENT_NOT_EXIST, String.format(Locale.ROOT,
                    "Can not find those segment names: [%s]", StringUtils.join(notExistSegList.iterator(), ",")));
        }

        Set<String> idList = segList.stream().map(NDataSegmentResponse::getId).collect(Collectors.toSet());
        switch (request.getBuildType()) {
        case "MERGE":
            if (idList.size() < 2) {
                throw new KylinException(INVALID_SEGMENT_PARAMETER,
                        "You should choose at least two segments to merge!");
            }
            val mergeResponse = modelBuildService.mergeSegmentsManually(new MergeSegmentParams(
                    dataModelResponse.getProject(), dataModelResponse.getId(), idList.toArray(new String[0])));
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, JobInfoResponseV2.convert(mergeResponse), "");
        case "REFRESH":
            if (CollectionUtils.isEmpty(idList)) {
                throw new KylinException(INVALID_SEGMENT_PARAMETER,
                        "You should choose at least one segment to refresh!");
            }
            val refreshResponse = modelBuildService.refreshSegmentById(new RefreshSegmentParams(
                    dataModelResponse.getProject(), dataModelResponse.getId(), idList.toArray(new String[0])));
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, JobInfoResponseV2.convert(refreshResponse), "");
        case "DROP":
            if (CollectionUtils.isEmpty(idList)) {
                throw new KylinException(INVALID_SEGMENT_PARAMETER, "You should choose at least one segment to drop!");
            }
            modelService.deleteSegmentById(dataModelResponse.getId(), dataModelResponse.getProject(),
                    idList.toArray(new String[0]), true);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "Drop segments successfully");
        default:
            return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "Invalid build type.");
        }
    }

    @ApiOperation(value = "getHoles", tags = { "DW" })
    @GetMapping(value = "/{cubeName}/holes")
    @ResponseBody
    public EnvelopeResponse getHoles(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project) {
        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new KylinException(MODEL_NOT_EXIST, FAILED_CUBE_MSG);
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

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, holes, "");
    }

    @ApiOperation(value = "getSql", tags = { "AI" })
    @GetMapping(value = "/{cubeName}/sql")
    @ResponseBody
    public EnvelopeResponse getSql(@PathVariable("cubeName") String modelAlias,
            @RequestParam(value = "project", required = false) String project) {
        NDataModelResponse dataModelResponse = modelService.getCube(modelAlias, project);
        if (Objects.isNull(dataModelResponse)) {
            throw new KylinException(MODEL_NOT_EXIST, FAILED_CUBE_MSG);
        }

        String sql = modelService.getModelSql(dataModelResponse.getId(), dataModelResponse.getProject());
        Properties response = new Properties();
        response.setProperty("sql", sql);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

}
