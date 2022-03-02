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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SEGMENT_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_MERGE_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_REFRESH_SEGMENT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Sets;

import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.IncrementBuildSegmentsRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentFixRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.MergeSegmentCheckResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.log4j.Log4j;

@Log4j
@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class SegmentController extends BaseController {

    public static final String MODEL_ID = "modelId";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @ApiOperation(value = "buildIndicesManually", tags = { "DW" }, notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/indices")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model") String modelId,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        checkRequiredArg(MODEL_ID, modelId);

        modelService.validateCCType(modelId, request.getProject());

        val response = modelBuildService.buildIndicesManually(modelId, request.getProject(), request.getPriority(),
                request.getYarnQueue(), request.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    /* Segments */
    @ApiOperation(value = "getSegments", tags = {
            "DW" }, notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{dataflow:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "dataflow") String dataflowId, //
            @RequestParam(value = "project") String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "0") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "with_indexes", required = false) List<Long> withAllIndexes,
            @RequestParam(value = "without_indexes", required = false) List<Long> withoutAnyIndexes,
            @RequestParam(value = "all_to_complement", required = false, defaultValue = "false") Boolean allToComplement,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean reverse) {
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(dataflowId, project, start, end, status,
                withAllIndexes, withoutAnyIndexes, allToComplement, sortBy, reverse);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(segments, offset, limit), "");
    }

    @ApiOperation(value = "fixSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segment_holes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> fixSegHoles(@PathVariable("model") String modelId,
            @RequestBody SegmentFixRequest segmentsRequest) throws Exception {
        checkProjectName(segmentsRequest.getProject());
        checkRequiredArg("segment_holes", segmentsRequest.getSegmentHoles());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(segmentsRequest.getProject(), modelId);
        segmentsRequest.getSegmentHoles()
                .forEach(seg -> validateDataRange(seg.getStart(), seg.getEnd(), partitionColumnFormat));
        JobInfoResponse response = modelService.fixSegmentHoles(segmentsRequest.getProject(), modelId,
                segmentsRequest.getSegmentHoles(), segmentsRequest.getIgnoredSnapshotTables());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "checkSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkSegment(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(buildSegmentsRequest.getProject(),
                modelId);
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        val res = modelService.checkSegHoleExistIfNewRangeBuild(buildSegmentsRequest.getProject(), modelId,
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "checkSegmentsIfDelete", tags = { "DW" })
    @GetMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkHolesIfSegDeleted(@PathVariable("model") String model,
            @RequestParam("project") String project, @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        val res = modelService.checkSegHoleIfSegDeleted(model, project, ids);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "deleteSegments", tags = { "DW" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{dataflow:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("dataflow") String dataflowId,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        checkProjectName(project);

        if (purge) {
            modelService.purgeModelManually(dataflowId, project);
        } else {
            checkSegmentParams(ids, names);
            String[] idsDeleted = modelService.convertSegmentIdWithName(dataflowId, project, ids, names);
            if (ArrayUtils.isEmpty(idsDeleted)) {
                throw new KylinException(EMPTY_SEGMENT_ID, MsgPicker.getMsg().getSEGMENT_LIST_IS_EMPTY());
            }
            modelService.deleteSegmentById(dataflowId, project, idsDeleted, force);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        checkSegmentParams(request.getIds(), request.getNames());
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        String[] segIds = modelService.convertSegmentIdWithName(modelId, request.getProject(), request.getIds(),
                request.getNames());

        if (SegmentsRequest.SegmentsRequestType.REFRESH == request.getType()) {
            if (ArrayUtils.isEmpty(segIds)) {
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
            }
            jobInfos = modelBuildService.refreshSegmentById(
                    new RefreshSegmentParams(request.getProject(), modelId, segIds, request.isRefreshAllIndexes())
                            .withIgnoredSnapshotTables(request.getIgnoredSnapshotTables())
                            .withPriority(request.getPriority()).withPartialBuild(request.isPartialBuild())
                            .withBatchIndexIds(request.getBatchIndexIds()).withYarnQueue(request.getYarnQueue())
                            .withTag(request.getTag()));
        } else {
            if (ArrayUtils.isEmpty(segIds) || segIds.length < 2) {
                throw new KylinException(FAILED_MERGE_SEGMENT,
                        MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
            }
            val jobInfo = modelBuildService.mergeSegmentsManually(
                    new MergeSegmentParams(request.getProject(), modelId, segIds).withPriority(request.getPriority())
                            .withYarnQueue(request.getYarnQueue()).withTag(request.getTag()));
            if (jobInfo != null) {
                jobInfos.add(jobInfo);
            }
        }
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segments/merge_check")
    @ResponseBody
    public EnvelopeResponse<MergeSegmentCheckResponse> checkMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        if (ArrayUtils.isEmpty(request.getIds()) || request.getIds().length < 2) {
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
        }
        Pair<Long, Long> merged = modelService
                .checkMergeSegments(new MergeSegmentParams(request.getProject(), modelId, request.getIds()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new MergeSegmentCheckResponse(merged.getFirst(), merged.getSecond()), "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(buildSegmentsRequest.getProject(),
                modelId);
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());
        JobInfoResponse response = modelBuildService.buildSegmentsManually(buildSegmentsRequest.getProject(), modelId,
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.isBuildAllIndexes(), buildSegmentsRequest.getIgnoredSnapshotTables(),
                buildSegmentsRequest.getSubPartitionValues(), buildSegmentsRequest.getPriority(),
                buildSegmentsRequest.isBuildAllSubPartitions(), buildSegmentsRequest.getBatchIndexIds(),
                buildSegmentsRequest.isPartialBuild(), buildSegmentsRequest.getYarnQueue(),
                buildSegmentsRequest.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/model_segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> incrementBuildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody IncrementBuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        String partitionColumnFormat = buildSegmentsRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());

        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.getPartitionDesc(), buildSegmentsRequest.getMultiPartitionDesc(),
                buildSegmentsRequest.getSegmentHoles(), buildSegmentsRequest.isBuildAllIndexes(),
                buildSegmentsRequest.getSubPartitionValues())
                        .withIgnoredSnapshotTables(buildSegmentsRequest.getIgnoredSnapshotTables())
                        .withPriority(buildSegmentsRequest.getPriority())
                        .withBuildAllSubPartitions(buildSegmentsRequest.isBuildAllSubPartitions())
                        .withYarnQueue(buildSegmentsRequest.getYarnQueue()).withTag(buildSegmentsRequest.getTag());

        JobInfoResponse response = fusionModelService.incrementBuildSegmentsManually(incrParams);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        val response = fusionModelService.addIndexesToSegments(modelId, buildSegmentsRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/all_indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addAllIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        JobInfoResponseWithFailure response = modelBuildService.addIndexesToSegments(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getSegmentIds(), null, buildSegmentsRequest.isParallelBuildBySegment(),
                buildSegmentsRequest.getPriority());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes/deletion")
    @ResponseBody
    public EnvelopeResponse<String> deleteIndexesFromSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest deleteSegmentsRequest) {
        checkProjectName(deleteSegmentsRequest.getProject());
        modelService.removeIndexesFromSegments(deleteSegmentsRequest.getProject(), modelId,
                deleteSegmentsRequest.getSegmentIds(), deleteSegmentsRequest.getIndexIds());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsBuildRequest param) {
        checkProjectName(param.getProject());
        checkParamLength("tag", param.getTag(), 1024);
        checkRequiredArg("segment_id", param.getSegmentId());
        checkRequiredArg("sub_partition_values", param.getSubPartitionValues());
        val response = modelBuildService.buildSegmentPartitionByValue(param.getProject(), modelId, param.getSegmentId(),
                param.getSubPartitionValues(), param.isParallelBuildBySegment(), param.isBuildAllSubPartitions(),
                param.getPriority(), param.getYarnQueue(), param.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PutMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsRefreshRequest param) {
        checkProjectName(param.getProject());
        checkParamLength("tag", param.getTag(), 1024);
        checkRequiredArg("segment_id", param.getSegmentId());
        val response = modelBuildService.refreshSegmentPartition(param, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "deleteMultiPartition", tags = { "DW" })
    @DeleteMapping(value = "/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartition(@RequestParam("model") String modelId,
            @RequestParam("project") String project, @RequestParam("segment") String segment,
            @RequestParam(value = "ids") String[] ids) {
        checkProjectName(project);
        HashSet<Long> partitions = Sets.newHashSet();
        Arrays.stream(ids).forEach(id -> partitions.add(Long.parseLong(id)));
        modelService.deletePartitions(project, segment, modelId, partitions);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMultiPartitions", tags = { "DW" })
    @GetMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SegmentPartitionResponse>>> getMultiPartition(
            @PathVariable("model") String modelId, @RequestParam("project") String project,
            @RequestParam("segment_id") String segId,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset, //
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        val responseList = modelService.getSegmentPartitions(project, modelId, segId, status, sortBy, reverse);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(responseList, pageOffset, pageSize),
                "");
    }

}
