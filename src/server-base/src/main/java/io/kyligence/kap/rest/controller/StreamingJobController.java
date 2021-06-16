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

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.rest.request.StreamingJobParamsRequest;
import io.kyligence.kap.rest.response.StreamingJobDataStatsResponse;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.StreamingJobService;
import io.kyligence.kap.streaming.request.LayoutUpdateRequest;
import io.kyligence.kap.streaming.request.SegmentMergeRequest;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

@Controller
@RequestMapping(value = "/api/streaming_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class StreamingJobController extends NBasicController {

    @Autowired
    @Qualifier("streamingJobService")
    private StreamingJobService streamingJobService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<StreamingJobResponse>>> getStreamingJobList(
            @RequestParam(value = "model_name", required = false, defaultValue = "") String modelName,
            @RequestParam(value = "model_names", required = false) List<String> modelNames,
            @RequestParam(value = "job_types", required = false, defaultValue = "") List<String> jobTypes,
            @RequestParam(value = "statuses", required = false, defaultValue = "") List<String> statuses,
            @RequestParam(value = "project", required = false, defaultValue = "") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_update_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse) {
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, modelNames, jobTypes, statuses, project,
                sortBy, reverse);
        val modelList = modelService.getModels(StringUtils.EMPTY, project, false, "", null, "last_modify", true,
                null, null, null);
        val data = streamingJobService.getStreamingJobList(jobFilter, pageOffset, pageSize, modelList);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    @GetMapping(value = "/model_name")
    @ResponseBody
    public EnvelopeResponse<List<String>> getStreamingModelNameList(
            @RequestParam(value = "model_name", required = false, defaultValue = "") String modelName,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize) {
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, Collections.EMPTY_LIST,
                Arrays.asList("STREAMING_BUILD"), Collections.EMPTY_LIST, project, "last_update_time", true);
        List<String> data;
        val dataResult = streamingJobService.getStreamingJobList(jobFilter, 0, pageSize);
        if (dataResult != null) {
            data = dataResult.getValue().stream().map(item -> item.getModelName()).collect(Collectors.toList());
        } else {
            data = new ArrayList<>();
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "updateStreamingJobStatus", notes = "Update Body: jobId")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobStatus(
            @RequestBody StreamingJobExecuteRequest streamingJobExecuteRequest) {
        checkRequiredArg("action", streamingJobExecuteRequest.getAction());
        checkProjectName(streamingJobExecuteRequest.getProject());
        streamingJobService.updateStreamingJobStatus(streamingJobExecuteRequest.getProject(),
                streamingJobExecuteRequest.getJobIds(), streamingJobExecuteRequest.getAction());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/params")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobParams(
            @RequestBody StreamingJobParamsRequest streamingJobParamsRequest) {
        checkProjectName(streamingJobParamsRequest.getProject());
        streamingJobService.updateStreamingJobParams(streamingJobParamsRequest.getProject(),
                streamingJobParamsRequest.getJobId(), streamingJobParamsRequest.getParams());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/stats/{jobId:.+}")
    @ResponseBody
    public EnvelopeResponse<StreamingJobDataStatsResponse> getStreamingJobDataStats(
            @PathVariable(value = "jobId") String jobId, @RequestParam(value = "project") String project,
            @RequestParam(value = "time_filter", required = false, defaultValue = "-1") Integer timeFilter) {
        val response = streamingJobService.getStreamingJobDataStats(jobId, project, timeFilter);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PutMapping(value = "/stats")
    @ResponseBody
    public EnvelopeResponse<String> collectStreamingJobStats(
            @RequestBody StreamingJobStatsRequest streamingJobStatsRequest) {
        checkProjectName(streamingJobStatsRequest.getProject());
        streamingJobService.collectStreamingJobStats(streamingJobStatsRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/spark")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobInfo(
            @RequestBody StreamingJobUpdateRequest streamingJobUpdateRequest) {
        checkProjectName(streamingJobUpdateRequest.getProject());
        streamingJobService.updateStreamingJobInfo(streamingJobUpdateRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/records")
    @ResponseBody
    public EnvelopeResponse<List<StreamingJobRecord>> getStreamingJobRecordList(
            @RequestParam(value = "project") String project, @RequestParam(value = "job_id") String jobId) {
        checkProjectName(project);
        val data = streamingJobService.getStreamingJobRecordList(project, jobId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    @PostMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse addSegment(@RequestBody SegmentMergeRequest request) {
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        SegmentRange segRange = request.getSegmentRange();
        checkProjectName(request.getProject());
        String newSegId = streamingJobService.addSegment(project, dataflowId, segRange, request.getLayer(),
                request.getNewSegId());
        return new RestResponse(newSegId);
    }

    @PutMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse updateSegment(@RequestBody SegmentMergeRequest request) {
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        String segId = request.getNewSegId();
        List<NDataSegment> removeSegmentList = request.getRemoveSegment();
        String status = request.getStatus();
        checkProjectName(request.getProject());
        streamingJobService.updateSegment(project, dataflowId, segId, removeSegmentList, status);
        return RestResponse.ok();
    }

    @PostMapping(value = "/dataflow/segment/deletion")
    @ResponseBody
    public RestResponse deleteSegment(@RequestBody SegmentMergeRequest request) {
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataSegment> removeSegmentList = request.getRemoveSegment();
        checkProjectName(request.getProject());
        streamingJobService.deleteSegment(project, dataflowId, removeSegmentList);
        return RestResponse.ok();
    }

    @PutMapping(value = "/dataflow/layout")
    @ResponseBody
    public RestResponse updateLayout(@RequestBody LayoutUpdateRequest request) {
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataLayout> layouts = request.getLayouts();
        List<NDataSegDetails> segDetails = request.getSegDetails();
        for (int i = 0; i < layouts.size(); i++) {
            layouts.get(i).setSegDetails(segDetails.get(i));
        }
        checkProjectName(project);
        streamingJobService.updateLayout(project, dataflowId, layouts);

        return RestResponse.ok();
    }

}
