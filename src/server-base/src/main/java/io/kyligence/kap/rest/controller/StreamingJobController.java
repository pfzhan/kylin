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
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.request.StreamingJobParamsRequest;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.rest.service.StreamingJobService;
import io.kyligence.kap.streaming.request.LayoutUpdateRequest;
import io.kyligence.kap.streaming.request.SegmentMergeRequest;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.swagger.annotations.ApiOperation;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

@Controller
@RequestMapping(value = "/api/streaming_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON})
public class StreamingJobController extends NBasicController {

    @Autowired
    @Qualifier("streamingJobService")
    private StreamingJobService streamingJobService;

    @ApiOperation(value = "updateStreamingJobStatus", notes = "Update Body: jobId")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobStatus(@RequestBody StreamingJobExecuteRequest streamingJobExecuteRequest) {
        checkRequiredArg("action", streamingJobExecuteRequest.getAction());
        checkProjectName(streamingJobExecuteRequest.getProject());
        streamingJobService.updateStreamingJobStatus(streamingJobExecuteRequest.getProject(),
                streamingJobExecuteRequest.getModelId(),
                streamingJobExecuteRequest.getAction());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/params")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobParams(@RequestBody StreamingJobParamsRequest streamingJobParamsRequest) {
        checkProjectName(streamingJobParamsRequest.getProject());
        streamingJobService.updateStreamingJobParams(streamingJobParamsRequest.getProject(),
                streamingJobParamsRequest.getModelId(),
                streamingJobParamsRequest.getBuildParams(),
                streamingJobParamsRequest.getMergeParams());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/stats")
    @ResponseBody
    public EnvelopeResponse<String> collectStreamingJobStats(@RequestBody StreamingJobStatsRequest streamingJobStatsRequest) {
        checkProjectName(streamingJobStatsRequest.getProject());
        streamingJobService.collectStreamingJobStats(streamingJobStatsRequest.getJobId(),
                streamingJobStatsRequest.getProject(),
                streamingJobStatsRequest.getBatchRowNum(),
                streamingJobStatsRequest.getRowsPerSecond(),
                streamingJobStatsRequest.getDurationMs(),
                streamingJobStatsRequest.getTriggerStartTime());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/spark")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobInfo(@RequestBody StreamingJobUpdateRequest streamingJobUpdateRequest) {
        checkProjectName(streamingJobUpdateRequest.getProject());
        streamingJobService.updateStreamingJobInfo(streamingJobUpdateRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<StreamingJobResponse> getStreamingJob(@RequestParam(value = "project") String project,
                                                                  @RequestParam(value = "model_id") String modelId) {
        checkProjectName(project);
        StreamingJobResponse response = streamingJobService.getStreamingJobInfo(modelId, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PostMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse addSegment(@RequestBody SegmentMergeRequest request){
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        SegmentRange segRange = request.getSegmentRange();
        checkProjectName(request.getProject());
        String newSegId = streamingJobService.addSegment(project, dataflowId, segRange, request.getLayer(), request.getNewSegId());
        return new RestResponse(newSegId);
    }

    @PutMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse updateSegment(@RequestBody SegmentMergeRequest request){
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        String segId = request.getNewSegId();
        List<NDataSegment> removeSegmentList =
                request.getRemoveSegment();
        String status = request.getStatus();
        checkProjectName(request.getProject());
        streamingJobService.updateSegment(project, dataflowId, segId, removeSegmentList, status);
        return RestResponse.ok();
    }

    @PostMapping(value = "/dataflow/segment/deletion")
    @ResponseBody
    public RestResponse deleteSegment(@RequestBody SegmentMergeRequest request){
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataSegment> removeSegmentList =
                request.getRemoveSegment();
        checkProjectName(request.getProject());
        streamingJobService.deleteSegment(project, dataflowId, removeSegmentList);
        return RestResponse.ok();
    }

    @PutMapping(value = "/dataflow/layout")
    @ResponseBody
    public RestResponse updateLayout(@RequestBody LayoutUpdateRequest request){
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataLayout> layouts = request.getLayouts();
        List<NDataSegDetails> segDetails = request.getSegDetails();
        for(int i = 0; i < layouts.size(); i++){
            layouts.get(i).setSegDetails(segDetails.get(i));
        }
        checkProjectName(project);
        streamingJobService.updateLayout(project, dataflowId, layouts);

        return RestResponse.ok();
    }

}
