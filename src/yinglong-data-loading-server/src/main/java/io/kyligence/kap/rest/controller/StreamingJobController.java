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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.rest.request.StreamingJobParamsRequest;
import io.kyligence.kap.rest.response.StreamingJobDataStatsResponse;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.rest.service.StreamingJobService;
import io.kyligence.kap.streaming.request.LayoutUpdateRequest;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.request.StreamingRequestHeader;
import io.kyligence.kap.streaming.request.StreamingSegmentRequest;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/streaming_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class StreamingJobController extends NBasicController {

    @Autowired
    @Qualifier("streamingJobService")
    private StreamingJobService streamingJobService;

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
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse) {
        checkStreamingEnabled();
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, modelNames, jobTypes, statuses, project,
                sortBy, reverse);
        val data = streamingJobService.getStreamingJobList(jobFilter, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @GetMapping(value = "/model_name")
    @ResponseBody
    public EnvelopeResponse<List<String>> getStreamingModelNameList(
            @RequestParam(value = "model_name", required = false, defaultValue = "") String modelName,
            @RequestParam(value = "project", required = false, defaultValue = "") String project,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize) {
        checkStreamingEnabled();
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, Collections.EMPTY_LIST,
                Arrays.asList("STREAMING_BUILD"), Collections.EMPTY_LIST, project, "last_modified", true);
        List<String> data;
        val dataResult = streamingJobService.getStreamingJobList(jobFilter, 0, pageSize);
        if (dataResult != null) {
            data = dataResult.getValue().stream().map(item -> item.getModelName()).collect(Collectors.toList());
        } else {
            data = new ArrayList<>();
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "updateStreamingJobStatus", notes = "Update Body: jobId")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobStatus(
            @RequestBody StreamingJobExecuteRequest streamingJobExecuteRequest) {
        checkStreamingEnabled();
        checkRequiredArg("action", streamingJobExecuteRequest.getAction());
        streamingJobService.updateStreamingJobStatus(streamingJobExecuteRequest.getProject(),
                streamingJobExecuteRequest.getJobIds(), streamingJobExecuteRequest.getAction());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/params")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobParams(
            @RequestBody StreamingJobParamsRequest streamingJobParamsRequest) {
        checkStreamingEnabled();
        checkProjectName(streamingJobParamsRequest.getProject());
        streamingJobService.updateStreamingJobParams(streamingJobParamsRequest.getProject(),
                streamingJobParamsRequest.getJobId(), streamingJobParamsRequest.getParams());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/stats/{jobId:.+}")
    @ResponseBody
    public EnvelopeResponse<StreamingJobDataStatsResponse> getStreamingJobDataStats(
            @PathVariable(value = "jobId") String jobId, @RequestParam(value = "project") String project,
            @RequestParam(value = "time_filter", required = false, defaultValue = "-1") Integer timeFilter) {
        checkStreamingEnabled();
        val response = streamingJobService.getStreamingJobDataStats(jobId, timeFilter);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    /**
     * called by build job
     * @param streamingJobStatsRequest
     * @return
     */
    @PutMapping(value = "/stats")
    @ResponseBody
    public EnvelopeResponse<String> collectStreamingJobStats(
            @RequestBody StreamingJobStatsRequest streamingJobStatsRequest) {
        checkStreamingEnabled();
        checkProjectName(streamingJobStatsRequest.getProject());
        val jobId = streamingJobStatsRequest.getJobId();
        checkToken(streamingJobStatsRequest.getProject(), jobId.substring(0, jobId.lastIndexOf("_")),
                streamingJobStatsRequest);
        streamingJobService.collectStreamingJobStats(streamingJobStatsRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * called by build and merge job
     * @param streamingJobUpdateRequest
     * @return
     */
    @PutMapping(value = "/spark")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobInfo(
            @RequestBody StreamingJobUpdateRequest streamingJobUpdateRequest) {
        checkStreamingEnabled();
        checkProjectName(streamingJobUpdateRequest.getProject());
        val meta = streamingJobService.updateStreamingJobInfo(streamingJobUpdateRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Objects.toString(meta.getJobExecutionId(), null),
                "");
    }

    @GetMapping(value = "/records")
    @ResponseBody
    public EnvelopeResponse<List<StreamingJobRecord>> getStreamingJobRecordList(
            @RequestParam(value = "project") String project, @RequestParam(value = "job_id") String jobId) {
        checkStreamingEnabled();
        checkProjectName(project);
        val data = streamingJobService.getStreamingJobRecordList(jobId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    /**
     * called by merge job
     * @param request
     * @return
     */
    @PostMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse addSegment(@RequestBody StreamingSegmentRequest request) {
        checkStreamingEnabled();
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        SegmentRange segRange = request.getSegmentRange();
        checkProjectName(request.getProject());
        checkToken(project, dataflowId, request);
        String newSegId = streamingJobService.addSegment(project, dataflowId, segRange, request.getLayer(),
                request.getNewSegId());
        return new RestResponse(newSegId);
    }

    /**
     * called by merge job
     * @param request
     * @return
     */
    @PutMapping(value = "/dataflow/segment")
    @ResponseBody
    public RestResponse updateSegment(@RequestBody StreamingSegmentRequest request) {
        checkStreamingEnabled();
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        String segId = request.getNewSegId();
        List<NDataSegment> removeSegmentList = request.getRemoveSegment();
        String status = request.getStatus();
        checkProjectName(request.getProject());
        checkToken(project, dataflowId, request);
        streamingJobService.updateSegment(project, dataflowId, segId, removeSegmentList, status,
                request.getSourceCount());
        return RestResponse.ok();
    }

    /**
     * called by merge job
     * @param request
     * @return
     */
    @PostMapping(value = "/dataflow/segment/deletion")
    @ResponseBody
    public RestResponse deleteSegment(@RequestBody StreamingSegmentRequest request) {
        checkStreamingEnabled();
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataSegment> removeSegmentList = request.getRemoveSegment();
        checkProjectName(request.getProject());
        checkToken(project, dataflowId, request);
        streamingJobService.deleteSegment(project, dataflowId, removeSegmentList);
        return RestResponse.ok();
    }

    /**
     * called by build and merge job
     * @param request
     * @return
     */
    @PutMapping(value = "/dataflow/layout")
    @ResponseBody
    public RestResponse updateLayout(@RequestBody LayoutUpdateRequest request) {
        checkStreamingEnabled();
        String project = request.getProject();
        String dataflowId = request.getDataflowId();
        List<NDataLayout> layouts = request.getLayouts();
        List<NDataSegDetails> segDetails = request.getSegDetails();
        for (int i = 0; i < layouts.size(); i++) {
            layouts.get(i).setSegDetails(segDetails.get(i));
        }
        checkProjectName(project);
        checkToken(project, dataflowId, request);
        streamingJobService.updateLayout(project, dataflowId, layouts);

        return RestResponse.ok();
    }

    private void checkToken(String project, String modelId, StreamingRequestHeader request) {
        val taskId = request.getJobExecutionId();
        val jobType = request.getJobType();
        val jobId = StreamingUtils.getJobId(modelId, jobType);
        streamingJobService.checkJobExecutionId(project, jobId, taskId);
    }

    @GetMapping(value = "/{job_id:.+}/simple_log")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getStreamingJobDriverLogSimple(@PathVariable("job_id") String jobId,
            @RequestParam("project") String project) {
        checkStreamingEnabled();
        String projectName = checkProjectName(project);

        Map<String, String> result = new HashMap<>();
        result.put("cmd_output", streamingJobService.getStreamingJobSimpleLog(projectName, jobId));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @GetMapping(value = "/{job_id:.+}/download_log")
    @ResponseBody
    public EnvelopeResponse<String> downloadStreamingJobDriverLog(@PathVariable("job_id") String jobId,
            @RequestParam("project") String project, HttpServletResponse response) {
        checkStreamingEnabled();
        String projectName = checkProjectName(project);

        String downloadFilename = String.format(Locale.ROOT, "%s_%s.log", projectName, jobId);
        InputStream inputStream = streamingJobService.getStreamingJobAllLog(projectName, jobId);
        setDownloadResponse(inputStream, downloadFilename, MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

}
