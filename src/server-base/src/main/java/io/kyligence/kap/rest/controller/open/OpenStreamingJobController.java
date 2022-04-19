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
package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_JOB_ID;

import java.util.List;
import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.rest.response.StreamingJobDataStatsResponse;
import io.kyligence.kap.rest.response.StreamingJobResponse;
import io.kyligence.kap.rest.service.StreamingJobService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/streaming_jobs", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenStreamingJobController extends NBasicController {

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
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "job_ids", required = false, defaultValue = "") List<String> jobIds) {
        checkStreamingEnabled();
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, modelNames, jobTypes, statuses, project,
                sortBy, reverse, jobIds);
        val data = streamingJobService.getStreamingJobList(jobFilter, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "updateStreamingJobStatus", notes = "Update Body: jobId")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobStatus(
            @RequestBody StreamingJobExecuteRequest streamingJobExecuteRequest) {
        checkStreamingEnabled();
        checkRequiredArg("action", streamingJobExecuteRequest.getAction());
        if (CollectionUtils.isEmpty(streamingJobExecuteRequest.getJobIds())) {
            throw new KylinException(EMPTY_JOB_ID, String.format(Locale.ROOT, "'%s' is required.", "job_ids"));
        }
        streamingJobService.updateStreamingJobStatus(streamingJobExecuteRequest.getProject(),
                streamingJobExecuteRequest.getJobIds(), streamingJobExecuteRequest.getAction());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/stats/{jobId:.+}")
    @ResponseBody
    public EnvelopeResponse<StreamingJobDataStatsResponse> getStreamingJobDataStats(
            @PathVariable(value = "jobId") String jobId,
            @RequestParam(value = "time_filter", required = false, defaultValue = "30") Integer timeFilter) {
        checkStreamingEnabled();
        val response = streamingJobService.getStreamingJobDataStats(jobId, timeFilter);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/records")
    @ResponseBody
    public EnvelopeResponse<List<StreamingJobRecord>> getStreamingJobRecordList(
            @RequestParam(value = "job_id") String jobId) {
        checkStreamingEnabled();
        if (StringUtils.isEmpty(jobId)) {
            throw new KylinException(EMPTY_JOB_ID, String.format(Locale.ROOT, "'%s' is required.", "job_id"));
        }
        val data = streamingJobService.getStreamingJobRecordList(jobId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }
}
