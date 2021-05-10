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
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_JOB_ID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.request.SparkJobTimeRequest;
import io.kyligence.kap.rest.request.SparkJobUpdateRequest;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import io.kyligence.kap.rest.service.JobService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NJobController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger("schedule");
    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "getJobList", tags = { "DW" }, notes = "Update Param: job_names, time_filter, subject_alias, project_name, page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ExecutableResponse>>> getJobList(
            @RequestParam(value = "statuses", required = false, defaultValue = "") List<String> statuses,
            @RequestParam(value = "job_names", required = false) List<String> jobNames,
            @RequestParam(value = "time_filter") Integer timeFilter,
            @RequestParam(value = "subject", required = false) String subject,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse) {
        jobService.checkJobStatus(statuses);
        checkRequiredArg("time_filter", timeFilter);
        JobFilter jobFilter = new JobFilter(statuses, jobNames, timeFilter, subject, key, project, sortBy, reverse);
        DataResult<List<ExecutableResponse>> executables;
        if (!StringUtils.isEmpty(project)) {
            executables = jobService.listJobs(jobFilter, pageOffset, pageSize);
        } else {
            executables = jobService.listGlobalJobs(jobFilter, pageOffset, pageSize);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, executables, "");
    }

    @ApiOperation(value = "getWaitingJobs", tags = { "DW" }, notes = "Update Response: total_size")
    @GetMapping(value = "/waiting_jobs")
    @ResponseBody
    @Deprecated
    public EnvelopeResponse<DataResult<List<EventResponse>>> getWaitingJobs(
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(null, offset, limit), "");
    }

    @Deprecated
    @ApiOperation(value = "waitingJobsByModel", tags = { "DW" })
    @GetMapping(value = "/waiting_jobs/models")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getWaitingJobsInfoGroupByModel(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getEventsInfoGroupByModel(project), "");
    }

    @ApiOperation(value = "dropJob dropGlobalJob", tags = { "DW" }, notes = "Update URL: {project}; Update Param: project, job_ids")
    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> dropJob(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "job_ids", required = false) List<String> jobIds,
            @RequestParam(value = "statuses", required = false) List<String> statuses) throws IOException {
        jobService.checkJobStatus(statuses);
        if (StringUtils.isBlank(project) && CollectionUtils.isEmpty(jobIds)) {
            throw new KylinException(EMPTY_JOB_ID, "At least one job should be selected to delete!");
        }

        if (null != project) {
            jobService.batchDropJob(project, jobIds, statuses);
        } else {
            jobService.batchDropGlobalJob(jobIds, statuses);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update Body: job_ids")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
        checkRequiredArg("action", jobUpdateRequest.getAction());
        jobService.checkJobStatusAndAction(jobUpdateRequest);
        if (StringUtils.isBlank(jobUpdateRequest.getProject())
                && CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())) {
            throw new KylinException(EMPTY_JOB_ID,
                    "At least one job should be selected to " + jobUpdateRequest.getAction());
        }

        if (!StringUtils.isEmpty(jobUpdateRequest.getProject())) {
            jobService.batchUpdateJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses());
        } else {
            jobService.batchUpdateGlobalJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getAction(),
                    jobUpdateRequest.getStatuses());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update Param: job_id")
    @GetMapping(value = "/{job_id:.+}/detail")
    @ResponseBody
    public EnvelopeResponse<List<ExecutableStepResponse>> getJobDetail(@PathVariable(value = "job_id") String jobId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getJobDetail(project, jobId), "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/output")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getJobOutput(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        Map<String, String> result = new HashMap<>();
        result.put(JOB_ID_ARG_NAME, jobId);
        result.put(STEP_ID_ARG_NAME, stepId);
        result.put("cmd_output", jobService.getJobOutput(project, jobId, stepId));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "downloadLogFile", tags = { "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/log")
    @ResponseBody
    public EnvelopeResponse<String> downloadLogFile(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project,
            HttpServletResponse response) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        checkRequiredArg(STEP_ID_ARG_NAME, stepId);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s.log", project, stepId);

        String jobOutput = jobService.getAllJobOutput(project, jobId, stepId);
        setDownloadResponse(new ByteArrayInputStream(jobOutput.getBytes(Charset.defaultCharset())), downloadFilename,
                MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/statistics")
    @ApiOperation(value = "jobStatistics", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<JobStatisticsResponse> getJobStats(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime),
                "");
    }

    @GetMapping(value = "/statistics/count")
    @ApiOperation(value = "jobStatisticsCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> getJobCount(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration_per_byte")
    @ApiOperation(value = "jobDurationCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Double>> getJobDurationPerByte(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                jobService.getJobDurationPerByte(project, startTime, endTime, dimension), "");
    }

    /**
     * RPC Call
     *
     * @param sparkJobUpdateRequest
     * @return
     */
    @PutMapping(value = "/spark")
    @ApiOperation(value = "updateURL", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobInfo(@RequestBody SparkJobUpdateRequest sparkJobUpdateRequest) {
        checkProjectName(sparkJobUpdateRequest.getProject());
        jobService.updateSparkJobInfo(sparkJobUpdateRequest.getProject(), sparkJobUpdateRequest.getJobId(),
                sparkJobUpdateRequest.getTaskId(), sparkJobUpdateRequest.getYarnAppId(),
                sparkJobUpdateRequest.getYarnAppUrl());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     *
     * @param sparkJobTimeRequest
     * @return
     */
    @PutMapping(value = "/wait_and_run_time")
    @ApiOperation(value = "updateWaitTime", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobTime(@RequestBody SparkJobTimeRequest sparkJobTimeRequest) {
        checkProjectName(sparkJobTimeRequest.getProject());
        jobService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getTaskId(), sparkJobTimeRequest.getYarnJobWaitTime(),
                sparkJobTimeRequest.getYarnJobRunTime());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}