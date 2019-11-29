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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
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
@RequestMapping(value = "/api/jobs")
public class NJobController extends NBasicController {

    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @ApiOperation(value = "getJobList (update)", notes = "Update Param: job_names, time_filter, subject_alias, project_name, page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ExecutableResponse>>> getJobList(
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "job_names", required = false) List<String> jobNames,
            @RequestParam(value = "time_filter") Integer timeFilter,
            @RequestParam(value = "subject", required = false) String subject,
            @RequestParam(value = "subject_alias", required = false) String subjectAlias,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkJobStatus(status);
        JobFilter jobFilter = new JobFilter(status, jobNames, timeFilter, subject, subjectAlias, project, sortBy,
                reverse);
        DataResult<List<ExecutableResponse>> executables;
        if (!StringUtils.isEmpty(project)) {
            executables = jobService.listJobs(jobFilter, pageOffset, pageSize);
        } else {
            executables = jobService.listGlobalJobs(jobFilter, pageOffset, pageSize);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, executables, "");
    }

    @ApiOperation(value = "getWaitingJobs (update)", notes = "Update Response: total_size")
    @GetMapping(value = "/waiting_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<EventResponse>>> getWaitingJobs(
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<EventResponse> waitingJobs = jobService.getWaitingJobsByModel(project, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(waitingJobs, offset, limit), "");
    }

    @GetMapping(value = "/waiting_jobs/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getWaitingJobsInfoGroupByModel(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getEventsInfoGroupByModel(project), "");
    }

    @ApiOperation(value = "dropJob dropGlobalJob (merge)(update)", notes = "Update URL: {project}; Update Param: project, job_ids")
    @DeleteMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> dropJob(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "job_ids", required = false) List<String> jobIds,
            @RequestParam(value = "status", required = false) String status) throws IOException {
        checkJobStatus(status);
        if (CollectionUtils.isEmpty(jobIds) && StringUtils.isEmpty(status)) {
            throw new BadRequestException("At least one job should be selected to delete!");
        }

        if (null != project) {
            jobService.batchDropJob(project, jobIds, status);
        } else {
            jobService.batchDropGlobalJob(jobIds, status);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus (update)", notes = "Update Body: job_ids")
    @PutMapping(value = "/status", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
        checkJobStatus(jobUpdateRequest.getStatus());
        if (CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())
                && StringUtils.isEmpty(jobUpdateRequest.getStatus())) {
            throw new BadRequestException("At least one job should be selected to " + jobUpdateRequest.getAction());
        }

        if (!StringUtils.isEmpty(jobUpdateRequest.getProject())) {
            jobService.batchUpdateJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatus());
        } else {
            jobService.batchUpdateGlobalJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getAction(),
                    jobUpdateRequest.getStatus());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus (update)", notes = "Update Param: job_id")
    @GetMapping(value = "/{job_id:.+}/detail", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<List<ExecutableStepResponse>> getJobDetail(@PathVariable(value = "job_id") String jobId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getJobDetail(project, jobId), "");
    }

    @ApiOperation(value = "updateJobStatus (update)", notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/output", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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

    @ApiOperation(value = "downloadLogFile (update)", notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/log", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> downloadLogFile(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project,
            HttpServletResponse response) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        checkRequiredArg(STEP_ID_ARG_NAME, stepId);
        String downloadFilename = String.format("%s_%s.log", project, stepId);

        String hdfsLogpath = jobService.getHdfsLogPath(project, stepId);
        if (Objects.isNull(hdfsLogpath)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, "", "can not find the log file!");
        }

        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                String.format("attachment; filename=\"%s\"", downloadFilename));

        jobService.downloadHdfsLogFile(response, hdfsLogpath);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/statistics", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<JobStatisticsResponse> getJobStats(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime),
                "");
    }

    @GetMapping(value = "/statistics/count", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> getJobCount(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration_per_byte", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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
     * @param sparkJobUpdateRequest
     * @return
     */
    @PutMapping(value = "/spark", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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
     * @param sparkJobTimeRequest
     * @return
     */
    @PutMapping(value = "/wait_and_run_time", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobTime(@RequestBody SparkJobTimeRequest sparkJobTimeRequest) {
        checkProjectName(sparkJobTimeRequest.getProject());
        jobService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getTaskId(), sparkJobTimeRequest.getYarnJobWaitTime(),
                sparkJobTimeRequest.getYarnJobRunTime());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}