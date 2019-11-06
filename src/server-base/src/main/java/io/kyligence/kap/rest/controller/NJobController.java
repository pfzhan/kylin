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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;

import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.request.SparkJobTimeRequest;
import io.kyligence.kap.rest.request.SparkJobUpdateRequest;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.service.JobService;

@Controller
@RequestMapping(value = "/jobs")
public class NJobController extends NBasicController {

    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobList(@RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "jobNames", required = false) List<String> jobNames,
            @RequestParam(value = "timeFilter", required = true) Integer timeFilter,
            @RequestParam(value = "subject", required = false) String subject,
            @RequestParam(value = "subjectAlias", required = false) String subjectAlias,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        if (Objects.nonNull(projectName)) {
            project = projectName;
        }
        checkJobStatus(status);
        JobFilter jobFilter = new JobFilter(status, jobNames, timeFilter, subject, subjectAlias, project, sortBy,
                reverse);
        List<ExecutableResponse> executables;
        if (!StringUtils.isEmpty(project)) {
            executables = jobService.listJobs(jobFilter);
            executables = jobService.addOldParams(executables);
        } else {
            executables = jobService.listGlobalJobs(jobFilter);
        }
        Map<String, Object> result = getDataResponse("jobList", executables, pageOffset, pageSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/waiting_jobs", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getWaitingJobs(@RequestParam(value = "project") String project,
            @RequestParam(value = "model") String modelId,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<EventResponse> waitingJobs = jobService.getWaitingJobsByModel(project, modelId);
        Map<String, Object> data = Maps.newHashMap();
        data.put("data", PagingUtil.cutPage(waitingJobs, offset, limit));
        data.put("size", waitingJobs.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/waiting_jobs/models", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getWaitingJobsInfoGroupByModel(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getEventsInfoGroupByModel(project), "");
    }

    @RequestMapping(value = "/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropJob(@PathVariable("project") String project,
            @RequestParam(value = "jobIds", required = false) List<String> jobIds,
            @RequestParam(value = "status", required = false) String status) throws IOException {
        checkProjectName(project);
        checkJobStatus(status);
        if (CollectionUtils.isEmpty(jobIds) && StringUtils.isEmpty(status)) {
            throw new BadRequestException("At least one job should be selected to delete!");
        }
        jobService.batchDropJob(project, jobIds, status);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropGlobalJob(@RequestParam(value = "jobIds", required = false) List<String> jobIds,
            @RequestParam(value = "status", required = false) String status) throws IOException {
        checkJobStatus(status);
        if (CollectionUtils.isEmpty(jobIds) && StringUtils.isEmpty(status)) {
            throw new BadRequestException("At least one job should be selected to delete!");
        }
        jobService.batchDropGlobalJob(jobIds, status);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/status", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
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
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/detail", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobDetail(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "jobId", required = true) String jobId) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        List<ExecutableStepResponse> jobDetails = jobService.getJobDetail(project, jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobDetails, "");
    }

    @RequestMapping(value = "/{jobId}/steps/{stepId}/output", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobOutput(@PathVariable("jobId") String jobId, @PathVariable("stepId") String stepId,
            @RequestParam(value = "project") String project) {
        Map<String, String> result = new HashMap<String, String>();
        result.put(JOB_ID_ARG_NAME, jobId);
        result.put(STEP_ID_ARG_NAME, stepId);
        result.put("cmd_output", jobService.getJobOutput(project, jobId, stepId));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/{jobId}/steps/{stepId}/log", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse downloadLogFile(@PathVariable("jobId") String jobId, @PathVariable("stepId") String stepId,
            @RequestParam(value = "project") String project, HttpServletResponse response) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        checkRequiredArg(STEP_ID_ARG_NAME, stepId);
        String downloadFilename = String.format("%s_%s.log", project, stepId);

        String hdfsLogpath = jobService.getHdfsLogPath(project, stepId);
        if (Objects.isNull(hdfsLogpath)) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, null, "can not find the log file!");
        }

        response.setHeader("content-type", "application/octet-stream");
        response.setContentType("application/octet-stream");
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                String.format("attachment; filename=\"%s\"", downloadFilename));

        jobService.downloadHdfsLogFile(response, hdfsLogpath);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/statistics", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobStats(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime), "");
    }

    @RequestMapping(value = "/statistics/count", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobCount(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @RequestMapping(value = "/statistics/duration_per_byte", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobDurationPerByte(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                jobService.getJobDurationPerByte(project, startTime, endTime, dimension), "");
    }

    @RequestMapping(value = "/spark", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateSparkJobInfo(@RequestBody SparkJobUpdateRequest sparkJobUpdateRequest)
            throws IOException {
        checkProjectName(sparkJobUpdateRequest.getProject());
        jobService.updateSparkJobInfo(sparkJobUpdateRequest.getProject(), sparkJobUpdateRequest.getJobId(),
                sparkJobUpdateRequest.getTaskId(), sparkJobUpdateRequest.getYarnAppId(),
                sparkJobUpdateRequest.getYarnAppUrl());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PutMapping(value = "/wait_and_run_time", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateSparkJobTime(@RequestBody SparkJobTimeRequest sparkJobTimeRequest)
            throws IOException {
        checkProjectName(sparkJobTimeRequest.getProject());
        jobService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getTaskId(), sparkJobTimeRequest.getYarnJobWaitTime(),
                sparkJobTimeRequest.getYarnJobRunTime());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }
}