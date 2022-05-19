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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ID_EMPTY;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.common.persistence.transaction.UpdateJobStatusEventNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.job.rest.ExecutableResponse;
import io.kyligence.kap.job.rest.ExecutableStepResponse;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.service.JobInfoService;
import io.kyligence.kap.rest.request.JobErrorRequest;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.request.SparkJobTimeRequest;
import io.kyligence.kap.rest.request.SparkJobUpdateRequest;
import io.kyligence.kap.rest.request.StageRequest;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import io.kyligence.kap.rest.service.JobService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger("schedule");
    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    private JobInfoService jobInfoService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "getJobList", tags = {
            "DW" }, notes = "Update Param: job_names, time_filter, subject_alias, project_name, page_offset, page_size, sort_by; Update Response: total_size")
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
        jobInfoService.checkJobStatus(statuses);
        checkRequiredArg("time_filter", timeFilter);
        JobFilter jobFilter = new JobFilter(statuses, jobNames, timeFilter, subject, key, project, sortBy, reverse);
        DataResult<List<ExecutableResponse>> executables = jobInfoService.listJobs(jobFilter, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, executables, "");
    }

    @ApiOperation(value = "dropJob dropGlobalJob", tags = {
            "DW" }, notes = "Update URL: {project}; Update Param: project, job_ids")
    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> dropJob(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "job_ids", required = false) List<String> jobIds,
            @RequestParam(value = "statuses", required = false) List<String> statuses) throws IOException {
        jobInfoService.checkJobStatus(statuses);
        if (StringUtils.isBlank(project) && CollectionUtils.isEmpty(jobIds)) {
            throw new KylinException(JOB_ID_EMPTY, "delete");
        }

        if (null != project) {
            jobInfoService.batchDropJob(project, jobIds, statuses);
        } else {
            jobInfoService.batchDropGlobalJob(jobIds, statuses);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update Body: job_ids")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest,
            @RequestHeader HttpHeaders headers) throws IOException {
        checkRequiredArg("action", jobUpdateRequest.getAction());
        jobInfoService.checkJobStatusAndAction(jobUpdateRequest);
        Map<String, List<String>> nodeWithJobs = splitJobIdsByScheduleInstance(jobUpdateRequest.getJobIds());
        if (needRouteToOtherInstance(nodeWithJobs, jobUpdateRequest.getAction(), headers)) {
            return remoteUpdateJobStatus(jobUpdateRequest, headers, nodeWithJobs);
        }
        if (StringUtils.isBlank(jobUpdateRequest.getProject())
                && CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())) {
            throw new KylinException(JOB_ID_EMPTY, jobUpdateRequest.getAction());
        }

        if (!StringUtils.isEmpty(jobUpdateRequest.getProject())) {
            jobInfoService.batchUpdateJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses());
        } else {
            jobInfoService.batchUpdateGlobalJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getAction(),
                    jobUpdateRequest.getStatuses());
            EventBusFactory.getInstance().postAsync(new UpdateJobStatusEventNotifier(jobUpdateRequest.getJobIds(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses()));
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private EnvelopeResponse<String> remoteUpdateJobStatus(JobUpdateRequest jobUpdateRequest, HttpHeaders headers,
            Map<String, List<String>> nodeWithJobs) throws IOException {
        for (Map.Entry<String, List<String>> entry : nodeWithJobs.entrySet()) {
            jobUpdateRequest.setJobIds(entry.getValue());
            forwardRequestToTargetNode(JsonUtil.writeValueAsBytes(jobUpdateRequest), headers, entry.getKey(),
                    "/jobs/status");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getJobDetail", tags = { "DW" }, notes = "Update Param: job_id")
    @GetMapping(value = "/{job_id:.+}/detail")
    @ResponseBody
    public EnvelopeResponse<List<ExecutableStepResponse>> getJobDetail(@PathVariable(value = "job_id") String jobId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobInfoService.getJobDetail(project, jobId), "");
    }

    @ApiOperation(value = "updateJobStatus", tags = {
            "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/output")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getJobOutput(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        Map<String, String> result = new HashMap<>();
        result.put(JOB_ID_ARG_NAME, jobId);
        result.put(STEP_ID_ARG_NAME, stepId);
        result.put("cmd_output", jobInfoService.getJobOutput(project, jobId, stepId));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "downloadLogFile", tags = {
            "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/log")
    @ResponseBody
    public EnvelopeResponse<String> downloadLogFile(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project,
            HttpServletResponse response) {
        final String projectName = checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        checkRequiredArg(STEP_ID_ARG_NAME, stepId);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s.log", projectName, stepId);
        InputStream jobOutput = jobService.getAllJobOutput(projectName, jobId, stepId);
        setDownloadResponse(jobOutput, downloadFilename, MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/statistics")
    @ApiOperation(value = "jobStatistics", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<JobStatisticsResponse> getJobStats(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime),
                "");
    }

    @GetMapping(value = "/statistics/count")
    @ApiOperation(value = "jobStatisticsCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> getJobCount(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration_per_byte")
    @ApiOperation(value = "jobDurationCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Double>> getJobDurationPerByte(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobService.getJobDurationPerByte(project, startTime, endTime, dimension), "");
    }

    /**
     * RPC Call
     *
     * @param request
     * @return
     */
    @ApiOperation(value = "updateJobError", tags = { "DW" }, notes = "Update Body: job error")
    @PutMapping(value = "error")
    @ResponseBody
    public EnvelopeResponse<String> updateJobError(@RequestBody JobErrorRequest request) {
        if (StringUtils.isBlank(request.getProject()) && StringUtils.isBlank(request.getJobId())) {
            throw new KylinException(JOB_ID_EMPTY, "At least one job should be selected to update stage status");
        }
        checkProjectName(request.getProject());
        logger.info("updateJobError errorRequest is : {}", request);

        jobInfoService.updateJobError(request.getProject(), request.getJobId(), request.getFailedStepId(),
                request.getFailedSegmentId(), request.getFailedStack(), request.getFailedReason());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     *
     * @param stageRequest
     * @return
     */
    @ApiOperation(value = "updateStageStatus", tags = { "DW" }, notes = "Update Body: jobIds(stage ids)")
    @PutMapping(value = "/stage/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStageStatus(@RequestBody StageRequest stageRequest) {
        if (StringUtils.isBlank(stageRequest.getProject()) && StringUtils.isBlank(stageRequest.getTaskId())) {
            throw new KylinException(JOB_ID_EMPTY, "At least one job should be selected to update stage status");
        }
        checkProjectName(stageRequest.getProject());
        logger.info("updateStageStatus stageRequest is : {}", stageRequest);
        jobInfoService.updateStageStatus(stageRequest.getProject(), stageRequest.getTaskId(), stageRequest.getSegmentId(),
                stageRequest.getStatus(), stageRequest.getUpdateInfo(), stageRequest.getErrMsg());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        jobInfoService.updateSparkJobInfo(sparkJobUpdateRequest.getProject(), sparkJobUpdateRequest.getJobId(),
                sparkJobUpdateRequest.getTaskId(), sparkJobUpdateRequest.getYarnAppId(),
                sparkJobUpdateRequest.getYarnAppUrl());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        jobInfoService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getTaskId(), sparkJobTimeRequest.getYarnJobWaitTime(),
                sparkJobTimeRequest.getYarnJobRunTime());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @Deprecated
    @ApiOperation(value = "getWaitingJobs", tags = { "DW" }, notes = "Update Response: total_size")
    @GetMapping(value = "/waiting_jobs")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<EventResponse>>> getWaitingJobs(
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(null, offset, limit), "");
    }

    @Deprecated
    @ApiOperation(value = "waitingJobsByModel", tags = { "DW" })
    @GetMapping(value = "/waiting_jobs/models")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getWaitingJobsInfoGroupByModel(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobService.getEventsInfoGroupByModel(project), "");
    }
}
