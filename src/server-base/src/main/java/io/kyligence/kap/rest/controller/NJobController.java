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
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.service.JobService;

@Controller
@RequestMapping(value = "/jobs")
public class NJobController extends NBasicController {

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
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        JobFilter jobFilter = new JobFilter(status, jobNames, timeFilter, subject, subjectAlias, project, sortBy,
                reverse);
        List<ExecutableResponse> executables = jobService.listJobs(jobFilter);
        Map<String, Object> result = getDataResponse("jobList", executables, pageOffset, pageSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropJob(@PathVariable("project") String project,
            @RequestParam(value = "jobIds", required = false) List<String> jobIds,
            @RequestParam(value = "status", required = false) String status) throws IOException {
        checkProjectName(project);
        if (CollectionUtils.isEmpty(jobIds) && StringUtils.isEmpty(status)) {
            throw new BadRequestException("At least one job should be selected to delete!");
        }
        jobService.dropJobBatchly(project, jobIds, status);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PutMapping(value = "/resume", produces = "application/vnd.apache.kylin-v2+json")
    public EnvelopeResponse resumeJob(@RequestParam(value = "project") String project,
            @RequestParam(value = "jobId") String jobId) {
        jobService.resumeJob(project, jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/status", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
        checkProjectName(jobUpdateRequest.getProject());
        if (CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())
                && StringUtils.isEmpty(jobUpdateRequest.getStatus())) {
            throw new BadRequestException("At least one job should be selected to " + jobUpdateRequest.getAction());
        }
        jobService.updateJobStatusBatchly(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                jobUpdateRequest.getAction(), jobUpdateRequest.getStatus());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/detail", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobDetail(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "jobId", required = true) String jobId) {
        checkProjectName(project);
        checkRequiredArg("jobId", jobId);
        List<ExecutableStepResponse> jobDetails = jobService.getJobDetail(project, jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobDetails, "");
    }

    @RequestMapping(value = "/statistics", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobStats(@RequestParam(value = "project") String project,
                                         @RequestParam(value = "start_time") long startTime,
                                        @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime), "");
    }

    @RequestMapping(value = "/statistics/count", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobCount(@RequestParam(value = "project") String project,
                                        @RequestParam(value = "start_time") long startTime,
                                        @RequestParam(value = "end_time") long endTime,
                                        @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @RequestMapping(value = "/statistics/duration_per_byte", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobDurationPerByte(@RequestParam(value = "project") String project,
                                                  @RequestParam(value = "start_time") long startTime,
                                                  @RequestParam(value = "end_time") long endTime,
                                                  @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobDurationPerByte(project, startTime, endTime, dimension), "");
    }
}