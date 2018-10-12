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

import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.service.JobService;

import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/jobs")
public class NJobController extends NBasicController {

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @RequestMapping(value = "", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getJobList(@RequestParam(value = "status", required = false) Integer[] status,
            @RequestParam(value = "jobName", required = false) String jobType,
            @RequestParam(value = "timeFilter", required = true) Integer timeFilter,
            @RequestParam(value = "subjects", required = false) String[] subjects,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        List<ExecutableResponse> executables = jobService.listJobs(project, status,
                JobTimeFilterEnum.getByCode(timeFilter), subjects, jobType, sortBy, reverse);
        Map<String, Object> result = getDataResponse("jobList", executables, pageOffset, pageSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }


    @RequestMapping(value = "/{project}/{jobId}", method = {RequestMethod.DELETE}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse dropJob(@PathVariable("project") String project, @PathVariable("jobId") String jobId) throws IOException {
        checkProjectName(project);
        jobService.dropJob(project, jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/status", method = {RequestMethod.PUT}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
        checkProjectName(jobUpdateRequest.getProject());
        jobService.updateJobStatus(jobUpdateRequest.getJobId(), jobUpdateRequest.getProject(), jobUpdateRequest.getAction());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/detail", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getJobDetail(@RequestParam(value = "project", required = true) String project,
                                         @RequestParam(value = "jobId", required = true) String jobId) {
        checkProjectName(project);
        checkRequiredArg("jobId", jobId);
        List<ExecutableStepResponse> jobDetails = jobService.getJobDetail(project, jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobDetails, "");
    }
}