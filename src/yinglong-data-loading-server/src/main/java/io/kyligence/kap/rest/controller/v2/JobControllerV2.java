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

package io.kyligence.kap.rest.controller.v2;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.job.rest.ExecutableResponse;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.service.JobInfoService;
import io.kyligence.kap.rest.controller.BaseController;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class JobControllerV2 extends BaseController {

    private static final String JOB_ID_ARG_NAME = "jobId";

    @Autowired
    private JobInfoService jobInfoService;

    @Autowired
    public AclEvaluate aclEvaluate;

    @ApiOperation(value = "resume", tags = { "DW" })
    @PutMapping(value = "/{jobId}/resume")
    @ResponseBody
    public EnvelopeResponse<ExecutableResponse> resume(@PathVariable(value = "jobId") String jobId) throws IOException {
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        final ExecutableResponse jobInstance = jobInfoService.getJobInstance(jobId);
        aclEvaluate.checkProjectOperationPermission(jobInstance.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobInfoService.manageJob(jobInstance.getProject(), jobInstance, JobActionEnum.RESUME.toString()), "");
    }

    @ApiOperation(value = "getJobList", tags = { "DW" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse getJobList(
            @RequestParam(value = "status", required = false, defaultValue = "") Integer[] status,
            @RequestParam(value = "timeFilter") Integer timeFilter,
            @RequestParam(value = "jobName", required = false) String jobName,
            @RequestParam(value = "projectName") String project,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        List<String> statuses = Lists.newArrayList();
        for (Integer code : status) {
            JobStatusEnum jobStatus = JobStatusEnum.getByCode(code);
            if (Objects.isNull(jobStatus)) {
                jobInfoService.checkJobStatus(String.valueOf(code));
                continue;
            }
            statuses.add(jobStatus.toString());
        }

        JobFilter jobFilter = new JobFilter(statuses,
                Objects.isNull(jobName) ? Lists.newArrayList() : Lists.newArrayList(jobName), timeFilter, null, key,
                project, sortBy, reverse);
        List<ExecutableResponse> executables = jobInfoService.listJobs(jobFilter);
        executables = jobInfoService.addOldParams(executables);
        Map<String, Object> result = getDataResponse("jobs", executables, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }
}
