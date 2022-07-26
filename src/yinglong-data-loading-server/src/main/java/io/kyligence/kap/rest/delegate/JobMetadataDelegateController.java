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

package io.kyligence.kap.rest.delegate;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.job.delegate.JobMetadataDelegate;

@Controller
@RequestMapping(value = "/api/job_delegate", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobMetadataDelegateController {

    @Autowired
    private JobMetadataDelegate jobMetadataDelegate;

    @PostMapping(value = "/feign/add_index_job")
    @ResponseBody
    public String addIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addIndexJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_second_storage_job")
    @ResponseBody
    public String addSecondStorageJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSecondStorageJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_segment_job")
    @ResponseBody
    public String addSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSegmentJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/get_layouts_by_running_jobs")
    @ResponseBody
    public Set<Long> getLayoutsByRunningJobs(@RequestParam("project") String project,
            @RequestParam("modelId") String modelId) {
        return jobMetadataDelegate.getLayoutsByRunningJobs(project, modelId);
    }

    @PostMapping(value = "/feign/count_by_model_and_status")
    @ResponseBody
    public long countByModelAndStatus(@RequestParam("project") String project, @RequestParam("model") String model,
            @RequestParam("status") String status,
            @RequestParam(value = "jobTypes", required = false) JobTypeEnum... jobTypes) {
        return jobMetadataDelegate.countByModelAndStatus(project, model, status, jobTypes);
    }

    @PostMapping(value = "/feign/get_job_executables")
    @ResponseBody
    public List<ExecutablePO> getJobExecutablesPO(@RequestParam("project") String project) {
        return jobMetadataDelegate.getJobExecutablesPO(project);
    }

    @PostMapping(value = "/feign/list_exec_by_job_type_and_status")
    @ResponseBody
    public List<ExecutablePO> listExecPOByJobTypeAndStatus(@RequestParam("project") String project,
            @RequestParam("state") String state, @RequestParam("jobTypes") JobTypeEnum... jobTypes) {
        return jobMetadataDelegate.listExecPOByJobTypeAndStatus(project, state, jobTypes);
    }

    @PostMapping(value = "/feign/get_exec_by_status")
    @ResponseBody
    public List<ExecutablePO> getExecutablePOsByStatus(@RequestParam("project") String project,
            @RequestParam("status") ExecutableState... status) {
        return jobMetadataDelegate.getExecutablePOsByStatus(project, status);
    }

    @PostMapping(value = "/feign/discard_job")
    @ResponseBody
    public void discardJob(@RequestParam("project") String project, @RequestParam("jobId") String jobId) {
        jobMetadataDelegate.discardJob(project, jobId);
    }

    @PostMapping(value = "/feign/delete_job_by_ids")
    @ResponseBody
    public void deleteJobByIdList(@RequestParam("project") String project, @RequestParam("jobIdList") List<String> jobIdList) {
        jobMetadataDelegate.deleteJobByIdList(project, jobIdList);
    }

    @PostMapping(value = "/feign/stop_batch_job")
    @ResponseBody
    public void stopBatchJob(@RequestParam("project") String project, @RequestBody TableDesc tableDesc) {
        jobMetadataDelegate.stopBatchJob(project, tableDesc);
    }


    @PostMapping(value = "/feign/clear_project_jobs")
    @ResponseBody
    public void clearJobsByProject(@RequestParam("project") String project){
        jobMetadataDelegate.clearJobsByProject(project);
    }

}
