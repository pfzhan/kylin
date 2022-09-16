/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.delegate;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.delegate.JobMetadataDelegate;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/api/job_delegate", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobMetadataDelegateController {

    @Autowired
    private JobMetadataDelegate jobMetadataDelegate;

    @PostMapping(value = "/feign/add_index_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String addIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addIndexJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_second_storage_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String addSecondStorageJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSecondStorageJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_segment_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String addSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addSegmentJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/build_partition_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String buildPartitionJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.buildPartitionJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_related_index_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String addRelatedIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addRelatedIndexJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/add_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String addJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.addJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/fetch_job_list")
    @ResponseBody
    public List<JobInfo> fetchJobList(@RequestBody JobMapperFilter jobMapperFilter) {
        return jobMetadataDelegate.fetchJobList(jobMapperFilter);
    }


    @PostMapping(value = "/feign/merge_segment_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String mergeSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest) {
        return jobMetadataDelegate.mergeSegmentJob(jobMetadataRequest);
    }

    @PostMapping(value = "/feign/refresh_segment_job")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public String refreshSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest,
                                    @RequestParam("refreshAllLayouts") boolean refreshAllLayouts) {
        return jobMetadataDelegate.refreshSegmentJob(jobMetadataRequest, refreshAllLayouts);
    }

    @PostMapping(value = "/feign/get_layouts_by_running_jobs")
    @ResponseBody
    @WaitForSyncBeforeRPC
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

    @PostMapping(value = "/feign/list_partial_exec")
    @ResponseBody
    public List<ExecutablePO> listPartialExec(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
                                              @RequestParam("state") String state,
                                              @RequestParam("jobTypes") JobTypeEnum... jobTypes) {
        return jobMetadataDelegate.listPartialExec(project, modelId, state, jobTypes);
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
