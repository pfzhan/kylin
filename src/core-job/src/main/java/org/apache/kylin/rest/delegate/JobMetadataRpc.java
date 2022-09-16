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

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.model.TableDesc;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "yinglong-data-loading-booter", path = "/kylin/api/job_delegate/feign")
public interface JobMetadataRpc extends JobMetadataContract {

    @PostMapping(value = "/add_index_job")
    String addIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/add_second_storage_job")
    String addSecondStorageJob(JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/add_segment_job")
    String addSegmentJob(JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/build_partition_job")
    String buildPartitionJob(@RequestBody JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/add_related_index_job")
    String addRelatedIndexJob(@RequestBody JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/add_job")
    String addJob(@RequestBody JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/merge_segment_job")
    String mergeSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest);

    @PostMapping(value = "/fetch_job_list")
    List<JobInfo> fetchJobList(@RequestBody JobMapperFilter jobMapperFilter);

    @PostMapping(value = "/refresh_segment_job")
    String refreshSegmentJob(@RequestBody JobMetadataRequest jobMetadataRequest,
                             @RequestParam("refreshAllLayouts") boolean refreshAllLayouts);

    @PostMapping(value = "/get_layouts_by_running_jobs")
    Set<Long> getLayoutsByRunningJobs(@RequestParam("project") String project, @RequestParam("modelId") String modelId);

    @PostMapping(value = "/count_by_model_and_status")
    long countByModelAndStatus(@RequestParam("project") String project, @RequestParam("model") String model,
            @RequestParam("status") String status, @RequestParam("jobTypes") JobTypeEnum... jobTypes);

    @PostMapping(value = "get_job_executables")
    List<ExecutablePO> getJobExecutablesPO(@RequestParam("project") String project);

    @PostMapping(value = "list_partial_exec")
    List<ExecutablePO> listPartialExec(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
                                       @RequestParam("state") String state, @RequestParam("jobTypes") JobTypeEnum... jobTypes);

    @PostMapping(value = "list_exec_by_job_type_and_status")
    List<ExecutablePO> listExecPOByJobTypeAndStatus(@RequestParam("project") String project,
            @RequestParam("state") String state, @RequestParam("jobTypes") JobTypeEnum... jobTypes);

    @PostMapping(value = "get_exec_by_status")
    List<ExecutablePO> getExecutablePOsByStatus(@RequestParam("project") String project,
            @RequestParam("status") ExecutableState... status);

    @PostMapping(value = "delete_job_by_ids")
    void deleteJobByIdList(@RequestParam("project") String project, @RequestParam("jobIdList") List<String> jobIdList);

    @PostMapping(value = "discard_job")
    void discardJob(@RequestParam("project") String project, @RequestParam("jobId") String jobId);

    @PostMapping(value = "stop_batch_job")
    void stopBatchJob(@RequestParam("project") String project, @RequestBody TableDesc tableDesc);

    @PostMapping(value = "clear_project_jobs")
    void clearJobsByProject(@RequestParam("project") String project);
}
