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

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
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

    @PostMapping(value = "/get_layouts_by_running_jobs")
    Set<Long> getLayoutsByRunningJobs(@RequestParam("project") String project, @RequestParam("modelId") String modelId);

    @PostMapping(value = "/count_by_model_and_status")
    long countByModelAndStatus(@RequestParam("project") String project, @RequestParam("model") String model,
            @RequestParam("status") String status, @RequestParam("jobTypes") JobTypeEnum... jobTypes);

    @PostMapping(value = "get_job_executables")
    List<ExecutablePO> getJobExecutablesPO(@RequestParam("project") String project);

    @PostMapping(value = "list_exec_by_job_type_and_status")
    List<ExecutablePO> listExecPOByJobTypeAndStatus(@RequestParam("project") String project,
            @RequestParam("state") String state, @RequestParam("jobTypes") JobTypeEnum... jobTypes);

    @PostMapping(value = "get_exec_by_status")
    List<ExecutablePO> getExecutablePOsByStatus(@RequestParam("project") String project,
            @RequestParam("status") ExecutableState status);

    @PostMapping(value = "discard_job")
    void discardJob(@RequestParam("project") String project, @RequestParam("jobId") String jobId);

    @PostMapping(value = "stop_batch_job")
    void stopBatchJob(@RequestParam("project") String project, @RequestBody TableDesc tableDesc);

    @PostMapping(value = "clear_project_jobs")
    void clearJobsByProject(@RequestParam("project") String project);
}
