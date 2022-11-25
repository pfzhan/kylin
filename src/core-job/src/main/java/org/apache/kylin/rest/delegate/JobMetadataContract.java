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
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.model.TableDesc;

public interface JobMetadataContract {

    String addIndexJob(JobMetadataRequest jobMetadataRequest);

    String addSecondStorageJob(JobMetadataRequest jobMetadataRequest);

    String addSegmentJob(JobMetadataRequest jobMetadataRequest);

    String buildPartitionJob(JobMetadataRequest jobMetadataRequest);

    String addRelatedIndexJob(JobMetadataRequest jobMetadataRequest);

    String addJob(JobMetadataRequest jobMetadataRequest);

    String mergeSegmentJob(JobMetadataRequest jobMetadataRequest);

    List<JobInfo> fetchJobList(JobMapperFilter filter);

    List<JobLock> fetchAllJobLock();

    List<JobInfo> fetchNotFinalJobsByTypes(String project, List<String> jobTypes, List<String> subjects);

    String refreshSegmentJob(JobMetadataRequest jobMetadataRequest, boolean refreshAllLayouts);

    Set<Long> getLayoutsByRunningJobs(String project, String modelId);

    long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes);

    List<ExecutablePO> getJobExecutablesPO(String project);

    List<ExecutablePO> listPartialExec(String project, String modelId, String state, JobTypeEnum... jobTypes);

    List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes);

    List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status);

    List<ExecutablePO> getExecutablePOsByFilter(JobMapperFilter filter);

    void deleteJobByIdList(String project, List<String> jobIdList);

    void discardJob(String project, String jobId);

    void stopBatchJob(String project, TableDesc tableDesc);

    void clearJobsByProject(String project);

    void checkSuicideJobOfModel(String project, String modelId);
}
