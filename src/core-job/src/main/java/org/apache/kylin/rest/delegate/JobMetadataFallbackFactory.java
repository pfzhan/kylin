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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.model.TableDesc;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;

@Component
public class JobMetadataFallbackFactory implements FallbackFactory<JobMetadataRpc> {

    @SneakyThrows
    @Override
    public JobMetadataRpc create(Throwable cause) {
        if (!KylinConfig.getInstanceFromEnv().isCommonOnlyMode()) {
            throw cause;
        }
        return new JobMetadataRpc() {
            @SneakyThrows
            @Override
            public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String addSecondStorageJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String buildPartitionJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String addRelatedIndexJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String addJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String mergeSegmentJob(JobMetadataRequest jobMetadataRequest) {
                throw cause;
            }

            @SneakyThrows
            @Override
            public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest, boolean refreshAllLayouts) {
                throw cause;
            }

            @Override
            public List<JobInfo> fetchJobList(JobMapperFilter jobMapperFilter) {
                return Collections.emptyList();
            }

            @Override
            public List<JobLock> fetchAllJobLock() {
                return Collections.emptyList();
            }

            @Override
            public List<JobInfo> fetchNotFinalJobsByTypes(String project, List<String> jobTypes,
                    List<String> subjects) {
                return Collections.emptyList();
            }

            @Override
            public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
                return Collections.emptySet();
            }

            @Override
            public long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes) {
                return 0;
            }

            @Override
            public List<ExecutablePO> getJobExecutablesPO(String project) {
                return Collections.emptyList();
            }

            @Override
            public List<ExecutablePO> listPartialExec(String project, String modelId, String state,
                    JobTypeEnum... jobTypes) {
                return Collections.emptyList();
            }

            @Override
            public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state,
                    JobTypeEnum... jobTypes) {
                return Collections.emptyList();
            }

            @Override
            public List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status) {
                return Collections.emptyList();
            }

            @Override
            public List<ExecutablePO> getExecutablePOsByFilter(JobMapperFilter filter) {
                return Collections.emptyList();
            }

            @Override
            public void restoreJobInfo(List<JobInfo> jobInfos, String project, boolean afterTruncate) {
                // do nothing
            }

            @Override
            public void deleteJobByIdList(String project, List<String> jobIdList) {
                // do nothing
            }

            @Override
            public void discardJob(String project, String jobId) {
                // do nothing
            }

            @Override
            public void stopBatchJob(String project, TableDesc tableDesc) {
                // do nothing
            }

            @Override
            public void clearJobsByProject(String project) {
                // do nothing
            }

            @Override
            public void checkSuicideJobOfModel(String project, String modelId) {
                // do nothing
            }
        };
    }
}
