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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.rest.util.SpringContext;

public class JobMetadataBaseInvoker {

    public static JobMetadataBaseInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null || getJobMetadataContract() == null) {
            // for UT
            return new JobMetadataBaseInvoker();
        } else {
            return SpringContext.getBean(JobMetadataBaseInvoker.class);
        }
    }

    private static JobMetadataContract getJobMetadataContract() {
        try {
            return SpringContext.getBean(JobMetadataContract.class);
        } catch (Exception e) {
            return null;
        }
    }

    private JobMetadataBaseDelegate jobMetadataBaseDelegate = new JobMetadataBaseDelegate();

    public String buildPartitionJob(JobMetadataRequest jobMetadataRequest) {
       return jobMetadataBaseDelegate.buildPartitionJob(jobMetadataRequest);
    }

    public String addRelatedIndexJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addRelatedIndexJob(jobMetadataRequest);
    }

    public String mergeSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.mergeSegmentJob(jobMetadataRequest);
    }

    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.refreshSegmentJob(jobMetadataRequest, false);
    }

    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest, boolean refreshAllLayouts) {
        return jobMetadataBaseDelegate.refreshSegmentJob(jobMetadataRequest, refreshAllLayouts);
    }

    public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addSegmentJob(jobMetadataRequest);
    }

    public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addIndexJob(jobMetadataRequest);
    }

    public String addJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addJob(jobMetadataRequest);
    }

    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        return jobMetadataBaseDelegate.getLayoutsByRunningJobs(project, modelId);
    }

    public long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes) {
        return jobMetadataBaseDelegate.countByModelAndStatus(project, model, status, jobTypes);
    }

    public List<ExecutablePO> getJobExecutablesPO(String project) {
        return jobMetadataBaseDelegate.getJobExecutablesPO(project);
    }

    public List<ExecutablePO> listPartialExec(String project, String modelId, String state,
                                              JobTypeEnum... jobTypes) {
        return jobMetadataBaseDelegate.listPartialExec(project, modelId, state, jobTypes);
    }

    public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes) {
        return jobMetadataBaseDelegate.listExecPOByJobTypeAndStatus(project, state, jobTypes);
    }

    public List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status) {
        return jobMetadataBaseDelegate.getExecutablePOsByStatus(project, status);
    }

    public List<ExecutablePO> getExecutablePOsByFilter(JobMapperFilter filter) {
        return jobMetadataBaseDelegate.getExecutablePOsByFilter(filter);
    }

    public List<JobInfo> fetchJobList(JobMapperFilter filter) {
        return jobMetadataBaseDelegate.fetchJobList(filter);
    }

    public List<JobLock> fetchAllJobLock() {
        return jobMetadataBaseDelegate.fetchAllJobLock();
    }

    public List<JobInfo> fetchNotFinalJobsByTypes(String project, List<String> jobNames, List<String> subjects) {
        return jobMetadataBaseDelegate.fetchNotFinalJobsByTypes(project, jobNames, subjects);
    }

    public void restoreJobInfo(List<JobInfo> jobInfos, String project, boolean afterTruncate) {
        jobMetadataBaseDelegate.restoreJobInfo(jobInfos, project, afterTruncate);
    }

    public void deleteJobByIdList(String project, List<String> jobIds) {
        jobMetadataBaseDelegate.deleteJobByIdList(project, jobIds);
    }

    public void clearJobsByProject(String project){
        jobMetadataBaseDelegate.clearJobsByProject(project);
    }

    public void discardJob(String project, String jobId) {
        jobMetadataBaseDelegate.discardJob(project, jobId);
    }

    public void checkSuicideJobOfModel(String project, String modelId) {
        jobMetadataBaseDelegate.checkSuicideJobOfModel(project, modelId);
    }
}
