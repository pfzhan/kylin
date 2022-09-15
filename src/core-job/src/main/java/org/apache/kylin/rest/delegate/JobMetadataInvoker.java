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
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class JobMetadataInvoker extends JobMetadataBaseInvoker {

    private static JobMetadataContract delegate = null;

    public static void setDelegate(JobMetadataContract delegate) {
        if (JobMetadataInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, JobMetadataInvoker.delegate);
        }
        JobMetadataInvoker.delegate = delegate;
    }

    public JobMetadataContract getDelegate() {
        if (delegate == null) {
            return SpringContext.getBean(JobMetadataContract.class);
        }
        return delegate;
    }

    public static JobMetadataInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new JobMetadataInvoker();
        } else {
            return SpringContext.getBean(JobMetadataInvoker.class);
        }
    }

    @Override
    public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().addIndexJob(jobMetadataRequest);
    }

    public String addSecondStorageJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().addSecondStorageJob(jobMetadataRequest);
    }

    @Override
    public String buildPartitionJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().buildPartitionJob(jobMetadataRequest);
    }

    @Override
    public String addRelatedIndexJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().addRelatedIndexJob(jobMetadataRequest);
    }

    @Override
    public String mergeSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().mergeSegmentJob(jobMetadataRequest);
    }

    @Override
    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().refreshSegmentJob(jobMetadataRequest, false);
    }

    @Override
    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest, boolean refreshAllLayouts) {
        return getDelegate().refreshSegmentJob(jobMetadataRequest, refreshAllLayouts);
    }

    @Override
    public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return getDelegate().addSegmentJob(jobMetadataRequest);
    }

    @Override
    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        return getDelegate().getLayoutsByRunningJobs(project, modelId);
    }

    @Override
    public long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes) {
        return getDelegate().countByModelAndStatus(project, model, status, jobTypes);
    }

    @Override
    public List<ExecutablePO> getJobExecutablesPO(String project) {
        return getDelegate().getJobExecutablesPO(project);
    }

    @Override
    public List<ExecutablePO> listPartialExec(String project, String modelId, String state, JobTypeEnum... jobTypes) {
        return getDelegate().listPartialExec(project, modelId, state, jobTypes);
    }

    @Override
    public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes) {
        return getDelegate().listExecPOByJobTypeAndStatus(project, state, jobTypes);
    }

    public List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status) {
        return getDelegate().getExecutablePOsByStatus(project, status);
    }

    @Override
    public void discardJob(String project, String jobId) {
        getDelegate().discardJob(project, jobId);
    }

    @Override
    public void deleteJobByIdList(String project, List<String> jobIds) {
        getDelegate().deleteJobByIdList(project, jobIds);
    }

    @Override
    public void clearJobsByProject(String project){
        getDelegate().clearJobsByProject(project);
    }

    public void stopBatchJob(String project, TableDesc tableDesc) {
        getDelegate().stopBatchJob(project, tableDesc);
    }

}
