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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.rest.util.SpringContext;

import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;

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

    public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addSegmentJob(jobMetadataRequest);
    }

    public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
        return jobMetadataBaseDelegate.addIndexJob(jobMetadataRequest);
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

    public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes) {
        return jobMetadataBaseDelegate.listExecPOByJobTypeAndStatus(project, state, jobTypes);
    }

    public void discardJob(String project, String jobId) {
        jobMetadataBaseDelegate.discardJob(project, jobId);
    }
}
