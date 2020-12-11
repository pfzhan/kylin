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

package io.kyligence.kap.engine.spark.job;

import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnTable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.spark_project.guava.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;

/**
 *
 */
public class NSparkSnapshotJob extends DefaultChainedExecutableOnTable {

    public static NSparkSnapshotJob create(TableDesc tableDesc, String project, String submitter, boolean isRefresh) {
        JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
        return create(tableDesc, project, submitter, jobType, UUID.randomUUID().toString());
    }

    private static NSparkSnapshotJob create(TableDesc tableDesc, String project, String submitter, JobTypeEnum jobType,
            String jobId) {
        Preconditions.checkArgument(submitter != null);
        NSparkSnapshotJob job = new NSparkSnapshotJob();

        job.setId(jobId);
        job.setProject(project);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setSubmitter(submitter);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_TABLE_NAME, tableDesc.getIdentity());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobStepType.BUILD_SNAPSHOT.createStep(job, config);

        return job;
    }

    public NSparkSnapshotBuildingStep getSnapshotBuildingStep() {
        return getTask(NSparkSnapshotBuildingStep.class);
    }
}