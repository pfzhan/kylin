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

package io.kyligence.kap.job.execution;

import static io.kyligence.kap.engine.spark.stats.utils.HiveTableRefChecker.isNeedCleanUpTransactionalTableJob;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.sparkproject.guava.base.Preconditions;

import io.kyligence.kap.job.execution.step.JobStepType;
import io.kyligence.kap.job.execution.step.NSparkSnapshotBuildingStep;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.SneakyThrows;

/**
 *
 */
public class NSparkSnapshotJob extends DefaultChainedExecutableOnTable {
    public NSparkSnapshotJob() {
        super();
    }

    public NSparkSnapshotJob(Object notSetId) {
        super(notSetId);
    }

    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, String partitionCol,
            boolean incrementBuild, Set<String> partitionToBuild, boolean isRefresh, String yarnQueue, Object tag) {
        JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
        return create(tableDesc, submitter, jobType, RandomUtil.randomUUIDStr(), partitionCol, incrementBuild,
                partitionToBuild, yarnQueue, tag);
    }

    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, boolean isRefresh, String yarnQueue) {
        JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
        return create(tableDesc, submitter, jobType, RandomUtil.randomUUIDStr(), null, false, null, yarnQueue, null);
    }

    @SneakyThrows
    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, JobTypeEnum jobType, String jobId,
            String partitionCol, boolean incrementalBuild, Set<String> partitionToBuild, String yarnQueue, Object tag) {
        Preconditions.checkArgument(submitter != null);
        NSparkSnapshotJob job = new NSparkSnapshotJob();
        String project = tableDesc.getProject();
        job.setId(jobId);
        job.setProject(project);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setSubmitter(submitter);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_TABLE_NAME, tableDesc.getIdentity());

        job.setParam(NBatchConstants.P_INCREMENTAL_BUILD, incrementalBuild + "");
        job.setParam(NBatchConstants.P_SELECTED_PARTITION_COL, partitionCol);
        if (partitionToBuild != null) {
            job.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE, JsonUtil.writeValueAsString(partitionToBuild));
        }

        job.setSparkYarnQueueIfEnabled(project, yarnQueue);
        job.setTag(tag);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobStepType.BUILD_SNAPSHOT.createStep(job, config);
        if(isNeedCleanUpTransactionalTableJob(tableDesc.isTransactional(), tableDesc.isRangePartition(),
                config.isReadTransactionalTableEnabled())) {
            JobStepType.CLEAN_UP_TRANSACTIONAL_TABLE.createStep(job, config);
        }
        return job;
    }

    public NSparkSnapshotBuildingStep getSnapshotBuildingStep() {
        return getTask(NSparkSnapshotBuildingStep.class);
    }

}
