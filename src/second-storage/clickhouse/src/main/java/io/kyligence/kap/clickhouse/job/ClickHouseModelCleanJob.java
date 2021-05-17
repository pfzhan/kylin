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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;

public class ClickHouseModelCleanJob extends DefaultChainedExecutable {

    public ClickHouseModelCleanJob() {}

    public ClickHouseModelCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);
        setTargetSubject(builder.getModelId());
        setProject(builder.df.getProject());
        long startTime = 0L;
        long endTime = Long.MAX_VALUE - 1;
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());

        AbstractClickHouseClean step = new ClickHouseTableClean();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.init();
        addTask(step);
    }

    public static class ModelCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseCleanJobParam.builder()
                    .modelId(params.getModelId())
                    .jobId(params.getJobId())
                    .submitter(params.getSubmitter())
                    .df(dfManager.getDataflow(params.getDataflowId()))
                    .project(params.getProject())
                    .build();
            return new ClickHouseModelCleanJob(param);
        }
    }
}
