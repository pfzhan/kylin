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

import java.util.Collections;
import java.util.Set;

import org.apache.kylin.job.ProjectJob;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.JobTypeEnum;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.val;

public class ClickHouseProjectCleanJob extends DefaultChainedExecutable implements ProjectJob {

    public ClickHouseProjectCleanJob() {
    }

    public ClickHouseProjectCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseProjectCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        setProject(builder.getProject());
        long startTime = 0L;
        long endTime = Long.MAX_VALUE - 1;
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);

        AbstractClickHouseClean step = new ClickHouseDatabaseClean();
        step.setProject(getProject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.init();
        addTask(step);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Collections.emptySet();
    }

    public static class ProjectCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val param = ClickHouseCleanJobParam.builder()
                    .jobId(params.getJobId())
                    .submitter(params.getSubmitter())
                    .project(params.getProject())
                    .build();
            return new ClickHouseProjectCleanJob(param);
        }
    }
}
