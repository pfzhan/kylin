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

package io.kyligence.kap.job.handler;

import static org.apache.kylin.job.factory.JobFactoryConstant.CUBE_JOB_FACTORY;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class RefreshSegmentHandler extends AbstractJobHandler {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfm = NDataflowManager.getInstance(kylinConfig, jobParam.getProject());
        NDataflow dataflow = dfm.getDataflow(jobParam.getModel()).copy();

        if (jobParam.isMultiPartitionJob()) {
            // update partition status to refresh
            val segment = dataflow.getSegment(jobParam.getSegment());
            segment.getMultiPartitions().forEach(partition -> {
                if (jobParam.getTargetPartitions().contains(partition.getPartitionId())) {
                    partition.setStatus(PartitionStatusEnum.REFRESH);
                }
            });
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setToUpdateSegs(segment);
            dfm.updateDataflow(dfUpdate);
        }

        return JobFactory.createJob(CUBE_JOB_FACTORY,
                new JobFactory.JobBuildParams(Sets.newHashSet(dataflow.getSegment(jobParam.getSegment())),
                        jobParam.getProcessLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                        jobParam.getJobId(), null, jobParam.getIgnoredSnapshotTables(), jobParam.getTargetPartitions(),
                        jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }
}
