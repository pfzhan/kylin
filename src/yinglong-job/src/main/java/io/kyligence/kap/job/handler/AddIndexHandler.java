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

import java.util.HashSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.collect.Sets;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class AddIndexHandler extends AbstractJobHandler {

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String modelId = jobParam.getModel();
        String project = jobParam.getProject();
        NDataflow df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(modelId);

        var readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        val targetSegments = new HashSet<>(jobParam.getTargetSegments());
        final Segments<NDataSegment> toDealSeg = new Segments<>();
        readySegs.stream().filter(segment -> targetSegments.contains(segment.getId() + "")).forEach(toDealSeg::add);
        readySegs = toDealSeg;

        if (CollectionUtils.isEmpty(jobParam.getProcessLayouts())
                && CollectionUtils.isEmpty(jobParam.getDeleteLayouts())) {
            log.info("Event {} is no longer valid because no layout awaits process", jobParam);
            return null;
        }
        if (readySegs.isEmpty()) {
            throw new IllegalArgumentException("No segment is ready in this job.");
        }
        return JobFactory.createJob(CUBE_JOB_FACTORY,
                new JobFactory.JobBuildParams(Sets.newLinkedHashSet(readySegs), jobParam.getProcessLayouts(),
                        jobParam.getOwner(), jobParam.getJobTypeEnum(), jobParam.getJobId(),
                        jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                        jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }

}
