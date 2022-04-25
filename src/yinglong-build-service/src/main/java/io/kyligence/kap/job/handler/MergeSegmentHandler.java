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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_FAIL;
import static org.apache.kylin.job.factory.JobFactoryConstant.MERGE_JOB_FACTORY;

import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class MergeSegmentHandler extends AbstractJobHandler {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        Set<NDataSegment> mergeSegment = new HashSet<>();
        mergeSegment.add(newSeg);
        return JobFactory.createJob(MERGE_JOB_FACTORY,
                new JobFactory.JobBuildParams(mergeSegment, jobParam.getProcessLayouts(), jobParam.getOwner(),
                        JobTypeEnum.INDEX_MERGE, jobParam.getJobId(), null, jobParam.getIgnoredSnapshotTables(),
                        jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }

    /**
     * Merge is abandoned when segment index is not aligned.
     */
    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        super.checkBeforeHandle(jobParam);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflow df = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        val segments = df.getSegments();
        NDataSegment newSegment = df.getSegment(jobParam.getSegment());
        Set<Long> layoutIds = null;
        for (val seg : segments) {
            if (seg.getId().equals(newSegment.getId())) {
                continue;
            }
            if (seg.getSegRange().overlaps(newSegment.getSegRange()) && null == layoutIds) {
                layoutIds = seg.getLayoutsMap().keySet();
            }
            if (seg.getSegRange().overlaps(newSegment.getSegRange())
                    && !seg.getLayoutsMap().keySet().equals(layoutIds)) {
                log.warn("Segment's layout is not matched,segID:{}, {} -> {}", seg.getId(), layoutIds,
                        seg.getLayoutsMap().keySet());
                throw new KylinException(JOB_CREATE_CHECK_SEGMENT_FAIL);
            }
        }
    }
}
