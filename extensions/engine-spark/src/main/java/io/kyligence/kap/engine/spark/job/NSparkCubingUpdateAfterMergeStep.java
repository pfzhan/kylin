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

import java.io.IOException;
import java.util.List;

import io.kyligence.kap.job.execution.NExecutableManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;

public class NSparkCubingUpdateAfterMergeStep extends AbstractExecutable {
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        NSparkMergingJob parent = (NSparkMergingJob) getParent();
        NSparkMergingStep mergeStep = parent.getSparkCubingStep();
        String dataflowName = mergeStep.getDataflowName();

        NDataflowManager mgr = NDataflowManager.getInstance(context.getConfig(), getProject());
        NDataflowUpdate update = new NDataflowUpdate(dataflowName);

        fillUpdateFromMergingStep(context.getConfig(), mergeStep, update);

        try {
            mgr.updateDataflow(update);
        } catch (IOException e) {
            throw new ExecuteException("failed to update NDataflow " + dataflowName, e);
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }

    private void fillUpdateFromMergingStep(KylinConfig config, NSparkMergingStep mergingStep, NDataflowUpdate update) {
        // the config from distributed metadata
        KylinConfig distConfig = KylinConfig.createKylinConfig(config);
        distConfig.setMetadataUrl(mergingStep.getDistMetaUrl());

        NDataflowManager distMgr = NDataflowManager.getInstance(distConfig, getProject());
        String dfName = mergingStep.getDataflowName();
        NDataflow distDataflow = distMgr.getDataflow(dfName).copy(); // avoid changing cached objects

        List<NDataSegment> toUpdateSegments = Lists.newArrayList();
        List<NDataSegment> toRemoveSegments = Lists.newArrayList();
        List<NDataCuboid> toUpdateCuboids = Lists.newArrayList();

        NDataSegment mergedSegment = distDataflow.getSegment(mergingStep.getSegmentIds());
        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateCuboids.addAll(mergedSegment.getSegDetails().getCuboids());

        toUpdateSegments.add(mergedSegment);
        toRemoveSegments.addAll(distDataflow.getMergingSegments(mergedSegment));

        update.setToRemoveSegs((NDataSegment[]) toRemoveSegments.toArray(new NDataSegment[toRemoveSegments.size()]));
        update.setToUpdateSegs((NDataSegment[]) toUpdateSegments.toArray(new NDataSegment[toUpdateSegments.size()]));
        update.setToAddOrUpdateCuboids(
                (NDataCuboid[]) toUpdateCuboids.toArray(new NDataCuboid[toUpdateCuboids.size()]));
        update.setStatus(RealizationStatusEnum.READY);
    }


    @Override
    protected ExecutableManager getManager() {
        return NExecutableManager.getInstance(getConfig(), getProject());
    }
}
