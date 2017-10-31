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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecuteResult.State;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;

public class NSparkCubingUpdateAfterBuildStep extends AbstractExecutable {

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        NSparkCubingJob parent = (NSparkCubingJob) getParent();
        NSparkCubingStep cubingStep = parent.getSparkCubingStep();
        String dataflowName = cubingStep.getDataflowName();

        NDataflowManager mgr = NDataflowManager.getInstance(context.getConfig());
        NDataflow dataflow = mgr.getDataflow(dataflowName);
        NDataflowUpdate update = new NDataflowUpdate(dataflow);

        fillUpdateFromCubingStep(context.getConfig(), cubingStep, update);

        try {
            mgr.updateDataflow(update);
        } catch (IOException e) {
            throw new ExecuteException("failed to update " + dataflow, e);
        }

        return new ExecuteResult(State.SUCCEED);
    }

    private void fillUpdateFromCubingStep(KylinConfig config, NSparkCubingStep cubingStep, NDataflowUpdate update) {
        // the config from distributed metadata
        KylinConfig distConfig = KylinConfig.createKylinConfig(config);
        distConfig.setMetadataUrl(cubingStep.getDistMetaUrl());

        NDataflowManager distMgr = NDataflowManager.getInstance(distConfig);
        String dfName = cubingStep.getDataflowName();
        NDataflow distDataflow = distMgr.getDataflow(dfName);

        List<NDataSegment> toUpdateSegments = new ArrayList<>();
        List<NDataCuboid> toAddCuboids = new ArrayList<>();
        List<NDataSegment> toRemoveSegments = new ArrayList<>();

        for (int segId : cubingStep.getSegmentIds()) {
            NDataSegment seg = distDataflow.getSegment(segId);
            if (seg.getStatus() == SegmentStatusEnum.NEW)
                seg.setStatus(SegmentStatusEnum.READY);
            toUpdateSegments.add(seg);
            toRemoveSegments.addAll(getToRemoveSegs(distDataflow, seg));

            for (long layoutId : cubingStep.getCuboidLayoutIds()) {
                toAddCuboids.add(seg.getCuboid(layoutId));
            }
        }
        update.setToRemoveSegs((NDataSegment[]) toRemoveSegments.toArray(new NDataSegment[toRemoveSegments.size()]));
        update.setToUpdateSegs((NDataSegment[]) toUpdateSegments.toArray(new NDataSegment[toUpdateSegments.size()]));
        update.setToAddCuboids((NDataCuboid[]) toAddCuboids.toArray(new NDataCuboid[toAddCuboids.size()]));
        update.setStatus(RealizationStatusEnum.READY);
    }

    private List<NDataSegment> getToRemoveSegs(NDataflow dataflow, NDataSegment segment) {
        Segments tobe = dataflow.calculateToBeSegments(segment);

        if (!tobe.contains(segment))
            throw new IllegalStateException(
                    "For NDataflow " + dataflow + ", segment " + segment + " is expected but not in the tobe " + tobe);

        if (segment.getStatus() == SegmentStatusEnum.NEW)
            segment.setStatus(SegmentStatusEnum.READY);

        List<NDataSegment> toRemoveSegs = Lists.newArrayList();
        for (NDataSegment s : dataflow.getSegments()) {
            if (!tobe.contains(s))
                toRemoveSegs.add(s);
        }

        logger.info("Promoting NDataflow " + dataflow + ", new segment " + segment + ", to remove segments "
                + toRemoveSegs);

        return toRemoveSegs;
    }

}
