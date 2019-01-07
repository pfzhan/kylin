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
package io.kyligence.kap.event.handle;

import java.util.List;

import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkMergingStep;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostMergeOrRefreshSegmentHandler extends AbstractEventPostJobHandler {

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        val event = (PostMergeOrRefreshSegmentEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        val task = getBuildTask(executable);
        if (task == null) {
            log.warn("Executable " + executable.getId() + " has no build task.");
            return;
        }

        val dataflowId = ExecutableUtils.getDataflowId(task);
        val segmentIds = ExecutableUtils.getSegmentIds(task);
        val buildResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), task);
        try {
            if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
                finishEvent(project, event.getId());
                return;
            }

            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterMergeOrRefreshResourceMerger(kylinConfig, project);
            val updatedCuboids = merger.mergeAfterJob(dataflowId, segmentIds.iterator().next(), buildResourceStore);

            recordDownJobStats(task, updatedCuboids);

            notifyUserIfNecessary(executable, updatedCuboids);

            finishEvent(project, event.getId());
        } finally {
            buildResourceStore.close();
        }
    }

    private AbstractExecutable getBuildTask(ChainedExecutable executable) {
        Preconditions.checkState(executable.getTasks().size() > 0, "job " + executable.getId() + " steps is not enough");
        if (executable instanceof NSparkCubingJob) {
            return executable.getTask(NSparkCubingStep.class);
        } else if (executable instanceof NSparkMergingJob) {
            return executable.getTask(NSparkMergingStep.class);
        }
        return null;

    }

    protected void doHandleWithNullJob(EventContext eventContext) {
        val event = (PostMergeOrRefreshSegmentEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        String modelId = event.getModelId();
        String segmentId = event.getSegmentId();
        if (!checkSubjectExists(project, modelId, segmentId, event)) {
            finishEvent(project, event.getId());
            return;
        }

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow df = dfMgr.getDataflow(modelId);

        //update target seg's status
        val dfUpdate = new NDataflowUpdate(modelId);
        NDataflow copy = df.copy();
        val seg = copy.getSegment(segmentId);
        seg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(seg);
        List<NDataSegment> toRemoveSegs = dfMgr.getToRemoveSegs(df, seg);
        dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));

        dfMgr.updateDataflow(dfUpdate);

        finishEvent(project, event.getId());

    }

}
