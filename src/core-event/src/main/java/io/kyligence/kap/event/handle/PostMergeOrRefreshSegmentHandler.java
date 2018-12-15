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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.ExecutableUtils;
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

        if (executable == null) {
            log.debug("executable is null when handling event {}", event);
            // in case the job is skipped
            doHandleWithNullJob(eventContext);
            return;
        } else if (executable.getStatus() == ExecutableState.DISCARDED) {
            log.debug("previous job suicide, current event:{} will be ignored", eventContext.getEvent());
            UnitOfWork.doInTransactionWithRetry(() -> {
                finishEvent(eventContext.getProject(), eventContext.getEvent().getId());
                return null;
            }, project);
            return;
        }

        val tasks = executable.getTasks();
        Preconditions.checkState(tasks.size() > 0, "job " + event.getJobId() + " steps is not enough");
        val task = tasks.get(0);
        val dataflowName = ExecutableUtils.getDataflowName(task);
        val segmentIds = ExecutableUtils.getSegmentIds(task);
        val buildResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), task);

        UnitOfWork.doInTransactionWithRetry(() -> {
            if (!checkSubjectExists(project, event.getCubePlanName(), event.getSegmentId(), event)) {
                finishEvent(project, event.getId());
                return null;
            }

            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterMergeOrRefreshResourceMerger(kylinConfig, project);
            merger.mergeAfterJob(dataflowName, segmentIds.iterator().next(), buildResourceStore);

            finishEvent(project, event.getId());
            return null;
        }, project);
    }

    private void doHandleWithNullJob(EventContext eventContext) {
        val event = (PostMergeOrRefreshSegmentEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        UnitOfWork.doInTransactionWithRetry(() -> {
            String cubePlanName = event.getCubePlanName();
            String segmentId = event.getSegmentId();
            if (!checkSubjectExists(project, cubePlanName, segmentId, event)) {
                finishEvent(project, event.getId());
                return null;
            }

            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dfMgr.getDataflow(cubePlanName);

            //update target seg's status
            val dfUpdate = new NDataflowUpdate(cubePlanName);
            NDataflow copy = df.copy();
            val seg = copy.getSegment(segmentId);
            seg.setStatus(SegmentStatusEnum.READY);
            dfUpdate.setToUpdateSegs(seg);
            List<NDataSegment> toRemoveSegs = dfMgr.getToRemoveSegs(df, seg);
            dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));

            dfMgr.updateDataflow(dfUpdate);

            finishEvent(project, event.getId());

            return null;
        }, project);
    }

}
