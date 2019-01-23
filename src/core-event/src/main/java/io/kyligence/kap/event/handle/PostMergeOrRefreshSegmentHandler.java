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

import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class PostMergeOrRefreshSegmentHandler extends AbstractEventPostJobHandler {
    private static final Logger logger = LoggerFactory.getLogger(PostAddSegmentHandler.class);

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        val event = (PostMergeOrRefreshSegmentEvent) eventContext.getEvent();
        String project = eventContext.getProject();
        if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
            finishEvent(project, event.getId());
            return;
        }
        try {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterMergeOrRefreshResourceMerger(kylinConfig, project);
            executable.getTasks().stream()
                    .filter(task -> task instanceof NSparkExecutable)
                    .filter(task -> ((NSparkExecutable)task).needMergeMetadata())
                    .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));


            finishEvent(project, event.getId());
        }  catch (Throwable throwable) {
            logger.error("Process event " + event.toString() + " failed:", throwable);
            throw throwable;
        }
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
