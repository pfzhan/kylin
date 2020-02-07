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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class RefreshSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(RefreshSegmentHandler.class);

    @Override
    public AbstractExecutable createJob(Event e, String project) {
        RefreshSegmentEvent event = (RefreshSegmentEvent) e;

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
            return null;
        }

        List<LayoutEntity> layouts = new ArrayList<>();
        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(event.getModelId());
        for (Map.Entry<Long, NDataLayout> cuboid : dataflow.getSegments().getLatestReadySegment().getLayoutsMap()
                .entrySet()) {
            layouts.add(cuboid.getValue().getLayout());
        }
        if (CollectionUtils.isEmpty(layouts)) {
            return null;
        }

        return NSparkCubingJob.create(Sets.newHashSet(dataflow.getSegment(event.getSegmentId())),
                Sets.newLinkedHashSet(layouts), event.getOwner(), JobTypeEnum.INDEX_REFRESH, event.getJobId());
    }

    protected void doHandleWithNullJob(EventContext eventContext) {
        val event = (RefreshSegmentEvent) eventContext.getEvent();
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
