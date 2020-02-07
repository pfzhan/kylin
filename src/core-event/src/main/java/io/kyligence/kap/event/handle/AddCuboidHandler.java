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

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;

public class AddCuboidHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddCuboidHandler.class);

    @Override
    public AbstractExecutable createJob(Event e, String project) {
        AddCuboidEvent event = (AddCuboidEvent) e;
        String modelId = event.getModelId();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (!checkSubjectExists(project, modelId, null, event)) {
            return null;
        }

        NDataflow df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(modelId);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project).getIndexPlan(modelId);

        val readySegs = df.getSegments(SegmentStatusEnum.READY);
        if (readySegs.isEmpty()) {
            logger.info("event {} is no longer valid because no ready segment exists in target index_plan {}", event,
                    modelId);
            return null;
        }

        // be process layouts = all layouts - ready layouts
        val lastReadySeg = readySegs.getLatestReadySegment();
        Set<LayoutEntity> toBeDeletedLayouts = Sets.newHashSet();
        Set<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        for (LayoutEntity layout : indexPlan.getAllLayouts()) {
            if (layout.isToBeDeleted()) {
                toBeDeletedLayouts.add(layout);
                continue;
            }
            NDataLayout nc = lastReadySeg.getLayout(layout.getId());
            if (nc == null) {
                toBeProcessedLayouts.add(layout);
            }
        }

        if (CollectionUtils.isEmpty(toBeProcessedLayouts)) {
            logger.info("event {} is no longer valid because no layout awaits building", event);
            return null;
        }

        return NSparkCubingJob.create(Sets.newLinkedHashSet(readySegs), toBeProcessedLayouts, event.getOwner(),
                JobTypeEnum.INDEX_BUILD, event.getJobId(), toBeDeletedLayouts);
    }

    protected void doHandleWithNullJob(EventContext eventContext) {
        AddCuboidEvent event = (AddCuboidEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        if (!checkSubjectExists(project, event.getModelId(), null, event)) {
            rollFQBackToInitialStatus(eventContext, SUBJECT_NOT_EXIST_COMMENT);
            return;
        }

        handleFavoriteQuery(eventContext);
    }

}