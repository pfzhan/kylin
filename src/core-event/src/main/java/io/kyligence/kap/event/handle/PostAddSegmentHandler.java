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

import java.io.IOException;
import java.util.UUID;

import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.event.model.PostMergeSegmentEvent;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostAddSegmentHandler extends AbstractEventPostJobHandler {

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        PostAddSegmentEvent event = (PostAddSegmentEvent) eventContext.getEvent();
        String project = event.getProject();
        val id = event.getId();
        val jobId = event.getJobId();

        val tasks = executable.getTasks();
        Preconditions.checkState(tasks.size() > 1, "job " + jobId + " steps is not enough");
        val buildTask = tasks.get(1);
        val dataflowName = ExecutableUtils.getDataflowName(buildTask);
        val segmentIds = ExecutableUtils.getSegmentIds(buildTask);
        val layoutIds = ExecutableUtils.getLayoutIds(buildTask);

        val analysisResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), tasks.get(0));
        val buildResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), buildTask);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            String cubePlanName = event.getCubePlanName();

            val merger = new AfterBuildResourceMerger(kylinConfig, project);
            merger.mergeAfterIncrement(dataflowName, segmentIds.iterator().next(), layoutIds, buildResourceStore);
            merger.mergeAnalysis(dataflowName, analysisResourceStore);

            NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
            NDataflow df = dfMgr.getDataflow(cubePlanName);
            updateDataLoadingRange(df);

            //TODO: take care of this
            autoMergeSegments(df, project, event.getModelName(), event.getOwner());

            EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
            eventDao.deleteEvent(id);

            return null;
        }, project);
    }

    private void updateDataLoadingRange(NDataflow df) throws IOException {
        NDataModel model = df.getModel();
        String tableName = model.getRootFactTableName();
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(df.getConfig(),
                df.getProject());
        dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);
    }

    private void autoMergeSegments(NDataflow df, String project, String modelName, String owner) {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(modelName);
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);

        Segments segments = df.getSegments();
        SegmentRange rangeToMerge = null;
        EventManager eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
            rangeToMerge = segments.autoMergeSegments(model.isAutoMergeEnabled(), model.getName(),
                    model.getAutoMergeTimeRanges(), model.getVolatileRange());
        } else if (model.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager
                    .getDataLoadingRange(model.getRootFactTableName());
            if (dataLoadingRange == null) {
                return;
            }
            rangeToMerge = segments.autoMergeSegments(dataLoadingRange.isAutoMergeEnabled(), model.getName(),
                    dataLoadingRange.getAutoMergeTimeRanges(), dataLoadingRange.getVolatileRange());
        }
        if (rangeToMerge == null) {
            return;
        } else {
            val mergeEvent = new MergeSegmentEvent();
            mergeEvent.setCubePlanName(df.getCubePlanName());
            mergeEvent.setModelName(model.getName());
            mergeEvent.setProject(project);
            mergeEvent.setSegmentRange(rangeToMerge);
            mergeEvent.setJobId(UUID.randomUUID().toString());
            mergeEvent.setOwner(owner);
            eventManager.post(mergeEvent);

            val postMergeEvent = new PostMergeSegmentEvent();
            postMergeEvent.setCubePlanName(df.getCubePlanName());
            postMergeEvent.setModelName(model.getName());
            postMergeEvent.setProject(project);
            postMergeEvent.setSegmentRange(rangeToMerge);
            postMergeEvent.setJobId(mergeEvent.getJobId());
            postMergeEvent.setOwner(owner);
            eventManager.post(mergeEvent);
        }

    }

}