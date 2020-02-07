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
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.SegmentUtils;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NSegmentConfigHelper;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class AddSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddSegmentHandler.class);

    @Override
    public AbstractExecutable createJob(Event e, String project) {
        AddSegmentEvent event = (AddSegmentEvent) e;

        AbstractExecutable job;

        if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
            return null;
        }

        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataflow(event.getModelId());
        Set<LayoutEntity> layouts = SegmentUtils.getToBuildLayouts(df);
        if (CollectionUtils.isEmpty(layouts)) {
            logger.info("event {} is no longer valid because no layout awaits building", event);
            return null;
        }

        job = NSparkCubingJob.create(Sets.newHashSet(df.getSegment(event.getSegmentId())),
                Sets.newLinkedHashSet(layouts), event.getOwner(), JobTypeEnum.INC_BUILD, event.getJobId());

        return job;
    }

    @Override
    protected void doHandleWithNullJob(EventContext eventContext) {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
            rollFQBackToInitialStatus(eventContext, SUBJECT_NOT_EXIST_COMMENT);
            finishEvent(project, event.getId());
            return;
        }

        makeSegmentReady(project, event);

        handleFavoriteQuery(eventContext);
        finishEvent(project, event.getId());
    }

    private void makeSegmentReady(String project, AddSegmentEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val segmentId = event.getSegmentId();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(event.getModelId());

        //update target seg's status
        val dfUpdate = new NDataflowUpdate(event.getModelId());
        val seg = df.copy().getSegment(segmentId);
        seg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(seg);
        dfMgr.updateDataflow(dfUpdate);

        markDFOnlineIfNecessary(dfMgr.getDataflow(df.getId()));

        //TODO: take care of this
        handleRetention(project, event.getModelId());
        autoMergeSegments(project, event.getModelId(), event.getOwner());
    }

    private void autoMergeSegments(String project, String modelId, String owner) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        Segments segments = df.getSegments();
        SegmentRange rangeToMerge = null;
        EventManager eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        Preconditions.checkState(segmentConfig != null);
        rangeToMerge = segments.autoMergeSegments(segmentConfig);
        if (rangeToMerge == null) {
            return;
        } else {
            NDataSegment mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .mergeSegments(df, rangeToMerge, true);

            val mergeEvent = new MergeSegmentEvent();
            mergeEvent.setModelId(modelId);
            mergeEvent.setSegmentId(mergeSeg.getId());
            mergeEvent.setJobId(UUID.randomUUID().toString());
            mergeEvent.setOwner(owner);
            eventManager.post(mergeEvent);
        }

    }

    private void markDFOnlineIfNecessary(NDataflow dataflow) {
        if (!RealizationStatusEnum.LAG_BEHIND.equals(dataflow.getStatus())) {
            return;
        }
        val model = dataflow.getModel();
        Preconditions.checkState(ManagementType.TABLE_ORIENTED.equals(model.getManagementType()));

        if (checkOnline(model)) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            dfManager.updateDataflow(dataflow.getId(),
                    copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.ONLINE));
        }
    }

    private boolean checkOnline(NDataModel model) {
        // 1. check the job status of the model
        val executableManager = getExecutableManager(model.getProject(), KylinConfig.getInstanceFromEnv());
        val count = executableManager.countByModelAndStatus(model.getId(), ExecutableState::isNotProgressing,
                JobTypeEnum.INC_BUILD);
        if (count > 0) {
            return false;
        }
        // 2. check the model aligned with data loading range
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val df = dfManager.getDataflow(model.getId());
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(model.getRootFactTableName());
        // In theory, dataLoadingRange can not be null, because full load table related model will build with INDEX_BUILD job or INDEX_REFRESH job.
        Preconditions.checkState(dataLoadingRange != null);
        val querableSegmentRange = dataLoadingRangeManager.getQuerableSegmentRange(dataLoadingRange);
        Preconditions.checkState(querableSegmentRange != null);
        val segments = df.getSegments().getSegmentsByRange(querableSegmentRange)
                .getSegmentsExcludeRefreshingAndMerging();
        for (NDataSegment segment : segments) {
            if (SegmentStatusEnum.NEW.equals(segment.getStatus())) {
                return false;
            }
        }
        return true;
    }

    private void handleRetention(String project, String modelId) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        dfManager.handleRetention(df);
    }
}
