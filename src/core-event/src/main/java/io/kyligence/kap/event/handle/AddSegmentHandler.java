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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;


public class AddSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddSegmentHandler.class);

    @Override
    protected void onJobSuccess(EventContext eventContext) throws Exception {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        String cubePlanName = event.getCubePlanName();
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(cubePlanName);
        updateDataLoadingRange(df);
        autoMergeSegments(df, project, event.getModelName());
    }

    private void autoMergeSegments(NDataflow df, String project, String modelName)
            throws IOException, PersistentException {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(modelName);
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);

        Segments segments = df.getSegments();
        SegmentRange rangeToMerge = null;
        EventManager eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
            rangeToMerge = segments.autoMergeSegments(model.isAutoMergeEnabled(), model.getName(), model.getAutoMergeTimeRanges(),
                    model.getVolatileRange());
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
            Event mergeEvent = new MergeSegmentEvent();
            mergeEvent.setApproved(true);
            mergeEvent.setCubePlanName(df.getCubePlanName());
            mergeEvent.setModelName(model.getName());
            mergeEvent.setProject(project);
            mergeEvent.setSegmentRange(rangeToMerge);
            eventManager.post(mergeEvent);
        }
        
    }

    @Override
    public AbstractExecutable createJob(EventContext eventContext) throws Exception {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        EventManager eventManager = EventManager.getInstance(kylinConfig, project);

        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        // repost event
        if (df.isReconstructing()) {
            val newEvent = new AddSegmentEvent();
            newEvent.setModelName(event.getModelName());
            newEvent.setProject(event.getProject());
            newEvent.setApproved(event.isApproved());
            newEvent.setCubePlanName(event.getCubePlanName());
            newEvent.setSegmentIds(event.getSegmentIds());
            newEvent.setAddedInfo(event.getAddedInfo());
            eventManager.post(newEvent);
            return null;
        }
        AbstractExecutable job;
        List<NCuboidLayout> layouts = new ArrayList<>();
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            if (CollectionUtils.isNotEmpty(df.getCubePlan().getAllCuboids())) {
                layouts.addAll(df.getCubePlan().getAllCuboidLayouts());
            }
        } else {
            for (Map.Entry<Long, NDataCuboid> cuboid : readySegments.getLatestReadySegment().getCuboidsMap().entrySet()) {
                if (cuboid.getValue().getStatus() == SegmentStatusEnum.READY) {
                    layouts.add(cuboid.getValue().getCuboidLayout());
                }
            }
        }
        if (CollectionUtils.isEmpty(layouts)) {
            return null;
        }

        List<Integer> segmentIds = event.getSegmentIds();
        Set<NDataSegment> segments = Sets.newHashSet();

        if (CollectionUtils.isEmpty(segmentIds)) {
            return null;
        }
        NDataSegment dataSegment;
        for (Integer segmentId : segmentIds) {
            dataSegment = df.getSegment(segmentId);
            if (dataSegment != null) {
                segments.add(dataSegment);
            }
        }

        job = NSparkCubingJob.create(segments,
                Sets.<NCuboidLayout> newLinkedHashSet(layouts), "ADMIN");

        return job;
    }

    @Override
    public Class<?> getEventClassType() {
        return AddSegmentEvent.class;
    }
}
