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

package io.kyligence.kap.rest.cli;


import com.google.common.collect.Sets;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class RecoverModelUtil {


    public static void recoverDf(NDataflow df, KylinConfig config, String project) {
        val segments = df.getSegments();
        if (CollectionUtils.isEmpty(segments)) {
            return;
        }
        val newSegs = segments.getSegments(SegmentStatusEnum.NEW);

        val eventManager = EventManager.getInstance(config, project);

        if (CollectionUtils.isNotEmpty(newSegs)) {
            //recover inc_build segs
            for (val newSeg : newSegs) {
                AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
                addSegmentEvent.setSegmentId(newSeg.getId());
                addSegmentEvent.setModelId(df.getModel().getId());
                addSegmentEvent.setJobId(UUID.randomUUID().toString());
                addSegmentEvent.setOwner(df.getModel().getOwner());
                eventManager.post(addSegmentEvent);

                PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
                postAddSegmentEvent.setSegmentId(newSeg.getId());
                postAddSegmentEvent.setModelId(df.getModel().getId());
                postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
                postAddSegmentEvent.setOwner(df.getModel().getOwner());
                eventManager.post(postAddSegmentEvent);
            }
        }

        IndexPlan indexPlan = NIndexPlanManager.getInstance(config, project).getIndexPlan(df.getId());
        // be process layouts = all layouts - ready layouts
        val addEvent = new AddCuboidEvent();
        addEvent.setModelId(indexPlan.getUuid());
        addEvent.setOwner(df.getModel().getOwner());
        addEvent.setJobId(UUID.randomUUID().toString());
        eventManager.post(addEvent);

        val postAddEvent = new PostAddCuboidEvent();
        postAddEvent.setModelId(indexPlan.getUuid());
        postAddEvent.setJobId(addEvent.getJobId());
        postAddEvent.setOwner(df.getModel().getOwner());
        eventManager.post(postAddEvent);


        updateDfStatus(df, config, project);
    }

    public static void recoverBySelf(NDataflow df, KylinConfig config, String project) {
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val targetRanges = df.getFlatSegments().getSegRanges();
        reconstructDf(df, config, project, targetRanges);
        df = dataflowManager.getDataflow(df.getId());
        recoverDf(df, config, project);
    }

    private static void reconstructDf(NDataflow df, KylinConfig config, String project, List<SegmentRange> targetRanges) {
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val segs = df.getSegments();
        val toRemoveSegs = getSegsToRemove(segs, targetRanges);
        if (CollectionUtils.isNotEmpty(toRemoveSegs)) {
            val update = new NDataflowUpdate(df.getId());
            update.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));
            dataflowManager.updateDataflow(update);
        }
        val ranges = dataflowManager.getDataflow(df.getId()).getSegments().getSegRanges();

        //add seg in target,not in current dataflow
        for (val targetRange : targetRanges) {
            if (!ranges.contains(targetRange)) {
                df = dataflowManager.getDataflow(df.getId());
                dataflowManager.appendSegment(df, targetRange);
            }
        }
    }


    public static void recoverByDataloadingRange(NDataflow df, KylinConfig config, String project) {
        val dataloadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        val dataflowManager = NDataflowManager.getInstance(config, project);

        val dataloadingRange = dataloadingRangeManager.getDataLoadingRange(df.getModel().getRootFactTableName());

        val targetRanges = dataloadingRangeManager.getSegRangesToBuildForNewDataflow(dataloadingRange);

        reconstructDf(df, config, project, targetRanges);

        df = dataflowManager.getDataflow(df.getId());
        //metadata already completed, recover by self
        recoverDf(df, config, project);
    }

    private static Set<NDataSegment> getSegsToRemove(Segments<NDataSegment> segs, List<SegmentRange> targetRanges) {
        Set<NDataSegment> toRemoveSegs = Sets.newHashSet();
        val newSegs = segs.getSegments(SegmentStatusEnum.NEW);
        //related jobs sucide
        toRemoveSegs.addAll(newSegs);

        //if seg ranges not contained in target ranges, remove it
        for (val seg : segs) {
            if (!targetRanges.contains(seg.getSegRange())) {
                toRemoveSegs.add(seg);
            }
        }
        return toRemoveSegs;
    }

    private static void updateDfStatus(NDataflow df, KylinConfig config, String project) {

        val mdoelManager = NDataModelManager.getInstance(config, project);


        val dfManager = NDataflowManager.getInstance(config, project);

        val model = mdoelManager.getDataModelDesc(df.getId());
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
            dfManager.updateDataflow(df.getId(), copyForWrite -> {
                copyForWrite.setStatus(RealizationStatusEnum.ONLINE);
                copyForWrite.setEventError(false);
            });
        } else {
            if (CollectionUtils.isNotEmpty(df.getSegments(SegmentStatusEnum.NEW))) {
                dfManager.updateDataflow(df.getId(), copyForWrite -> {
                    copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
                    copyForWrite.setEventError(false);
                });
            } else {
                //only index build,set it online
                dfManager.updateDataflow(df.getId(), copyForWrite -> {
                    copyForWrite.setStatus(RealizationStatusEnum.ONLINE);
                    copyForWrite.setEventError(false);
                });
            }
        }
    }
}
