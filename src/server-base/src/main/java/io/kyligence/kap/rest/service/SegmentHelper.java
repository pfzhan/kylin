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
package io.kyligence.kap.rest.service;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SegmentHelper extends BasicService {

    public void refreshLoadingRange(String project, String tableName, SegmentRange toBeRefreshSegmentRange) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }

        NDataLoadingRange dataLoadingRange = NDataLoadingRangeManager.getInstance(kylinConfig, project)
                .getDataLoadingRange(tableName);
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredReadySegmentRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new IllegalArgumentException("ToBeRefreshSegmentRange " + toBeRefreshSegmentRange
                    + " is out of range the coveredReadySegmentRange of dataLoadingRange, the coveredReadySegmentRange is "
                    + coveredReadySegmentRange);
        }

        List<String> modelNames = NDataModelManager.getInstance(kylinConfig, project)
                .getTableOrientedModelsUsingRootTable(tableDesc);
        boolean first = true;
        List<SegmentRange> firstRanges = Lists.newArrayList();

        if (CollectionUtils.isNotEmpty(modelNames)) {

            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            for (String modelName : modelNames) {
                NCubePlan cubePlan = NCubePlanManager.getInstance(kylinConfig, project).findMatchingCubePlan(modelName
                );
                NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
                NDataflow df = dfMgr.getDataflow(cubePlan.getName());
                Segments<NDataSegment> segments = df.getSegments(SegmentStatusEnum.READY);
                List<SegmentRange> ranges = Lists.newArrayList();
                for (NDataSegment seg : segments) {
                    if (!toBeRefreshSegmentRange.contains(seg.getSegRange())) {
                        continue;
                    }
                    NDataSegment newSeg = dfMgr.refreshSegment(df, seg.getSegRange());

                    RefreshSegmentEvent refreshSegmentEvent = new RefreshSegmentEvent();
                    refreshSegmentEvent.setModelName(modelName);
                    refreshSegmentEvent.setCubePlanName(cubePlan.getName());
                    refreshSegmentEvent.setSegmentId(newSeg.getId());
                    refreshSegmentEvent.setJobId(UUID.randomUUID().toString());
                    refreshSegmentEvent.setOwner(getUsername());
                    eventManager.post(refreshSegmentEvent);

                    PostMergeOrRefreshSegmentEvent postE = new PostMergeOrRefreshSegmentEvent();
                    postE.setModelName(modelName);
                    postE.setCubePlanName(cubePlan.getName());
                    postE.setSegmentId(newSeg.getId());
                    postE.setJobId(refreshSegmentEvent.getJobId());
                    postE.setOwner(getUsername());
                    eventManager.post(postE);

                    ranges.add(seg.getSegRange());
                }

                if (first) {
                    firstRanges = ranges;
                    first = false;
                } else {
                    Preconditions.checkState(firstRanges.equals(ranges));
                }
            }
        }
    }

    public void removeSegment(String project, String dataflowName, Set<String> tobeRemoveSegmentIds) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(dataflowName);
        if (CollectionUtils.isEmpty(tobeRemoveSegmentIds)) {
            return;
        }

        List<NDataSegment> dataSegments = Lists.newArrayList();
        for (String tobeRemoveSegmentId : tobeRemoveSegmentIds) {
            NDataSegment dataSegment = df.getSegment(tobeRemoveSegmentId);
            if (dataSegment == null) {
                continue;
            }
            dataSegments.add(dataSegment);
        }

        if (CollectionUtils.isNotEmpty(dataSegments)) {
            NDataflowUpdate update = new NDataflowUpdate(df.getName());
            update.setToRemoveSegs(dataSegments.toArray(new NDataSegment[0]));
            dfMgr.updateDataflow(update);
        }

    }
}
