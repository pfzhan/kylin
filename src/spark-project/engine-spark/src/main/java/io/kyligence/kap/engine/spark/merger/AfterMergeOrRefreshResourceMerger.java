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

package io.kyligence.kap.engine.spark.merger;

import java.util.List;
import java.util.stream.Collectors;

import io.kyligence.kap.cube.model.NDataLayout;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import lombok.val;

public class AfterMergeOrRefreshResourceMerger {

    private final KylinConfig config;
    private final String project;

    public AfterMergeOrRefreshResourceMerger(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    public NDataLayout[] mergeAfterJob(String dataflowId, String segmentId, ResourceStore remoteResourceStore) {
        NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);

        NDataflowManager distMgr = NDataflowManager.getInstance(remoteResourceStore.getConfig(), project);
        NDataflow distDataflow = distMgr.getDataflow(update.getDataflowId()).copy(); // avoid changing cached objects

        List<NDataSegment> toUpdateSegments = Lists.newArrayList();
        List<NDataLayout> toUpdateCuboids = Lists.newArrayList();

        NDataSegment mergedSegment = distDataflow.getSegment(segmentId);

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateSegments.add(mergedSegment);

        // only add layouts which still in segments, others maybe deleted by user
        List<NDataSegment> toRemoveSegments = distMgr.getToRemoveSegs(distDataflow, mergedSegment);
        val livedLayouts = mgr.getDataflow(dataflowId).getLatestReadySegment().getLayoutsMap().values().stream()
                .map(NDataLayout::getLayoutId).collect(Collectors.toSet());
        toUpdateCuboids.addAll(mergedSegment.getSegDetails().getLayouts().stream()
                .filter(c -> livedLayouts.contains(c.getLayoutId())).collect(Collectors.toList()));

        update.setToAddOrUpdateCuboids(toUpdateCuboids.toArray(new NDataLayout[0]));
        update.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));

        mgr.updateDataflow(update);

        return update.getToAddOrUpdateCuboids();
    }

}
