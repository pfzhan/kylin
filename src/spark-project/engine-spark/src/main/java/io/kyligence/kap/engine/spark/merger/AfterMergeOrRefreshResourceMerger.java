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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.cube.model.NDataCuboid;
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

    public void mergeAfterJob(String dataflowName, String segmentId, ResourceStore remoteResourceStore) {
        NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        NDataflowUpdate update = new NDataflowUpdate(dataflowName);

        NDataflowManager distMgr = NDataflowManager.getInstance(remoteResourceStore.getConfig(), project);
        NDataflow distDataflow = distMgr.getDataflow(update.getDataflowName()).copy(); // avoid changing cached objects

        List<NDataSegment> toUpdateSegments = Lists.newArrayList();
        List<NDataCuboid> toUpdateCuboids = Lists.newArrayList();

        NDataSegment mergedSegment = distDataflow.getSegment(segmentId);

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateSegments.add(mergedSegment);

        // only add layouts which still in segments, others maybe deleted by user
        List<NDataSegment> toRemoveSegments = distMgr.getToRemoveSegs(distDataflow, mergedSegment);
        val livedLayouts = toRemoveSegments.get(toRemoveSegments.size() - 1).getCuboidsMap().values().stream()
                .map(NDataCuboid::getCuboidLayoutId).collect(Collectors.toSet());
        toUpdateCuboids.addAll(mergedSegment.getSegDetails().getCuboids().stream()
                .filter(c -> livedLayouts.contains(c.getCuboidLayoutId())).collect(Collectors.toList()));

        update.setToAddOrUpdateCuboids(toUpdateCuboids.toArray(new NDataCuboid[0]));
        update.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));

        mgr.updateDataflow(update);
    }

}
