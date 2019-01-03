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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.cube.model.NDataLayout;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.SegmentUtils;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AfterBuildResourceMerger {

    private final KylinConfig config;
    private final String project;

    public AfterBuildResourceMerger(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    public NDataLayout[] mergeAfterIncrement(String flowName, String segmentId, Set<Long> layoutIds, ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(config, project);
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), project);
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dfUpdate = new NDataflowUpdate(flowName);
        val availableLayoutIds = intersectionWithLastSegment(localDataflow, layoutIds);
        val theSeg = remoteDataflow.getSegment(segmentId);
        theSeg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(theSeg);
        dfUpdate.setToAddOrUpdateCuboids(theSeg.getSegDetails().getLayouts().stream()
                .filter(c -> availableLayoutIds.contains(c.getLayoutId())).toArray(NDataLayout[]::new));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateCuboids();
    }

    public NDataLayout[] mergeAfterCatchup(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
                                           ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(config, project);
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), project);
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dataflow = localDataflowManager.getDataflow(flowName);
        val dfUpdate = new NDataflowUpdate(flowName);
        val addCuboids = Lists.<NDataLayout> newArrayList();

        val layoutInCubeIds = dataflow.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toList());
        val availableLayoutIds = layoutIds.stream().filter(layoutInCubeIds::contains).collect(Collectors.toSet());
        for (String segId : segmentIds) {
            val localSeg = localDataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId);
            // ignore if local segment is not ready
            if (localSeg == null || localSeg.getStatus() != SegmentStatusEnum.READY) {
                continue;
            }
            for (long layoutId : availableLayoutIds) {
                NDataLayout dataCuboid = remoteSeg.getLayout(layoutId);
                Preconditions.checkNotNull(dataCuboid);
                addCuboids.add(dataCuboid);
            }
            dfUpdate.setToUpdateSegs(remoteSeg);
        }
        dfUpdate.setToAddOrUpdateCuboids(addCuboids.toArray(new NDataLayout[0]));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateCuboids();
    }

    public void mergeAnalysis(String dataflowId, ResourceStore remoteStore) {
        val remoteConfig = remoteStore.getConfig();
        final NTableMetadataManager remoteTblMgr = NTableMetadataManager.getInstance(remoteConfig, project);
        final NTableMetadataManager localTblMgr = NTableMetadataManager.getInstance(config, project);

        final NDataModel dataModel = NDataflowManager.getInstance(config, project).getDataflow(dataflowId).getModel();

        mergeAndUpdateTableExt(localTblMgr, remoteTblMgr, dataModel.getRootFactTableName());
        for (final JoinTableDesc lookupDesc : dataModel.getJoinTables()) {
            mergeAndUpdateTableExt(localTblMgr, remoteTblMgr, lookupDesc.getTable());
        }

    }

    private Set<Long> intersectionWithLastSegment(NDataflow dataflow, Collection<Long> layoutIds) {
        val layoutInSegmentIds = SegmentUtils.getToBuildLayouts(dataflow).stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        return layoutIds.stream().filter(layoutInSegmentIds::contains).collect(Collectors.toSet());
    }

    private void mergeAndUpdateTableExt(NTableMetadataManager localTblMgr, NTableMetadataManager remoteTblMgr,
            String tableName) {
        val localFactTblExt = localTblMgr.getOrCreateTableExt(tableName);
        val remoteFactTblExt = remoteTblMgr.getOrCreateTableExt(tableName);

        localTblMgr.mergeAndUpdateTableExt(localFactTblExt, remoteFactTblExt);
    }
}
