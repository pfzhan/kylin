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

package io.kyligence.kap.metadata.cube.storage;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.CuboidLayoutQueryTimes;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import lombok.val;
import lombok.var;

public class GarbageStorageCollector implements StorageInfoCollector {

    @Override
    public void collect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) {
        config = NProjectManager.getInstance(config).getProject(project).getConfig();
        int queryTimesThreshold = config.getQueryTimesThresholdOfGarbageStorage();
        List<CuboidLayoutQueryTimes> hotCuboidLayoutQueryTimesList = QueryHistoryDAO.getInstance(config, project)
                .getCuboidLayoutQueryTimes(queryTimesThreshold, CuboidLayoutQueryTimes.class);
        // query history can not provide the statistics of cuboidLayout which never been queried,
        // so that we calculate out the cuboidLayouts which query times is more than the queryTimesThreshold
        // then calculate the garbage cuboidLayouts by subtraction
        calculateGarbageStorage(config, project, hotCuboidLayoutQueryTimesList, storageVolumeInfo);
    }

    private Map<String, Set<Long>> convertToModelIndexMap(List<CuboidLayoutQueryTimes> cuboidLayoutQueryTimesList) {
        Map<String, Set<Long>> modelIndexMap = Maps.newHashMap();
        if (CollectionUtils.isEmpty(cuboidLayoutQueryTimesList)) {
            return modelIndexMap;
        }
        cuboidLayoutQueryTimesList.forEach(qt -> {
            val modelId = qt.getModelId();
            val cuboidLayoutIdStr = qt.getLayoutId();
            var cuboidLayoutIdSet = modelIndexMap.get(modelId);
            if (CollectionUtils.isEmpty(cuboidLayoutIdSet)) {
                cuboidLayoutIdSet = Sets.newHashSet();
                modelIndexMap.put(modelId, cuboidLayoutIdSet);
            }
            cuboidLayoutIdSet.add(Long.valueOf(cuboidLayoutIdStr));
        });
        return modelIndexMap;
    }

    private void calculateGarbageStorage(KylinConfig config, String project,
            List<CuboidLayoutQueryTimes> hotCuboidLayoutQueryTimesList, StorageVolumeInfo storageVolumeInfo) {
        long garbageStorageSize = 0L;
        Map<String, Set<Long>> garbageModelIndexMap = Maps.newHashMap();
        Map<String, Set<Long>> hotModelIndexMap = convertToModelIndexMap(hotCuboidLayoutQueryTimesList);

        List<NDataModel> models = NDataflowManager.getInstance(config, project).listUnderliningDataModels();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        long cuboidSurvivalTimeThreshold = config.getCuboidLayoutSurvivalTimeThreshold();

        for (NDataModel model : models) {
            val modelId = model.getId();
            val dataflow = dataflowManager.getDataflow(modelId);
            // TODO if dataflow.checkBrokenWithRelatedInfo, all layouts are garbage
            // issue : https://github.com/Kyligence/KAP/issues/9804
            if (dataflow == null || dataflow.checkBrokenWithRelatedInfo()) {
                continue;
            }
            val firstReadySegment = getFirstBuildAndReadySegment(dataflow);
            if (firstReadySegment == null) {
                continue;
            }

            val dataCuboids = firstReadySegment.getLayoutsMap().values();
            var hotCuboidLayoutIdSet = hotModelIndexMap.get(modelId);
            if (CollectionUtils.isEmpty(hotCuboidLayoutIdSet)) {
                hotCuboidLayoutIdSet = Sets.newHashSet();
            }
            val indexPlan = dataflow.getIndexPlan();
            // ruleBaseCuboidLayouts will not be identified as garbage for the moment
            val ruleBaseCuboidLayoutIdSet = indexPlan.getRuleBaseLayouts().stream().map(LayoutEntity::getId)
                    .collect(Collectors.toSet());
            val finalHotCuboidLayoutIdSet = hotCuboidLayoutIdSet;
            val cuboidLayoutIdSet = dataCuboids.stream()
                    .filter(dc -> (System.currentTimeMillis() - dc.getCreateTime()) > cuboidSurvivalTimeThreshold)
                    .map(NDataLayout::getLayoutId).filter(id -> !finalHotCuboidLayoutIdSet.contains(id))
                    .filter(id -> !ruleBaseCuboidLayoutIdSet.contains(id)).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(cuboidLayoutIdSet)) {
                garbageStorageSize += calculateLayoutSize(cuboidLayoutIdSet, dataflow);
                garbageModelIndexMap.put(modelId, cuboidLayoutIdSet);
            }
        }
        storageVolumeInfo.setGarbageModelIndexMap(garbageModelIndexMap);
        storageVolumeInfo.setGarbageStorageSize(garbageStorageSize);
    }

    private NDataSegment getFirstBuildAndReadySegment(NDataflow dataflow) {
        val readySegments = dataflow.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            return null;
        }
        Collections.sort(readySegments, new Comparator<NDataSegment>() {
            @Override
            public int compare(NDataSegment o1, NDataSegment o2) {
                return Long.compare(o1.getCreateTimeUTC(), o2.getCreateTimeUTC());
            }
        });
        return readySegments.get(0);
    }

    private long calculateLayoutSize(Set<Long> cuboidLayoutIdSet, NDataflow dataflow) {
        long cuboidLayoutSize = 0L;
        for (NDataSegment segment : dataflow.getSegments(SegmentStatusEnum.READY)) {
            for (Long cuboidLayoutId : cuboidLayoutIdSet) {
                NDataLayout dataCuboid = segment.getSegDetails().getLayoutById(cuboidLayoutId);
                if (dataCuboid != null) {
                    cuboidLayoutSize += dataCuboid.getByteSize();
                }
            }
        }
        return cuboidLayoutSize;
    }

}
