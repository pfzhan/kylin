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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class GarbageStorageCollector implements StorageInfoCollector {

    @Override
    public void collect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) {
        val favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);
        val lowFrequencyFqs = favoriteQueryManager.getLowFrequencyFQs();
        Set<FavoriteQueryRealization> lowFrequencyFqrs = lowFrequencyFqs.stream()
                .flatMap(fq -> fq.getRealizations().stream()).collect(Collectors.toSet());

        Map<String, Set<Long>> garbageIndexMap = Maps.newHashMap();
        long storageSize = 0L;

        for (val model : getModels(project)) {
            val dataflow = getDataflow(model);

            val autoLayouts = getAutoLayouts(dataflow);
            // referenced layouts
            val referencedlayouts = favoriteQueryManager.getRealizationsByConditions(model.getId(), null).stream().filter(fqr -> !lowFrequencyFqrs.contains(fqr))
                    .map(FavoriteQueryRealization::getLayoutId).collect(Collectors.toSet());
            autoLayouts.removeAll(referencedlayouts);
            if (CollectionUtils.isNotEmpty(autoLayouts)) {
                storageSize += calculateLayoutSize(autoLayouts, dataflow);
                garbageIndexMap.put(model.getId(), autoLayouts);
            }
        }

        storageVolumeInfo.setGarbageModelIndexMap(garbageIndexMap);
        storageVolumeInfo.setGarbageStorageSize(storageSize);
    }

    private List<NDataModel> getModels(String project) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return dataflowManager.listUnderliningDataModels();
    }

    private NDataflow getDataflow(NDataModel model) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        return dataflowManager.getDataflow(model.getUuid());
    }

    private Set<Long> getAutoLayouts(NDataflow dataflow) {
        val cube = dataflow.getIndexPlan();
        return cube.getWhitelistLayouts().stream().filter(layoutEntity -> !layoutEntity.isManual())
                .map(LayoutEntity::getId).collect(Collectors.toSet());
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
