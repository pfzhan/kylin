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

package io.kyligence.kap.smart.index;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.utils.IndexPlanReduceUtil;
import io.kyligence.kap.smart.AbstractContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class NIndexReducer extends NAbstractIndexProposer {

    NIndexReducer(NModelContext context) {
        super(context);
    }

    @Override
    public IndexPlan execute(IndexPlan indexPlan) {

        log.debug("Start to reduce redundant layouts...");
        List<IndexEntity> allProposedIndexes = indexPlan.getIndexes();

        // collect redundant layouts
        List<LayoutEntity> layoutsToHandle = indexPlan.getAllLayouts();
        if (!KylinConfig.getInstanceFromEnv().isIncludedStrategyConsiderTableIndex()) {
            layoutsToHandle.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
        Map<LayoutEntity, LayoutEntity> redundantToReservedMap = Maps.newHashMap();
        redundantToReservedMap.putAll(IndexPlanReduceUtil.collectIncludedLayouts(layoutsToHandle, false));

        redundantToReservedMap.forEach((redundant, reserved) -> {
            val indexEntityOptional = allProposedIndexes.stream()
                    .filter(index -> index.getId() == redundant.getIndexId()) //
                    .findFirst();
            indexEntityOptional.ifPresent(entity -> entity.getLayouts().remove(redundant));
        });

        Set<String> redundantRecord = Sets.newHashSet();
        redundantToReservedMap.forEach((key, value) -> redundantRecord.add(key.getId() + "->" + value.getId()));
        log.trace("In this round, IndexPlan({}) found redundant layout(s) is|are: {}", indexPlan.getUuid(),
                String.join(", ", redundantRecord));

        // remove indexes without layouts
        List<IndexEntity> allReservedIndexList = allProposedIndexes.stream()
                .filter(indexEntity -> !indexEntity.getLayouts().isEmpty()) //
                .collect(Collectors.toList());
        log.debug("Proposed {} indexes, {} indexes will be reserved.", allProposedIndexes.size(),
                allReservedIndexList.size());
        allReservedIndexList.forEach(index -> index.getLayouts().forEach(layout -> layout.setInProposing(false)));
        indexPlan.setIndexes(allReservedIndexList);
        cleanRedundantLayoutRecommendations(redundantToReservedMap);

        // remove reduced index for indexRexItem
        Set<Long> layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        context.setIndexRexItemMap(context.getIndexRexItemMap().entrySet().stream()
                .filter(e -> layoutIds.contains(e.getValue().getLayout().getId()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

        // adjust acceleration info
        adjustAccelerationInfo(redundantToReservedMap);
        log.debug("End of reduce indexes and layouts!");
        return indexPlan;
    }

    private void cleanRedundantLayoutRecommendations(Map<LayoutEntity, LayoutEntity> redundantToReserveMap) {
        if (!context.getProposeContext().needCollectRecommendations()) {
            return;
        }
        redundantToReserveMap.forEach((redundant, reserved) -> {
            context.getIndexRexItemMap().remove(redundant.genUniqueFlag());
        });
    }

    private void adjustAccelerationInfo(Map<LayoutEntity, LayoutEntity> redundantToReservedMap) {
        Map<String, AccelerateInfo> accelerateInfoMap = context.getProposeContext().getAccelerateInfoMap();
        Map<Long, LayoutEntity> redundantMap = Maps.newHashMap();
        redundantToReservedMap.forEach((redundant, reserved) -> redundantMap.putIfAbsent(redundant.getId(), redundant));
        accelerateInfoMap.forEach((key, value) -> {
            if (value.getRelatedLayouts() == null) {
                return;
            }
            value.getRelatedLayouts().forEach(relatedLayout -> {
                if (redundantMap.containsKey(relatedLayout.getLayoutId())) {
                    LayoutEntity entity = redundantMap.get(relatedLayout.getLayoutId());
                    // confirm layoutInfo in accelerationInfoMap equals to redundant layout
                    if (entity.getIndex().getIndexPlan().getUuid().equalsIgnoreCase(relatedLayout.getModelId())) {
                        LayoutEntity reserved = redundantToReservedMap.get(entity);
                        relatedLayout.setLayoutId(reserved.getId());
                        relatedLayout.setModelId(reserved.getIndex().getIndexPlan().getUuid());
                    }
                }
            });
            value.setRelatedLayouts(Sets.newHashSet(value.getRelatedLayouts())); // may exist equal objects
        });
    }

}
