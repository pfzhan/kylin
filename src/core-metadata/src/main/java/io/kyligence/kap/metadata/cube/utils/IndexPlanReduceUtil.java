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

package io.kyligence.kap.metadata.cube.utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.ImmutableBitSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;

public class IndexPlanReduceUtil {

    private IndexPlanReduceUtil() {
    }

    /**
     * Collect a redundant map from redundant layout to reserved layout of aggregate index.
     */
    public static Map<LayoutEntity, LayoutEntity> collectRedundantLayoutsOfAggIndex(List<IndexEntity> aggIndexList,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> redundantMap = Maps.newHashMap();
        Map<ImmutableBitSet, List<IndexEntity>> indexGroupedByDims = Maps.newHashMap();
        aggIndexList.forEach(index -> {
            indexGroupedByDims.putIfAbsent(index.getDimensionBitset(), Lists.newArrayList());
            indexGroupedByDims.get(index.getDimensionBitset()).add(index);
        });
        Map<ImmutableBitSet, List<LayoutEntity>> layoutGroupedByDims = Maps.newHashMap();
        indexGroupedByDims.forEach((dimBitSet, indexes) -> {
            layoutGroupedByDims.putIfAbsent(dimBitSet, Lists.newArrayList());
            indexes.forEach(index -> layoutGroupedByDims.get(dimBitSet).addAll(index.getLayouts()));
        });
        layoutGroupedByDims.forEach((dims, layouts) -> {
            List<LayoutEntity> layoutsShareSameDims = descSort(layouts);
            redundantMap.putAll(collectRedundantLayouts(layoutsShareSameDims, isGarbageCleaning));
        });

        return redundantMap;
    }

    /**
     * Collect a redundant map from redundant layout to reserved layout of table index.
     */
    public static Map<LayoutEntity, LayoutEntity> collectRedundantLayoutsOfTableIndex(List<IndexEntity> tableIndexList,
            boolean isGarbageCleaning) {
        List<LayoutEntity> allLayouts = Lists.newArrayList();
        if (isGarbageCleaning) {
            tableIndexList.forEach(index -> allLayouts.addAll(index.getLayouts()));
        } else {
            tableIndexList.forEach(index -> allLayouts.addAll(//
                    index.getLayouts().stream() //
                            .filter(LayoutEntity::isAuto) //
                            .collect(Collectors.toSet())));
        }
        return collectRedundantLayouts(descSort(allLayouts), isGarbageCleaning);
    }

    /**
     * Collect a redundant map from redundant layout to reserved layout.
     */
    private static Map<LayoutEntity, LayoutEntity> collectRedundantLayouts(List<LayoutEntity> allLayouts,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> redundantMap = Maps.newHashMap();
        for (int i = 0; i < allLayouts.size(); i++) {
            LayoutEntity targetLayout = allLayouts.get(i);
            if ((!isGarbageCleaning && !targetLayout.isInProposing()) || redundantMap.containsKey(targetLayout)) {
                continue;
            }
            for (int j = i + 1; j < allLayouts.size(); j++) {
                LayoutEntity current = allLayouts.get(j);
                // In the process of garbage cleaning all existing layouts were taken into account, 
                // but in the process of propose only layouts with status of inProposing were taken into account.
                if (!isGarbageCleaning && !current.isInProposing()) {
                    continue;
                }
                if (!redundantMap.containsKey(current)
                        && isSubPartColOrder(current.getColOrder(), targetLayout.getColOrder())
                        && Objects.equals(current.getShardByColumns(), targetLayout.getShardByColumns())) {
                    redundantMap.put(current, targetLayout);
                }
            }
        }
        return redundantMap;
    }

    /**
     * Check whether currentColOrder is a part of targetColOrder.
     * Say: [1, 2, 3, 100000] is a part of [1, 2, 3, 100000, 100001],
     * [1, 3, 2, 100000] is not a part of [1, 2, 3, 100000, 100001],
     * [1, 2, 3] is a part of [1, 2, 3, 4],
     * [1, 2, 3, 100000] is a part of [1, 2, 3, 4, 100000, 100001] but this will not happen
     * for aggregate index layout should share the same dimensions.
     */
    private static boolean isSubPartColOrder(List<Integer> currentColOrder, List<Integer> targetColOrder) {
        //TODO improve performance
        int before = -1; // not work when i = 0
        for (int i = 0; i < currentColOrder.size(); i++) {
            int tmp = targetColOrder.indexOf(currentColOrder.get(i));
            if (tmp < i || (i > 0 && tmp <= before)) {
                return false;
            }
            before = tmp;
        }

        return true;
    }

    // sort layout first to get a stable result for problem diagnosis
    private static List<LayoutEntity> descSort(List<LayoutEntity> allLayouts) {
        allLayouts.sort((o1, o2) -> {
            if (o2.getColOrder().size() - o1.getColOrder().size() == 0) {
                return (int) (o1.getId() - o2.getId());
            }
            return o2.getColOrder().size() - o1.getColOrder().size();
        }); // desc by colOrder size
        return allLayouts;
    }
}
