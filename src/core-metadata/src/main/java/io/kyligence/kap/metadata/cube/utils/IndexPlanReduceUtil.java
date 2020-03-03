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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;

public class IndexPlanReduceUtil {

    private IndexPlanReduceUtil() {
    }

    public static Map<LayoutEntity, LayoutEntity> collectIncludedLayouts(List<LayoutEntity> inputLayouts,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> redundantMap = Maps.newHashMap();
        Set<LayoutEntity> tableIndexGroup = Sets.newHashSet();
        Map<List<Integer>, Set<LayoutEntity>> aggIndexDimGroup = Maps.newHashMap();
        inputLayouts.forEach(layout -> {
            // layout.getOrderedDimensions() maybe more better, but difficult to cover with simple UT
            if (layout.getId() > IndexEntity.TABLE_INDEX_START_ID) {
                tableIndexGroup.add(layout);
            } else {
                List<Integer> aggIndexDims = layout.getColOrder().stream().filter(idx -> idx < NDataModel.MEASURE_ID_BASE)
                        .collect(Collectors.toList());
                aggIndexDimGroup.putIfAbsent(aggIndexDims, Sets.newHashSet());
                aggIndexDimGroup.get(aggIndexDims).add(layout);
            }
        });

        List<LayoutEntity> tableIndexsShareSameDims = descSortByColOrderSize(Lists.newArrayList(tableIndexGroup));
        redundantMap.putAll(findIncludedLayoutMap(tableIndexsShareSameDims, isGarbageCleaning));

        aggIndexDimGroup.forEach((dims, layouts) -> {
            List<LayoutEntity> aggIndexsShareSameDims = descSortByColOrderSize(Lists.newArrayList(layouts));
            redundantMap.putAll(findIncludedLayoutMap(aggIndexsShareSameDims, isGarbageCleaning));
        });

        return redundantMap;
    }

    /**
     * Collect a redundant map from included layout to reserved layout.
     * @param sortedLayouts sorted by layout's colOrder
     * @param isGarbageCleaning if true for gc, otherwise for auto-modeling tailor layout
     */
    private static Map<LayoutEntity, LayoutEntity> findIncludedLayoutMap(List<LayoutEntity> sortedLayouts,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> includedMap = Maps.newHashMap();
        if (sortedLayouts.size() <= 1) {
            return includedMap;
        }

        for (int i = 0; i < sortedLayouts.size(); i++) {
            LayoutEntity target = sortedLayouts.get(i);
            if (includedMap.containsKey(target)) {
                continue;
            }
            for (int j = i + 1; j < sortedLayouts.size(); j++) {
                LayoutEntity current = sortedLayouts.get(j);
                // In the process of garbage cleaning all existing layouts were taken into account, 
                // but in the process of propose only layouts with status of inProposing were taken into account.
                if ((!isGarbageCleaning && !current.isInProposing())
                        || (target.getColOrder().size() == current.getColOrder().size())
                        || includedMap.containsKey(current)
                        || !Objects.equals(current.getShardByColumns(), target.getShardByColumns())) {
                    continue;
                }

                if (isContained(current, target)) {
                    includedMap.put(current, target);
                }
            }
        }
        return includedMap;
    }

    /**
     * When two layouts comes from the same group, judge whether the current is contained by the target.
     * For AggIndex, only need to judge measures included; for TableIndex, compare colOrder.
     */
    private static boolean isContained(LayoutEntity current, LayoutEntity target) {
        boolean isTableIndex = target.getId() > IndexEntity.TABLE_INDEX_START_ID;
        if (isTableIndex) {
            return isSubPartColOrder(current.getColOrder(), target.getColOrder());
        }
        Set<Integer> currentMeasures = Sets.newHashSet(current.getIndex().getMeasures());
        Set<Integer> targetMeasures = Sets.newHashSet(target.getIndex().getMeasures());
        return targetMeasures.containsAll(currentMeasures);
    }

    /**
     * Check whether current sequence is a part of target sequence.
     */
    public static boolean isSubPartColOrder(List<Integer> curSeq, List<Integer> targetSeq) {
        int i = 0;
        int j = 0;
        while (i < curSeq.size() && j < targetSeq.size()) {
            if (curSeq.get(i).intValue() == targetSeq.get(j).intValue()) {
                i++;
            }
            j++;
        }
        return i == curSeq.size() && j <= targetSeq.size();
    }

    // sort layout first to get a stable result for problem diagnosis
    public static List<LayoutEntity> descSortByColOrderSize(List<LayoutEntity> allLayouts) {
        allLayouts.sort((o1, o2) -> {
            if (o2.getColOrder().size() - o1.getColOrder().size() == 0) {
                return (int) (o1.getId() - o2.getId());
            }
            return o2.getColOrder().size() - o1.getColOrder().size();
        }); // desc by colOrder size
        return allLayouts;
    }
}
