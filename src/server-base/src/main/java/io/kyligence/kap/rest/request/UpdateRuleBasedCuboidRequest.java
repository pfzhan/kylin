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
package io.kyligence.kap.rest.request;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.BeanUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UpdateRuleBasedCuboidRequest {

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> aggregationGroups;

    @JsonProperty("global_dim_cap")
    private Integer globalDimCap;

    @Builder.Default
    @JsonProperty("load_data")
    private boolean isLoadData = true;

    public NRuleBasedIndex convertToRuleBasedIndex() {
        val newRuleBasedCuboid = new NRuleBasedIndex();
        BeanUtils.copyProperties(this, newRuleBasedCuboid);
        newRuleBasedCuboid.setDimensions(getSortedDimensions());
        newRuleBasedCuboid.setGlobalDimCap(globalDimCap);

        return newRuleBasedCuboid;
    }

    /**
     * for example,
     * [1,2,3] [4,3] [2,4] [5,4]
     *
     * this algorithm sorts them from last to first
     *
     * step 1:
     * mergedAndSorted = [5, 4]
     * trying merge [5, 4] to [2, 4]
     * the point is merging new elements to former agg group!!
     * currentSortedList = [2, 4]
     * 5 -> 5 is before 4, so insert 5 before 4
     * currentSortedList = [2, 5, 4]
     * so mergedAndSorted = [2, 5, 4], assgined from currentSortedList
     *
     * step 2:
     * mergedAndSorted = [2, 5, 4]
     * trying merge new elements from [2, 5, 4] to [4, 3]
     * 2 -> 2 is before 4, so insert 2 before 4
     * currentSortedList = [2, 4, 3]
     * 5 -> 5 is before 4, so insert 5 before 4
     * currentSortedList = [2, 5, 4, 3]
     * assign currentSortedList to mergedAndSorted
     *
     * step 3:
     * mergedAndSorted = [2, 5, 4, 3]
     * trying merge new elements from [2, 5, 4, 3] to [1, 2, 3]
     * 5 -> 5 is before 3, so insert 5 before 3
     * currentSortedList = [1, 2, 5, 3]
     * 4 -> 4 is before 3, so insert 4 before 3
     * currentSortedList = [1, 2, 5, 4, 3]
     *
     * get final result mergedAndSorted = [1, 2, 5, 4, 3]
     * @return
     */

    public List<Integer> getSortedDimensions() {
        if (CollectionUtils.isEmpty(aggregationGroups)) {
            return Lists.newArrayList();
        }

        // final result
        List<Integer> mergedAndSorted = Lists.newArrayList();

        // merging from bottom to top
        for (int aggGroupIndex = aggregationGroups.size() - 1; aggGroupIndex >= 0; aggGroupIndex--) {
            val includes = aggregationGroups.get(aggGroupIndex).getIncludes();
            if (includes == null || includes.length == 0)
                continue;

            final List<Integer> currentSortedList = Lists.newArrayList(includes);
            Map<Integer, Integer> mergedAndSortedIndexMap = Maps.newHashMap();

            int count = 0;
            for (int element : mergedAndSorted) {
                mergedAndSortedIndexMap.put(element, count);
                count++;
            }

            for (int dimensionId : mergedAndSorted) {
                calculateCurrentSortdList(mergedAndSortedIndexMap, currentSortedList, dimensionId);
            }

            mergedAndSorted = Lists.newArrayList(currentSortedList);
        }

        return mergedAndSorted;
    }

    private void calculateCurrentSortdList(Map<Integer, Integer> mergedAndSortedIndexMap,
            List<Integer> currentSortedList, int dimensionId) {
        boolean needToAppendToTail = true;
        Set<Integer> currentSortedSet = Sets.newHashSet(currentSortedList);
        if (currentSortedSet.contains(dimensionId)) {
            return;
        }

        Integer indexOfNewDimension = mergedAndSortedIndexMap.get(dimensionId);

        for (int oldDimensionId : currentSortedSet) {
            Integer indexOfOldDimension = mergedAndSortedIndexMap.get(oldDimensionId);

            if (indexOfOldDimension != null && indexOfNewDimension < indexOfOldDimension) {
                currentSortedList.add(currentSortedList.indexOf(oldDimensionId), dimensionId);
                needToAppendToTail = false;
                break;
            }
        }

        if (needToAppendToTail)
            currentSortedList.add(dimensionId);
    }
}
