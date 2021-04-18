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

package io.kyligence.kap.metadata.cube.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.cuboid.CuboidScheduler;
import io.kyligence.kap.metadata.cube.cuboid.CuboidScheduler.ColOrder;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity.IndexIdentifier;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.var;

@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RuleBasedIndex implements Serializable, IKeep {

    @Getter
    @JsonBackReference
    private IndexPlan indexPlan;

    @JsonProperty("dimensions")
    private List<Integer> dimensions = Lists.newArrayList();
    @JsonProperty("measures")
    private List<Integer> measures = Lists.newArrayList();

    @Setter
    @JsonProperty("global_dim_cap")
    private Integer globalDimCap;

    @Getter
    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> aggregationGroups = Lists.newArrayList();

    @Setter
    @Getter
    @JsonProperty("layout_id_mapping")
    private List<Long> layoutIdMapping = Lists.newArrayList();

    @Getter
    @JsonProperty("parent_forward")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private int parentForward = 3;

    @Setter
    @Getter
    @JsonProperty("index_start_id")
    private long indexStartId;

    @Getter
    @Setter
    @JsonProperty("last_modify_time")
    private long lastModifiedTime = System.currentTimeMillis();

    @Setter
    @Getter
    @JsonProperty("layout_black_list")
    private Set<Long> layoutBlackList = new HashSet<>();

    @Setter
    @Getter
    @JsonProperty("scheduler_version")
    private int schedulerVersion = 1;

    // computed fields below

    @Getter
    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    @Getter
    private ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures;
    @Getter
    private ImmutableBitSet dimensionBitset = null;
    @Getter
    private ImmutableBitSet measureBitset = null;
    @Getter
    private ImmutableSet<TblColRef> dimensionSet = null;
    @Getter
    private ImmutableSet<NDataModel.Measure> measureSet = null;
    private Map<Integer, Integer> dimMea2bitIndex; // dim id/measure id -> bit index
    @Getter
    private BigInteger fullMask = BigInteger.ZERO;

    @Getter(lazy = true)
    private final CuboidScheduler cuboidScheduler = initCuboidScheduler();

    public void init() {
        NDataModel model = getModel();
        this.dimensionBitset = ImmutableBitSet.valueOf(dimensions);
        this.measureBitset = ImmutableBitSet.valueOf(measures);

        this.effectiveDimCols = Maps.filterKeys(model.getEffectiveCols(),
                input -> input != null && dimensionBitset.get(input));

        this.dimensionSet = ImmutableSet.copyOf(this.effectiveDimCols.values());

        // all layouts' measure order follow cuboid_desc's define
        ImmutableBiMap.Builder<Integer, NDataModel.Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            if (model.getEffectiveMeasures().containsKey(m)) {
                measuresBuilder.put(m, model.getEffectiveMeasures().get(m));
            }
        }
        this.orderedMeasures = measuresBuilder.build();
        this.measureSet = orderedMeasures.values();

        dimMea2bitIndex = Maps.newHashMap();
        int bitSize = dimensions.size() + measures.size();
        for (int i = 0; i < dimensions.size(); i++) {
            dimMea2bitIndex.put(dimensions.get(i), bitSize - i - 1);
        }

        for (int i = 0; i < measures.size(); i++) {
            dimMea2bitIndex.put(measures.get(i), measures.size() - i - 1);
        }

        if (CollectionUtils.isNotEmpty(dimensions)) {
            for (int i = 0; i < dimensions.size() + measures.size(); i++) {
                fullMask = fullMask.setBit(i);
            }
        }

        for (NAggregationGroup nAggregationGroup : aggregationGroups) {
            nAggregationGroup.init(this);
        }
    }

    public CuboidScheduler initCuboidScheduler() {
        return CuboidScheduler.getInstance(indexPlan, this);
    }

    public int getGlobalDimCap() {
        return globalDimCap == null ? 0 : globalDimCap;
    }

    public int getColumnBitIndex(Integer colId) {
        return dimMea2bitIndex.get(colId);
    }

    public Set<LayoutEntity> genCuboidLayouts() {
        return genCuboidLayouts(Sets.newHashSet(), Sets.newHashSet(), true);
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataModel getModel() {
        return indexPlan.getModel();
    }

    public void setIndexPlan(IndexPlan indexPlan) {
        checkIsNotCachedAndShared();
        this.indexPlan = indexPlan;
    }

    public List<Integer> getDimensions() {
        return isCachedAndShared() ? Lists.newArrayList(dimensions) : dimensions;
    }

    public void setDimensions(List<Integer> dimensions) {
        checkIsNotCachedAndShared();
        this.dimensions = dimensions;
    }

    public List<Integer> getMeasures() {
        return isCachedAndShared() ? Lists.newArrayList(measures) : measures;
    }

    public void setMeasures(List<Integer> measures) {
        checkIsNotCachedAndShared();
        this.measures = measures;
    }

    public void setAggregationGroups(List<NAggregationGroup> aggregationGroups) {
        checkIsNotCachedAndShared();
        this.aggregationGroups = aggregationGroups;
    }

    public boolean isCachedAndShared() {
        return indexPlan != null && indexPlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (indexPlan != null)
            indexPlan.checkIsNotCachedAndShared();
    }

    public void setParentForward(int parentForward) {
        checkIsNotCachedAndShared();
        this.parentForward = parentForward;
    }

    @Getter(lazy = true)
    private final ImmutableBitSet measuresBitSet = initMeasuresBitSet();

    private ImmutableBitSet initMeasuresBitSet() {
        return ImmutableBitSet.valueOf(getMeasures());
    }

    Set<LayoutEntity> genCuboidLayouts(Set<LayoutEntity> previousLayouts) {
        return genCuboidLayouts(previousLayouts, Sets.newHashSet(), true);
    }

    Set<LayoutEntity> genCuboidLayouts(Set<LayoutEntity> previousLayouts, Set<LayoutEntity> needDelLayouts) {
        return genCuboidLayouts(previousLayouts, needDelLayouts, true);
    }

    Set<LayoutEntity> genCuboidLayouts(Set<LayoutEntity> previousLayouts, Set<LayoutEntity> needDelLayouts,
            boolean excludeDel) {

        Set<LayoutEntity> genLayouts = Sets.newHashSet();

        Map<LayoutEntity, Long> existLayouts = Maps.newHashMap();
        for (LayoutEntity layout : previousLayouts) {
            existLayouts.put(layout, layout.getId());
        }
        for (LayoutEntity layout : indexPlan.getWhitelistLayouts()) {
            existLayouts.put(layout, layout.getId());
        }

        Map<LayoutEntity, Long> delLayouts = Maps.newHashMap();
        for (LayoutEntity layout : needDelLayouts) {
            delLayouts.put(layout, layout.getId());
        }

        Map<IndexIdentifier, IndexEntity> identifierIndexMap = existLayouts.keySet().stream()
                .map(LayoutEntity::getIndex).collect(Collectors.groupingBy(IndexEntity::createIndexIdentifier,
                        Collectors.reducing(null, (l, r) -> r)));
        boolean needAllocationId = layoutIdMapping.isEmpty();
        long proposalId = indexStartId + 1;

        val colOrders = getCuboidScheduler().getAllColOrders();
        for (int i = 0; i < colOrders.size(); i++) {
            val colOrder = colOrders.get(i);

            val layout = createLayout(colOrder);

            val dimensionsInLayout = colOrder.getDimensions();
            val measuresInLayout = colOrder.getMeasures();

            // if a cuboid is same as the layout's one, then reuse it
            val indexIdentifier = new IndexEntity.IndexIdentifier(dimensionsInLayout, measuresInLayout, false);
            var layoutIndex = identifierIndexMap.get(indexIdentifier);
            // if two layout is equal, the id should be same
            Long prevId = existLayouts.get(layout);

            if (needAllocationId) {
                if (prevId != null) {
                    layout.setId(existLayouts.get(layout));
                } else if (delLayouts.containsKey(layout)) {
                    layout.setId(delLayouts.get(layout));
                    layoutBlackList.add(delLayouts.get(layout));
                } else if (layoutIndex != null) {
                    val id = layoutIndex.getId() + layoutIndex.getNextLayoutOffset();
                    layout.setId(id);
                } else {
                    layout.setId(proposalId);
                    proposalId += IndexEntity.INDEX_ID_STEP;
                }
                layoutIdMapping.add(layout.getId());
            } else {
                layout.setId(layoutIdMapping.get(i));
            }

            if (layoutIndex == null) {
                long indexId = layout.getIndexId();
                layoutIndex = new IndexEntity();
                layoutIndex.setId(indexId);
                layoutIndex.setDimensions(dimensionsInLayout);
                layoutIndex.setMeasures(measuresInLayout);
                layoutIndex.setIndexPlan(indexPlan);
                layoutIndex.setNextLayoutOffset(layout.getId() % IndexEntity.INDEX_ID_STEP + 1);

                identifierIndexMap.putIfAbsent(layoutIndex.createIndexIdentifier(), layoutIndex);
            } else {
                layoutIndex.setNextLayoutOffset(
                        Math.max(layout.getId() % IndexEntity.INDEX_ID_STEP + 1, layoutIndex.getNextLayoutOffset()));
            }
            layout.setIndex(layoutIndex);

            genLayouts.add(layout);
        }

        // remove layout in blacklist
        if (excludeDel) {
            genLayouts.removeIf(layout -> layoutBlackList.contains(layout.getId()));
        }
        return genLayouts;
    }

    private LayoutEntity createLayout(ColOrder colOrder) {
        LayoutEntity layout = new LayoutEntity();
        layout.setManual(true);
        layout.setColOrder(colOrder.toList());
        if (colOrder.getDimensions().containsAll(indexPlan.getAggShardByColumns())) {
            layout.setShardByColumns(indexPlan.getAggShardByColumns());
        }
        if (colOrder.getDimensions().containsAll(indexPlan.getExtendPartitionColumns())
                && getModel().getStorageType() == 2) {
            layout.setPartitionByColumns(indexPlan.getExtendPartitionColumns());
        }
        layout.setUpdateTime(lastModifiedTime);
        layout.setStorageType(IStorageAware.ID_NDATA_STORAGE);
        return layout;
    }

    public Set<LayoutEntity> getBlacklistLayouts() {
        val allLayouts = genCuboidLayouts(Sets.newHashSet(), Sets.newHashSet(), false);
        val existLayouts = genCuboidLayouts();
        return allLayouts.stream().filter(layout -> !existLayouts.contains(layout)).collect(Collectors.toSet());
    }

    public static RuleBasedIndex copy(RuleBasedIndex oldRuleBasedIndex) {
        RuleBasedIndex newRuleBasedIndex = new RuleBasedIndex();
        newRuleBasedIndex.setAggregationGroups(oldRuleBasedIndex.getAggregationGroups());
        newRuleBasedIndex.setGlobalDimCap(oldRuleBasedIndex.getGlobalDimCap());
        newRuleBasedIndex.setSchedulerVersion(oldRuleBasedIndex.getSchedulerVersion());
        newRuleBasedIndex.adjustDimensions();
        newRuleBasedIndex.adjustMeasures();
        newRuleBasedIndex.setLastModifiedTime(System.currentTimeMillis());
        return newRuleBasedIndex;
    }

    public void adjustMeasures() {
        if (CollectionUtils.isEmpty(aggregationGroups)) {
            return;
        }

        List<Integer> measures = recomputeMeasures(this.getAggregationGroups());
        this.setMeasures(Lists.newArrayList(measures));
    }

    private List<Integer> recomputeMeasures(List<NAggregationGroup> aggregationGroups) {
        TreeSet<Integer> measures = new TreeSet<>();
        if (CollectionUtils.isEmpty(aggregationGroups))
            return Lists.newArrayList();

        for (NAggregationGroup agg : aggregationGroups) {
            Integer[] aggMeasures = agg.getMeasures();
            if (aggMeasures == null || aggMeasures.length == 0)
                continue;
            measures.addAll(Sets.newHashSet(aggMeasures));
        }
        return Lists.newArrayList(measures);
    }

    public void adjustDimensions() {
        List<Integer> dimensions = recomputeSortedDimensions(this.aggregationGroups);
        setDimensions(dimensions);
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
     */
    private List<Integer> recomputeSortedDimensions(List<NAggregationGroup> aggregationGroups) {

        if (CollectionUtils.isEmpty(aggregationGroups)) {
            return Lists.newArrayList();
        }

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
                calculateCurrentSortedList(mergedAndSortedIndexMap, currentSortedList, dimensionId);
            }

            mergedAndSorted = Lists.newArrayList(currentSortedList);
        }
        return mergedAndSorted;
    }

    private void calculateCurrentSortedList(Map<Integer, Integer> mergedAndSortedIndexMap,
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
