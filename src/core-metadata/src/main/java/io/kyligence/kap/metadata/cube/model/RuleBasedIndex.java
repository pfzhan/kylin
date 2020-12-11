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
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.var;

@SuppressWarnings("serial")
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
        val result = Sets.<LayoutEntity> newHashSet();
        genCuboidLayouts(result);
        return result;
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

    void genCuboidLayouts(Set<LayoutEntity> result) {
        CuboidScheduler initialCuboidScheduler = getCuboidScheduler();
        val colOrders = initialCuboidScheduler.getAllColOrders();
        Map<LayoutEntity, Long> layoutIdMap = Maps.newHashMap();
        for (LayoutEntity layout : result) {
            layoutIdMap.put(layout, layout.getId());
        }
        for (LayoutEntity layout : indexPlan.getWhitelistLayouts()) {
            layoutIdMap.put(layout, layout.getId());
        }
        val identifierIndexMap = layoutIdMap.keySet().stream().map(LayoutEntity::getIndex).collect(
                Collectors.groupingBy(IndexEntity::createIndexIdentifier, Collectors.reducing(null, (l, r) -> r)));
        val needAllocationId = layoutIdMapping.isEmpty();
        long proposalId = indexStartId + 1;
        for (int i = 0; i < colOrders.size(); i++) {
            val colOrder = colOrders.get(i);
            val layout = new LayoutEntity();
            layout.setManual(true);

            val dimensionsInLayout = colOrder.getDimensions();
            val measuresInLayout = colOrder.getMeasures();
            layout.setColOrder(colOrder.toList());
            if (colOrder.getDimensions().containsAll(indexPlan.getAggShardByColumns())) {
                layout.setShardByColumns(indexPlan.getAggShardByColumns());
            }
            if (colOrder.getDimensions().containsAll(indexPlan.getExtendPartitionColumns())
                    && getModel().getStorageType() == 2) {
                layout.setPartitionByColumns(indexPlan.getExtendPartitionColumns());
            }
            layout.setStorageType(IStorageAware.ID_NDATA_STORAGE);

            // if a cuboid is same as the layout's one, then reuse it
            val indexIdentifier = new IndexEntity.IndexIdentifier(dimensionsInLayout, measuresInLayout, false);
            var maybeIndex = identifierIndexMap.get(indexIdentifier);
            // if two layout is equal, the id should be same
            Long prevId = layoutIdMap.get(layout);
            if (!needAllocationId) {
                layout.setId(layoutIdMapping.get(i));
            } else if (prevId != null) {
                layout.setId(layoutIdMap.get(layout));
            } else if (maybeIndex != null) {
                val id = maybeIndex.getId() + maybeIndex.getNextLayoutOffset();
                layout.setId(id);
            } else {
                layout.setId(proposalId);
                proposalId += IndexEntity.INDEX_ID_STEP;
            }
            if (needAllocationId) {
                layoutIdMapping.add(layout.getId());
            }

            if (maybeIndex == null) {
                long indexId = layout.getIndexId();
                maybeIndex = new IndexEntity();
                maybeIndex.setId(indexId);
                maybeIndex.setDimensions(dimensionsInLayout);
                maybeIndex.setMeasures(measuresInLayout);
                maybeIndex.setIndexPlan(indexPlan);
                maybeIndex.setNextLayoutOffset(layout.getId() % IndexEntity.INDEX_ID_STEP + 1);
                identifierIndexMap.putIfAbsent(maybeIndex.createIndexIdentifier(), maybeIndex);
            } else {
                maybeIndex.setNextLayoutOffset(
                        Math.max(layout.getId() % IndexEntity.INDEX_ID_STEP + 1, maybeIndex.getNextLayoutOffset()));
            }
            layout.setUpdateTime(lastModifiedTime);
            layout.setIndex(maybeIndex);

            result.add(layout);
        }

        // remove layout in blacklist
        result.removeIf(layout -> layoutBlackList.contains(layout.getId()));
    }
}