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

package io.kyligence.kap.cube.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.cuboid.NCuboidScheduler;
import io.kyligence.kap.metadata.model.IKapStorageAware;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NRuleBasedCuboidsDesc implements Serializable, IKeep {

    @Getter
    @JsonBackReference
    private NCubePlan cubePlan;

    @JsonProperty("dimensions")
    private List<Integer> dimensions = Lists.newArrayList();
    @JsonProperty("measures")
    private List<Integer> measures = Lists.newArrayList();

    @Getter
    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> aggregationGroups = Lists.newArrayList();

    @JsonProperty("cuboid_black_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Set<Long> cuboidBlackSet = Sets.newHashSet();

    @Getter
    @JsonProperty("parent_forward")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private int parentForward = 3;

    @Setter
    @Getter
    @JsonProperty("new_rule_based_cuboid")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private NRuleBasedCuboidsDesc newRuleBasedCuboid;

    @Setter
    @Getter
    @JsonProperty("cuboid_id_mapping")
    private List<Long> cuboidIdMapping = Lists.newArrayList();

    // computed fields below

    private NDataModel model;
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
    private Map<Integer, Integer> dim2bitIndex;//dim id -> bit index in rowkey, same as in org.apache.kylin.cube.model.CubeDesc.getColumnByBitIndex()
    @Getter
    private long fullMask = 0L;
    private NCuboidScheduler cuboidScheduler = null;

    public NRuleBasedCuboidsDesc() {
    }

    public void init() {
        this.model = getModel();
        this.dimensionBitset = ImmutableBitSet.valueOf(dimensions);
        this.measureBitset = ImmutableBitSet.valueOf(measures);

        this.effectiveDimCols = Maps.filterKeys(model.getEffectiveColsMap(), new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer input) {
                return input != null && dimensionBitset.get(input);
            }
        });

        this.dimensionSet = ImmutableSet.copyOf(this.effectiveDimCols.values());

        // all layouts' measure order follow cuboid_desc's define
        ImmutableBiMap.Builder<Integer, NDataModel.Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            measuresBuilder.put(m, model.getEffectiveMeasureMap().get(m));
        }
        this.orderedMeasures = measuresBuilder.build();
        this.measureSet = orderedMeasures.values();

        dim2bitIndex = Maps.newHashMap();
        for (int i = 0; i < dimensions.size(); i++) {
            dim2bitIndex.put(dimensions.get(i), dimensions.size() - i - 1);
        }

        for (int i = 0; i < dimensions.size(); i++) {
            fullMask |= 1L << i;
        }

        for (NAggregationGroup nAggregationGroup : aggregationGroups) {
            nAggregationGroup.init(cubePlan);
        }

        if (newRuleBasedCuboid != null) {
            newRuleBasedCuboid.setCubePlan(cubePlan);
            newRuleBasedCuboid.init();
        }
    }

    public NCuboidScheduler getInitialCuboidScheduler() {
        if (cuboidScheduler != null)
            return cuboidScheduler;

        synchronized (this) {
            if (cuboidScheduler == null) {
                cuboidScheduler = NCuboidScheduler.getInstance(cubePlan, this);
            }
            return cuboidScheduler;
        }
    }

    public int getColumnBitIndex(Integer colId) {
        return dim2bitIndex.get(colId);
    }
    public Set<NCuboidDesc> genCuboidDescs() {
        Set<NCuboidDesc> result = Sets.newHashSet();
        genCuboidDescs(NCuboidDesc.RULE_BASED_CUBOID_START_ID, 1, result);
        return result;
    }

    public Set<NCuboidLayout> genCuboidLayouts() {
        val result = Sets.<NCuboidLayout>newHashSet();
        for (NCuboidDesc genCuboidDesc : genCuboidDescs()) {
            result.addAll(genCuboidDesc.getLayouts());
        }
        return result;
    }

    public int getColumnBitIndex(TblColRef tblColRef) {
        int dimensionId = effectiveDimCols.inverse().get(tblColRef);
        return dim2bitIndex.get(dimensionId);
    }

    public boolean dimensionsDerive(TblColRef... dimensions) {
        Map<TblColRef, Integer> colIdMap = getEffectiveDimCols().inverse();
        for (TblColRef fk : dimensions) {
            if (!colIdMap.containsKey(fk)) {
                return false;
            }
        }
        return true;
    }

    public boolean dimensionDerive(NRuleBasedCuboidsDesc child) {
        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty();
    }

    public boolean fullyDerive(NRuleBasedCuboidsDesc child) {
        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty()
                && child.getMeasureBitset().andNot(getMeasureBitset()).isEmpty();
    }

    public List<MeasureDesc> getMeasureDescs() {
        Collection<NDataModel.Measure> measures = getOrderedMeasures().values();
        List<MeasureDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        result.addAll(measures);
        return result;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataModel getModel() {
        return cubePlan.getModel();
    }

    public void setCubePlan(NCubePlan cubePlan) {
        checkIsNotCachedAndShared();
        this.cubePlan = cubePlan;
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
        return cubePlan != null && cubePlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (cubePlan != null)
            cubePlan.checkIsNotCachedAndShared();
    }

    public void setParentForward(int parentForward) {
        checkIsNotCachedAndShared();
        this.parentForward = parentForward;
    }

    public boolean isBlackedCuboid(long cuboidID) {
        return cuboidBlackSet.contains(cuboidID);
    }

    private void genCuboidDescs(final long advisedStartId, int version, Set<NCuboidDesc> result) {
        NCuboidScheduler initialCuboidScheduler = getInitialCuboidScheduler();
        List<Long> allCuboidIds = Lists.newArrayList(initialCuboidScheduler.getAllCuboidIds());

        Map<NCuboidLayout, Long> layoutIdMap = Maps.newHashMap();
        for (NCuboidDesc nCuboidDesc : result) {
            for (NCuboidLayout layout : nCuboidDesc.getLayouts()) {
                layoutIdMap.put(layout, layout.getId());
            }
        }
        if (cuboidIdMapping.isEmpty()) {
            for (int i = 0; i < allCuboidIds.size(); i++) {
                cuboidIdMapping.add(advisedStartId + i * NCuboidDesc.CUBOID_DESC_ID_STEP);
            }
        }

        //convert all legacy cuboids generated from rules to NCuboidDesc & NCuboidLayout
        for (int i = 0; i < allCuboidIds.size(); i++) {
            long cuboidId = allCuboidIds.get(i);

            long nCuboidId = cuboidIdMapping.get(i);
            long nlayoutId = nCuboidId + 1;

            //mock a NCuboidLayout for one legacy cuboid
            NCuboidLayout layout = new NCuboidLayout();
            layout.setId(nlayoutId);
            layout.setVersion(version);

            List<Integer> colOrder = Lists.newArrayList(tailor(getDimensions(), cuboidId));
            colOrder.addAll(getMeasures());
            layout.setColOrder(colOrder);
            layout.setStorageType(IKapStorageAware.ID_NDATA_STORAGE);

            // if two layout is equal, the id should be same
            Long prevId = layoutIdMap.get(layout);
            if (prevId != null) {
                layout.setId(layoutIdMap.get(layout));
                cuboidIdMapping.set(i, prevId - 1);
            }

            //mock a NCuboidDesc for one legacy cuboid
            NCuboidDesc nCuboidDesc = new NCuboidDesc();
            layout.setCuboidDesc(nCuboidDesc);
            nCuboidDesc.setId(nCuboidId);
            nCuboidDesc.setLayouts(Lists.newArrayList(layout));
            nCuboidDesc.setDimensions(tailor(getDimensions(), cuboidId));
            nCuboidDesc.setMeasures(getMeasures());
            nCuboidDesc.setCubePlan(cubePlan);
            nCuboidDesc.init();

            result.add(nCuboidDesc);
        }
        if (newRuleBasedCuboid != null) {
            newRuleBasedCuboid.genCuboidDescs(cuboidIdMapping.isEmpty() ? advisedStartId : Collections.max(cuboidIdMapping) + 1000,
                    version + 1, result);
        }
    }

    private List<Integer> tailor(List<Integer> complete, long cuboidId) {

        int bitCount = Long.bitCount(cuboidId);

        Integer[] ret = new Integer[bitCount];

        int next = 0;
        for (int i = 0; i < complete.size(); i++) {
            int shift = complete.size() - i - 1;
            if ((cuboidId & (1L << shift)) != 0) {
                ret[next++] = complete.get(i);
            }
        }

        if (ret.length > 0 && ret[ret.length - 1] == null) {
            System.out.println();
        }
        return Arrays.asList(ret);
    }
}
