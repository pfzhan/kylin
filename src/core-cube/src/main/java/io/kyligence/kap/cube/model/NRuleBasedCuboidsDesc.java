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
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NRuleBasedCuboidsDesc implements Serializable, IKeep {

    @JsonBackReference
    private NCubePlan cubePlan;

    @JsonProperty("dimensions")
    private int[] dimensions = new int[0];
    @JsonProperty("measures")
    private int[] measures = new int[0];

    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> nAggregationGroups;

    @JsonProperty("cuboid_black_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Set<Long> cuboidBlackSet = Sets.newHashSet();

    @JsonProperty("parent_forward")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private int parentForward = 3;

    // computed fields below

    private NDataModel model;
    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures;
    private ImmutableBitSet dimensionBitset = null;
    private ImmutableBitSet measureBitset = null;
    private ImmutableSet<TblColRef> dimensionSet = null;
    private ImmutableSet<NDataModel.Measure> measureSet = null;
    private Map<Integer, Integer> dim2bitIndex;//dim id -> bit index in rowkey, same as in org.apache.kylin.cube.model.CubeDesc.getColumnByBitIndex()
    private long fullMask = 0L;
    private NCuboidScheduler cuboidScheduler = null;

    public NRuleBasedCuboidsDesc() {
    }

    void init() {
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
        for (int i = 0; i < dimensions.length; i++) {
            dim2bitIndex.put(dimensions[i], dimensions.length - i - 1);
        }

        for (int i = 0; i < dimensions.length; i++) {
            fullMask |= 1L << i;
        }

        for (NAggregationGroup nAggregationGroup : nAggregationGroups) {
            nAggregationGroup.init(cubePlan);
        }
    }

    public NCuboidScheduler getInitialCuboidScheduler() {
        if (cuboidScheduler != null)
            return cuboidScheduler;

        synchronized (this) {
            if (cuboidScheduler == null) {
                cuboidScheduler = NCuboidScheduler.getInstance(this.cubePlan);
            }
            return cuboidScheduler;
        }
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

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public ImmutableBiMap<Integer, NDataModel.Measure> getOrderedMeasures() {
        return orderedMeasures;
    }

    public ImmutableBitSet getDimensionBitset() {
        return dimensionBitset;
    }

    public ImmutableBitSet getMeasureBitset() {
        return measureBitset;
    }

    public ImmutableSet<TblColRef> getDimensionSet() {
        return dimensionSet;
    }

    public ImmutableSet<NDataModel.Measure> getMeasureSet() {
        return measureSet;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public long getFullMask() {
        return fullMask;
    }

    public NCubePlan getCubePlan() {
        return cubePlan;
    }

    public NDataModel getModel() {
        return cubePlan.getModel();
    }

    public void setCubePlan(NCubePlan cubePlan) {
        checkIsNotCachedAndShared();
        this.cubePlan = cubePlan;
    }

    public int[] getDimensions() {
        return isCachedAndShared() ? Arrays.copyOf(dimensions, dimensions.length) : dimensions;
    }

    public void setDimensions(int[] dimensions) {
        checkIsNotCachedAndShared();
        this.dimensions = dimensions;
    }

    public int[] getMeasures() {
        return isCachedAndShared() ? Arrays.copyOf(measures, measures.length) : measures;
    }

    public void setMeasures(int[] measures) {
        checkIsNotCachedAndShared();
        this.measures = measures;
    }

    public boolean isCachedAndShared() {
        return cubePlan != null && cubePlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (cubePlan != null)
            cubePlan.checkIsNotCachedAndShared();
    }

    public List<NAggregationGroup> getNAggregationGroups() {
        return nAggregationGroups;
    }

    public int getParentForward() {
        return parentForward;
    }

    public void setParentForward(int parentForward) {
        checkIsNotCachedAndShared();
        this.parentForward = parentForward;
    }

    public boolean isBlackedCuboid(long cuboidID) {
        return cuboidBlackSet.contains(cuboidID);
    }
}
