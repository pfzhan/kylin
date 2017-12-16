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
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NCuboidDesc implements Serializable, IKeep {

    @JsonProperty("id")
    private long id;
    @JsonProperty("dimensions")
    private int[] dimensions = new int[0];
    @JsonProperty("measures")
    private int[] measures = new int[0];
    @JsonManagedReference
    @JsonProperty("layouts")
    private List<NCuboidLayout> layouts = Lists.newArrayList();

    @JsonBackReference
    private NCubePlan cubePlan;

    private NDataModel model;

    // FIXME why the below are transient??
    private transient BiMap<Integer, TblColRef> effectiveDimCols;
    private ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures;
    private transient ImmutableBitSet dimensionSet = null;
    private transient ImmutableBitSet measureSet = null;

    public NCuboidDesc() {
    }

    void init() {
        this.model = cubePlan.getModel();
        this.dimensionSet = ImmutableBitSet.valueOf(dimensions);
        this.measureSet = ImmutableBitSet.valueOf(measures);

        this.effectiveDimCols = Maps.filterKeys(model.getEffectiveColsMap(), new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer input) {
                return input != null && dimensionSet.get(input);
            }
        });

        // all layouts' measure order follow cuboid_desc's define
        ImmutableBiMap.Builder<Integer, NDataModel.Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            measuresBuilder.put(m, model.getEffectiveMeasureMap().get(m));
        }
        orderedMeasures = measuresBuilder.build();
    }

    public ImmutableBitSet getEffectiveDimBitSetIncludingDerived() {
        BitSet bitSet = new BitSet();
        bitSet.or(dimensionSet.mutable());

        for (Map.Entry<TableRef, BitSet> derivedColsEntry : model.getEffectiveDerivedColsMap().entrySet()) {
            JoinDesc joinDesc = model.getJoinByPKSide(derivedColsEntry.getKey());
            TblColRef[] fkColRefs = joinDesc.getForeignKeyColumns();
            if (effectiveDimCols.values().containsAll(Arrays.asList(fkColRefs))) {
                bitSet.or(derivedColsEntry.getValue());
            }
        }

        return new ImmutableBitSet(bitSet);
    }

    public boolean dimensionDerive(NCuboidDesc child) {
        return child.getDimensionSet().andNot(getDimensionSet()).isEmpty();
    }

    public boolean fullyDerive(NCuboidDesc child) {
        return child.getDimensionSet().andNot(getDimensionSet()).isEmpty()
                && child.getMeasureSet().andNot(getMeasureSet()).isEmpty();
    }

    public List<MeasureDesc> getMeasureDescs() {
        Collection<NDataModel.Measure> measures = getOrderedMeasures().values();
        List<MeasureDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        result.addAll(measures);
        return result;
    }

    public NCuboidLayout getLastLayout() {
        List<NCuboidLayout> existing = getLayouts();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public ImmutableBiMap<Integer, NDataModel.Measure> getOrderedMeasures() {
        return orderedMeasures;
    }

    public ImmutableBitSet getDimensionSet() {
        return dimensionSet;
    }

    public ImmutableBitSet getMeasureSet() {
        return measureSet;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NCubePlan getCubePlan() {
        return cubePlan;
    }

    void setCubePlan(NCubePlan cubePlan) {
        checkIsNotCachedAndShared();
        this.cubePlan = cubePlan;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
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

    public List<NCuboidLayout> getLayouts() {
        return isCachedAndShared() ? ImmutableList.copyOf(layouts) : layouts;
    }

    public void setLayouts(List<NCuboidLayout> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public boolean isCachedAndShared() {
        return cubePlan == null ? false : cubePlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (cubePlan != null)
            cubePlan.checkIsNotCachedAndShared();
    }

}
