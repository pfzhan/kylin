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

import static io.kyligence.kap.metadata.model.NDataModel.Measure;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.EqualsAndHashCode;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class NCuboidDesc implements Serializable, IKeep {
    /**
     * Here suppose cuboid's number is not bigger than 1_000_000, so if the id is bigger than 1_000_000 * 1_000
     * means it should be a table index cuboid.
     */
    public static final long TABLE_INDEX_START_ID = 20_000_000_000L;
    public static final long CUBOID_DESC_ID_STEP = 1000L;
    public static final long CUBOID_LAYOUT_ID_STEP = 1L;

    @JsonBackReference
    private NCubePlan cubePlan;

    @EqualsAndHashCode.Include
    @JsonProperty("id")
    private long id;

    @EqualsAndHashCode.Include
    @JsonProperty("dimensions")
    private List<Integer> dimensions = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("measures")
    private List<Integer> measures = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonManagedReference
    @JsonProperty("layouts")
    private List<NCuboidLayout> layouts = Lists.newArrayList();

    // computed fields below

    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private ImmutableBiMap<Integer, Measure> effectiveMeasures;
    private ImmutableBitSet dimensionBitset = null;
    private ImmutableBitSet measureBitset = null;
    private ImmutableSet<TblColRef> dimensionSet = null;
    private ImmutableSet<Measure> measureSet = null;

    void init() {
        NDataModel model = getModel();
        this.dimensionBitset = ImmutableBitSet.valueOf(dimensions);
        this.measureBitset = ImmutableBitSet.valueOf(measures);

        this.effectiveDimCols = Maps.filterKeys(model.getEffectiveColsMap(),
                input -> input != null && dimensionBitset.get(input));

        this.dimensionSet = ImmutableSet.copyOf(this.effectiveDimCols.values());

        // TODO: all layouts' measure order must follow cuboid_desc's define ?
        ImmutableBiMap.Builder<Integer, Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            if (model.getEffectiveMeasureMap().containsKey(m)) {
                measuresBuilder.put(m, model.getEffectiveMeasureMap().get(m));
            }
        }
        this.effectiveMeasures = measuresBuilder.build();
        this.measureSet = effectiveMeasures.values();
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

    public boolean dimensionDerive(NCuboidDesc child) {
        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty();
    }

    public boolean fullyDerive(NCuboidDesc child) {
        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty()
                && child.getMeasureBitset().andNot(getMeasureBitset()).isEmpty();
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

    public ImmutableBiMap<Integer, Measure> getEffectiveMeasures() {
        return effectiveMeasures;
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

    public ImmutableSet<Measure> getMeasureSet() {
        return measureSet;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
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

    public List<NCuboidLayout> getLayouts() {
        return isCachedAndShared() ? ImmutableList.copyOf(layouts) : layouts;
    }

    public void setLayouts(List<NCuboidLayout> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public boolean isCachedAndShared() {
        return cubePlan != null && cubePlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (cubePlan != null)
            cubePlan.checkIsNotCachedAndShared();
    }

    public boolean bothTableIndexOrNot(NCuboidDesc another) {
        return this.isTableIndex() == another.isTableIndex();
    }

    public boolean isTableIndex() {
        return id >= TABLE_INDEX_START_ID;
    }

    void removeLayoutsInCuboid(List<NCuboidLayout> deprecatedLayouts, Predicate<NCuboidLayout> isSkip,
            Predicate2<NCuboidLayout, NCuboidLayout> equal, boolean deleteAuto, boolean deleteManual) {
        checkIsNotCachedAndShared();
        List<NCuboidLayout> toRemoveLayouts = Lists.newArrayList();
        for (NCuboidLayout cuboidLayout : deprecatedLayouts) {
            if (isSkip != null && isSkip.test(cuboidLayout)) {
                continue;
            }
            NCuboidLayout toRemoveLayout = getLayouts().stream()
                    .filter(originLayout -> equal.apply(originLayout, cuboidLayout)).findFirst().orElse(null);
            if (toRemoveLayout != null) {
                if (deleteAuto) {
                    toRemoveLayout.setAuto(false);
                }
                if (deleteManual) {
                    toRemoveLayout.setManual(false);
                }
                if (toRemoveLayout.isExpired()) {
                    toRemoveLayouts.add(toRemoveLayout);
                }
            }
        }
        getLayouts().removeAll(toRemoveLayouts);
    }

    // ============================================================================
    // NCuboidIdentifier used for auto-modeling
    // ============================================================================

    public static class NCuboidIdentifier {
        BitSet dimBitSet;
        BitSet measureBitSet;
        boolean isTableIndex;

        NCuboidIdentifier(BitSet dimBitSet, BitSet measureBitSet, boolean isTableIndex) {
            this(dimBitSet, measureBitSet);
            this.isTableIndex = isTableIndex;
        }

        NCuboidIdentifier(BitSet dimBitSet, BitSet measureBitSet) {
            this.dimBitSet = dimBitSet;
            this.measureBitSet = measureBitSet;
        }

        @Override
        public String toString() {
            return "CuboidToken{" + "dimBitSet=" + dimBitSet + ", measureBitSet=" + measureBitSet + ", isTableIndex="
                    + isTableIndex + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            NCuboidIdentifier that = (NCuboidIdentifier) o;
            return isTableIndex == that.isTableIndex && Objects.equals(dimBitSet, that.dimBitSet)
                    && Objects.equals(measureBitSet, that.measureBitSet);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dimBitSet, measureBitSet, isTableIndex);
        }
    }

    public NCuboidIdentifier createCuboidIdentifier() {
        return new NCuboidIdentifier(//
                ImmutableBitSet.valueOf(getDimensions()).mutable(), //
                ImmutableBitSet.valueOf(getMeasures()).mutable(), //
                isTableIndex()//
        );
    }

}