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

import static io.kyligence.kap.metadata.model.NDataModel.Measure;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.kylin.common.util.BitSets;
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
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class IndexEntity implements Serializable, IKeep {
    /**
     * Here suppose cuboid's number is not bigger than 1_000_000, so if the id is bigger than 1_000_000 * 1_000
     * means it should be a table index cuboid.
     */
    public static final long TABLE_INDEX_START_ID = 20_000_000_000L;
    public static final long INDEX_ID_STEP = 10000L;
    public static final long LAYOUT_ID_STEP = 1L;

    @JsonBackReference
    private IndexPlan indexPlan;

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
    private List<LayoutEntity> layouts = Lists.newArrayList();

    @Setter
    @Getter
    @JsonProperty("next_layout_offset")
    private long nextLayoutOffset = 1;

    // computed fields below
    @Getter(lazy = true)
    private final BiMap<Integer, TblColRef> effectiveDimCols = initEffectiveDimCols();

    private BiMap<Integer, TblColRef> initEffectiveDimCols() {
        return Maps.filterKeys(getModel().getEffectiveColsMap(),
                input -> input != null && getDimensionBitset().get(input));
    }

    @Getter(lazy = true)
    private final ImmutableBiMap<Integer, Measure> effectiveMeasures = initEffectiveMeasures();

    private ImmutableBiMap<Integer, Measure> initEffectiveMeasures() {
        val model = getModel();
        ImmutableBiMap.Builder<Integer, Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            if (model.getEffectiveMeasureMap().containsKey(m)) {
                measuresBuilder.put(m, model.getEffectiveMeasureMap().get(m));
            }
        }
        return measuresBuilder.build();
    }

    @Getter(lazy = true)
    private final ImmutableBitSet dimensionBitset = initDimensionBitset();

    private ImmutableBitSet initDimensionBitset() {
        return ImmutableBitSet.valueOf(dimensions);
    }

    @Getter(lazy = true)
    private final ImmutableBitSet measureBitset = initMeasureBitset();

    private ImmutableBitSet initMeasureBitset() {
        return ImmutableBitSet.valueOf(measures);
    }

    @Getter(lazy = true)
    private final ImmutableSet<TblColRef> dimensionSet = initDimensionSet();

    private ImmutableSet<TblColRef> initDimensionSet() {
        return ImmutableSet.copyOf(getEffectiveDimCols().values());
    }

    @Getter(lazy = true)
    private final ImmutableSet<Measure> measureSet = initMeasureSet();

    private ImmutableSet<Measure> initMeasureSet() {
        // TODO: all layouts' measure order must follow cuboid_desc's define ?
        return getEffectiveMeasures().values();
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

    public boolean fullyDerive(IndexEntity child) {
        // both table index or not.
        if (!this.isTableIndex() == child.isTableIndex()) {
            return false;
        }

        if (totalFieldSize(child) >= totalFieldSize(this)) {
            return false;
        }

        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty()
                && child.getMeasureBitset().andNot(getMeasureBitset()).isEmpty();
    }

    private int totalFieldSize(IndexEntity entity) {
        return entity.getDimensions().size() + entity.getMeasures().size();
    }

    public LayoutEntity getLastLayout() {
        List<LayoutEntity> existing = getLayouts();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }

    public NDataModel getModel() {
        return indexPlan.getModel();
    }

    public void setIndexPlan(IndexPlan indexPlan) {
        checkIsNotCachedAndShared();
        this.indexPlan = indexPlan;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
    }

    /**
     * If there is no need to consider the order of dimensions,
     * please use getDimensionBitset() instead of this method.
     */
    public List<Integer> getDimensions() {
        return getColIds(dimensions);
    }

    public void setDimensions(List<Integer> dimensions) {
        checkIsNotCachedAndShared();
        this.dimensions = dimensions;
    }

    /**
     * If there is no need to consider the order of measures,
     * please use getDimensionBitset() instead of this method.
     */
    public List<Integer> getMeasures() {
        return getColIds(measures);
    }

    public void setMeasures(List<Integer> measures) {
        checkIsNotCachedAndShared();
        this.measures = measures;
    }

    private List<Integer> getColIds(List<Integer> cols) {
        return isCachedAndShared() ? Lists.newArrayList(cols) : cols;
    }

    public List<LayoutEntity> getLayouts() {
        return isCachedAndShared() ? ImmutableList.copyOf(layouts) : layouts;
    }

    public void setLayouts(List<LayoutEntity> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public boolean isCachedAndShared() {
        return indexPlan != null && indexPlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (indexPlan != null)
            indexPlan.checkIsNotCachedAndShared();
    }

    public boolean isTableIndex() {
        return id >= TABLE_INDEX_START_ID;
    }

    void removeLayoutsInCuboid(List<LayoutEntity> deprecatedLayouts, Predicate<LayoutEntity> isSkip,
            BiPredicate<LayoutEntity, LayoutEntity> equal, boolean deleteAuto, boolean deleteManual) {
        checkIsNotCachedAndShared();
        List<LayoutEntity> toRemoveLayouts = Lists.newArrayList();
        for (LayoutEntity cuboidLayout : deprecatedLayouts) {
            if (isSkip != null && isSkip.test(cuboidLayout)) {
                continue;
            }
            LayoutEntity toRemoveLayout = getLayouts().stream()
                    .filter(originLayout -> equal.test(originLayout, cuboidLayout)).findFirst().orElse(null);
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
    // IndexIdentifier used for auto-modeling
    // ============================================================================

    @Override
    public String toString() {
        return "IndexEntity{ Id=" + id + ", dimBitSet=" + getDimensionBitset() + ", measureBitSet=" + getMeasureBitset()
                + "}.";
    }

    public static class IndexIdentifier {
        BitSet dimBitSet;
        BitSet measureBitSet;
        boolean isTableIndex;

        public IndexIdentifier(BitSet dimBitSet, BitSet measureBitSet, boolean isTableIndex) {
            this(dimBitSet, measureBitSet);
            this.isTableIndex = isTableIndex;
        }

        IndexIdentifier(BitSet dimBitSet, BitSet measureBitSet) {
            this.dimBitSet = dimBitSet;
            this.measureBitSet = measureBitSet;
        }

        @Override
        public String toString() {
            return "IndexEntity{" + "dimBitSet=" + dimBitSet + ", measureBitSet=" + measureBitSet + ", isTableIndex="
                    + isTableIndex + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            IndexIdentifier that = (IndexIdentifier) o;
            return isTableIndex == that.isTableIndex && Objects.equals(dimBitSet, that.dimBitSet)
                    && Objects.equals(measureBitSet, that.measureBitSet);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dimBitSet, measureBitSet, isTableIndex);
        }
    }

    public IndexIdentifier createIndexIdentifier() {
        return new IndexIdentifier(//
                BitSets.valueOf(getDimensions()), //
                BitSets.valueOf(getMeasures()), //
                isTableIndex()//
        );
    }

}