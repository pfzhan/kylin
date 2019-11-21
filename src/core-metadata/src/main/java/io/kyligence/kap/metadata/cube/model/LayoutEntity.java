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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.MapUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class LayoutEntity implements IStorageAware, Serializable, IKeep {
    @JsonBackReference
    private IndexEntity index;

    @JsonProperty("id")
    private long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @EqualsAndHashCode.Include
    @JsonProperty("col_order")
    private List<Integer> colOrder = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("layout_override_indexes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, String> layoutOverrideIndexes = Maps.newHashMap();

    @EqualsAndHashCode.Include
    @JsonProperty("shard_by_columns")
    private List<Integer> shardByColumns = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("sort_by_columns")
    private List<Integer> sortByColumns = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_NDATA_STORAGE;

    @JsonProperty("update_time")
    private long updateTime;

    @JsonProperty("manual")
    private boolean isManual = false;

    @JsonProperty("auto")
    private boolean isAuto = false;

    @Setter
    @JsonProperty("draft_version")
    private String draftVersion;

    @Setter
    private boolean isInProposing; // only used in the process of propose

    // computed fields below

    /**
     * https://stackoverflow.com/questions/3810738/google-collections-immutablemap-iteration-order
     * <p>
     * the ImmutableMap factory methods and builder return instances that follow the iteration order of the inputs provided when the map in constructed.
     * However, an ImmutableSortedMap, which is a subclass of ImmutableMap. sorts the keys.
     */
    private ImmutableBiMap<Integer, TblColRef> orderedDimensions;
    private ImmutableBiMap<Integer, Measure> orderedMeasures;

    public LayoutEntity() {
        // Only used by Jackson
    }

    public ImmutableBiMap<Integer, TblColRef> getOrderedDimensions() { // dimension order abides by rowkey_col_desc
        if (orderedDimensions != null)
            return orderedDimensions;

        synchronized (this) {
            if (orderedDimensions != null)
                return orderedDimensions;

            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();

            for (int colId : colOrder) {
                if (colId < NDataModel.MEASURE_ID_BASE)
                    dimsBuilder.put(colId, index.getEffectiveDimCols().get(colId));
            }

            orderedDimensions = dimsBuilder.build();
            return orderedDimensions;
        }
    }

    public ImmutableBiMap<Integer, Measure> getOrderedMeasures() { // measure order abides by column family
        if (orderedMeasures != null)
            return orderedMeasures;

        synchronized (this) {
            if (orderedMeasures != null)
                return orderedMeasures;

            ImmutableBiMap.Builder<Integer, Measure> measureBuilder = ImmutableBiMap.builder();

            for (int colId : colOrder) {
                if (colId >= NDataModel.MEASURE_ID_BASE)
                    measureBuilder.put(colId, index.getEffectiveMeasures().get(colId));
            }

            orderedMeasures = measureBuilder.build();
            return orderedMeasures;
        }
    }

    public String getColIndexType(int colId) {
        return MapUtil.getOrElse(this.layoutOverrideIndexes, colId,
                MapUtil.getOrElse(getIndex().getIndexPlan().getIndexPlanOverrideIndexes(), colId, "eq"));
    }

    public Integer getDimensionPos(TblColRef tblColRef) {
        return getOrderedDimensions().inverse().get(tblColRef);
    }

    public List<TblColRef> getColumns() {
        return Lists.newArrayList(getOrderedDimensions().values());
    }

    public boolean isExtendedColumn(TblColRef tblColRef) {
        return false; // TODO: enable derived
    }

    public Set<TblColRef> getShardByColumnRefs() {
        Set<TblColRef> colRefs = Sets.newHashSetWithExpectedSize(shardByColumns.size());
        for (int c : shardByColumns) {
            colRefs.add(getOrderedDimensions().get(c));
        }
        return colRefs;
    }

    public NDataModel getModel() {
        return index.getIndexPlan().getModel();
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
    }

    public long getIndexId() {
        return this.getId() - this.getId() % IndexEntity.INDEX_ID_STEP;
    }

    public ImmutableList<Integer> getColOrder() {
        return ImmutableList.copyOf(colOrder);
    }

    public void setColOrder(List<Integer> l) {
        checkIsNotCachedAndShared();
        this.colOrder = l;
    }

    public ImmutableMap<Integer, String> getLayoutOverrideIndexes() {
        return ImmutableMap.copyOf(this.layoutOverrideIndexes);
    }

    public void setLayoutOverrideIndexes(Map<Integer, String> m) {
        checkIsNotCachedAndShared();
        this.layoutOverrideIndexes = m;
    }

    public List<Integer> getShardByColumns() {
        return isCachedAndShared() ? Lists.newArrayList(shardByColumns) : shardByColumns;
    }

    public void setShardByColumns(List<Integer> shardByColumns) {
        checkIsNotCachedAndShared();
        this.shardByColumns = shardByColumns;
    }

    public List<Integer> getSortByColumns() {
        return isCachedAndShared() ? Lists.newArrayList(sortByColumns) : sortByColumns;
    }

    public void setSortByColumns(List<Integer> sortByColumns) {
        checkIsNotCachedAndShared();
        this.sortByColumns = sortByColumns;
    }

    public void setStorageType(int storageType) {
        checkIsNotCachedAndShared();
        this.storageType = storageType;
    }

    public void setIndex(IndexEntity index) {
        checkIsNotCachedAndShared();
        this.index = index;
    }

    public void setUpdateTime(long updateTime) {
        checkIsNotCachedAndShared();
        this.updateTime = updateTime;
    }

    public void setName(String name) {
        checkIsNotCachedAndShared();
        this.name = name;
    }

    public void setOwner(String owner) {
        checkIsNotCachedAndShared();
        this.owner = owner;
    }

    public void setManual(boolean manual) {
        checkIsNotCachedAndShared();
        isManual = manual;
    }

    public void setAuto(boolean auto) {
        checkIsNotCachedAndShared();
        isAuto = auto;
    }

    public boolean isExpired() {
        return !isAuto && !isManual;
    }

    public boolean isCachedAndShared() {
        return index != null && index.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (index != null)
            index.checkIsNotCachedAndShared();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id).toString();
    }

    public boolean isDraft() {
        return this.draftVersion != null;
    }

    public void publish() {
        this.draftVersion = null;
    }

    public boolean matchDraftVersion(String draftVersion) {
        return isDraft() && this.draftVersion.equals(draftVersion);
    }


}
