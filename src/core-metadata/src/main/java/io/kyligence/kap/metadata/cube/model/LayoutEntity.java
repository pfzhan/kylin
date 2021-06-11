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

import static io.kyligence.kap.metadata.cube.model.IndexEntity.INDEX_ID_STEP;
import static io.kyligence.kap.metadata.cube.model.IndexEntity.TABLE_INDEX_START_ID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.MapUtil;
import io.kyligence.kap.guava20.shaded.common.base.MoreObjects;
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

    /** inital id for table or agg
     */
    public static final long TABLE_LAYOUT_INIT_ID = 40_000_000_001L;
    public static final long AGG_LAYOUT_INIT_ID = TABLE_INDEX_START_ID - INDEX_ID_STEP + 1;

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

    @JsonProperty("partition_by_columns")
    private List<Integer> partitionByColumns = Lists.newArrayList();// Current case auto and manual are same partition columns

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

    @JsonProperty("base")
    private boolean isBase = false;

    @Setter
    @JsonProperty("draft_version")
    private String draftVersion;

    @Setter
    private boolean isInProposing; // only used in the process of propose

    @Setter
    @JsonProperty("index_range")
    private IndexEntity.Range indexRange;

    // computed fields below

    /**
     * https://stackoverflow.com/questions/3810738/google-collections-immutablemap-iteration-order
     * <p>
     * the ImmutableMap factory methods and builder return instances that follow the iteration order of the inputs provided when the map in constructed.
     * However, an ImmutableSortedMap, which is a subclass of ImmutableMap. sorts the keys.
     */
    private ImmutableBiMap<Integer, TblColRef> orderedDimensions;
    private ImmutableBiMap<Integer, Measure> orderedMeasures;

    @Setter
    @Getter
    private boolean toBeDeleted = false;

    public LayoutEntity() {
        // Only used by Jackson
    }

    public ImmutableList<Integer> getDimsIds() {
        ImmutableList.Builder<Integer> dimsBuilder = ImmutableList.<Integer> builder();
        for (int colId : colOrder) {
            if (colId < NDataModel.MEASURE_ID_BASE)
                dimsBuilder.add(colId);
        }
        return dimsBuilder.build();
    }

    public ImmutableList<Integer> getMeasureIds() {
        ImmutableList.Builder<Integer> measureIdBuilder = ImmutableList.<Integer> builder();
        for (int colId : colOrder) {
            if (colId >= NDataModel.MEASURE_ID_BASE)
                measureIdBuilder.add(colId);
        }
        return measureIdBuilder.build();
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

    public List<TblColRef> getStreamingColumns() {
        NDataModel model = getModel();
        if (model.isFusionModel()) {
            IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject()).getIndexPlan(model.getFusionId());
            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();
            for (int colId : colOrder) {
                if (colId < NDataModel.MEASURE_ID_BASE)
                    dimsBuilder.put(colId, indexPlan.getEffectiveDimCols().get(colId));
            }

            return Lists.newArrayList(dimsBuilder.build().values());
        } else {
            return Lists.newArrayList();
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

    public List<Measure> getStreamingMeasures() {
        NDataModel model = getModel();
        if (model.isFusionModel()) {
            IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject()).getIndexPlan(model.getFusionId());
            ImmutableBiMap.Builder<Integer, Measure> measuresBuilder = ImmutableBiMap.builder();
            for (int colId : colOrder) {
                if (colId >= NDataModel.MEASURE_ID_BASE)
                    measuresBuilder.put(colId, indexPlan.getEffectiveMeasures().get(colId));
            }

            return Lists.newArrayList(measuresBuilder.build().values());
        } else {
            return Lists.newArrayList();
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

    public List<String> listBitmapMeasure() {
        List<String> countDistinct = new ArrayList<>();
        getOrderedMeasures().forEach((a, b) -> {
            if ("bitmap".equals(b.getFunction().getReturnDataType().getName())) {
                countDistinct.add(a.toString());
            }
        });
        return countDistinct;
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
        return this.getId() - this.getId() % INDEX_ID_STEP;
    }

    public ImmutableList<Integer> getColOrder() {
        return ImmutableList.copyOf(colOrder);
    }

    public boolean equalsCols(LayoutEntity layout) {
        Set<Integer> order1 = ImmutableSortedSet.<Integer> naturalOrder().addAll(getColOrder()).build();
        Set<Integer> order2 = ImmutableSortedSet.<Integer> naturalOrder().addAll(layout.getColOrder()).build();
        return order1.equals(order2);
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

    public List<Integer> getPartitionByColumns() {
        return isCachedAndShared() ? Lists.newArrayList(partitionByColumns) : partitionByColumns;
    }

    public void setPartitionByColumns(List<Integer> partitionByColumns) {
        checkIsNotCachedAndShared();
        this.partitionByColumns = partitionByColumns;
    }

    public List<Integer> getSortByColumns() {
        return isCachedAndShared() ? Lists.newArrayList(sortByColumns) : sortByColumns;
    }

    public int getBucketNum() {
        return this.getIndex().getIndexPlan().getLayoutBucketNumMapping().get(id);
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
        return MoreObjects.toStringHelper(this).add("id", id).toString();
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

    public String genUniqueContent() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("colOrder=").append(getColOrder().toString());
        sb.append(",sortCols=").append(getSortByColumns().toString());
        sb.append(",shardCols=").append(getShardByColumns().toString());
        sb.append("}");
        return sb.toString();
    }

    public boolean isBaseIndex() {
        return isBase;
    }

    public void initalId(boolean isAgg) {
        id = isAgg ? AGG_LAYOUT_INIT_ID : TABLE_LAYOUT_INIT_ID;
    }

    public boolean notAssignId() {
        return id == AGG_LAYOUT_INIT_ID || id == TABLE_LAYOUT_INIT_ID;
    }

    public void setBase(boolean base) {
        checkIsNotCachedAndShared();
        isBase = base;
        if(!base){
            setAuto(true);
        }
    }
}
