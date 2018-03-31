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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NCuboidLayout implements IStorageAware, Serializable, IKeep {
    @JsonBackReference
    private NCuboidDesc cuboidDesc;

    @JsonProperty("id")
    private long id;
    @JsonProperty("rowkeys")
    private NRowkeyColumnDesc[] rowkeys = new NRowkeyColumnDesc[0];
    @JsonProperty("dim_cf")
    private NColumnFamilyDesc.DimensionCF[] dimensionCFs = new NColumnFamilyDesc.DimensionCF[0];
    @JsonProperty("measure_cf")
    private NColumnFamilyDesc.MeasureCF[] measureCFs = new NColumnFamilyDesc.MeasureCF[0];
    @JsonProperty("shard_by_columns")
    private int[] shardByColumns = new int[0];
    @JsonProperty("sort_by_columns")
    private int[] sortByColumns = new int[0];
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_HBASE;

    // computed fields below

    private ImmutableBiMap<Integer, TblColRef> orderedDimensions;
    private Map<Integer, String> dimensionIndexMap;
    private Map<Integer, Integer> dimensionPosMap;

    public NCuboidLayout() {
    }

    public ImmutableBiMap<Integer, TblColRef> getOrderedDimensions() { // dimension order abides by rowkey_col_desc
        if (orderedDimensions != null)
            return orderedDimensions;

        synchronized (this) {
            if (orderedDimensions != null)
                return orderedDimensions;

            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();
            for (NRowkeyColumnDesc rowkeyColDesc : rowkeys) {
                dimsBuilder.put(rowkeyColDesc.getDimensionId(),
                        cuboidDesc.getEffectiveDimCols().get(rowkeyColDesc.getDimensionId()));
            }
            orderedDimensions = dimsBuilder.build();
            return orderedDimensions;
        }
    }

    public ImmutableBiMap<Integer, NDataModel.Measure> getOrderedMeasures() { // measure order abides by column family
        return cuboidDesc.getOrderedMeasures();
    }

    public Map<Integer, String> getDimensionIndexMap() {
        if (dimensionIndexMap != null)
            return dimensionIndexMap;

        synchronized (this) {
            if (dimensionIndexMap != null)
                return dimensionIndexMap;

            dimensionIndexMap = Maps.newHashMapWithExpectedSize(rowkeys.length);
            for (NRowkeyColumnDesc rowkey : rowkeys) {
                dimensionIndexMap.put(rowkey.getDimensionId(), rowkey.getIndex());
            }
            return dimensionIndexMap;
        }
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
        Set<TblColRef> colRefs = Sets.newHashSetWithExpectedSize(shardByColumns.length);
        for (int c : shardByColumns) {
            colRefs.add(getOrderedDimensions().get(c));
        }
        return colRefs;
    }

    public NDataModel getModel() {
        return cuboidDesc.getCubePlan().getModel();
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public long getId() {
        return id;
    }

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
    }

    public NRowkeyColumnDesc[] getRowkeyColumns() {
        return isCachedAndShared() ? Arrays.copyOf(rowkeys, rowkeys.length) : rowkeys;
    }

    public void setRowkeyColumns(NRowkeyColumnDesc[] rowkeys) {
        checkIsNotCachedAndShared();
        this.rowkeys = rowkeys;
    }

    public int[] getShardByColumns() {
        return isCachedAndShared() ? Arrays.copyOf(shardByColumns, shardByColumns.length) : shardByColumns;
    }

    public void setShardByColumns(int[] shardByColumns) {
        checkIsNotCachedAndShared();
        this.shardByColumns = shardByColumns;
    }

    public int[] getSortByColumns() {
        return isCachedAndShared() ? Arrays.copyOf(sortByColumns, sortByColumns.length) : sortByColumns;
    }

    public void setSortByColumns(int[] sortByColumns) {
        checkIsNotCachedAndShared();
        this.sortByColumns = sortByColumns;
    }

    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        checkIsNotCachedAndShared();
        this.storageType = storageType;
    }

    public NCuboidDesc getCuboidDesc() {
        return cuboidDesc;
    }

    public void setCuboidDesc(NCuboidDesc cuboidDesc) {
        checkIsNotCachedAndShared();
        this.cuboidDesc = cuboidDesc;
    }

    public NColumnFamilyDesc.DimensionCF[] getDimensionCFs() {
        return isCachedAndShared() ? Arrays.copyOf(dimensionCFs, dimensionCFs.length) : dimensionCFs;
    }

    public void setDimensionCFs(NColumnFamilyDesc.DimensionCF[] dimensionCFs) {
        checkIsNotCachedAndShared();
        this.dimensionCFs = dimensionCFs;
    }

    public NColumnFamilyDesc.MeasureCF[] getMeasureCFs() {
        return isCachedAndShared() ? Arrays.copyOf(measureCFs, measureCFs.length) : measureCFs;
    }

    public void setMeasureCFs(NColumnFamilyDesc.MeasureCF[] measureCFs) {
        checkIsNotCachedAndShared();
        this.measureCFs = measureCFs;
    }

    public boolean isCachedAndShared() {
        return cuboidDesc == null ? false : cuboidDesc.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (cuboidDesc != null)
            cuboidDesc.checkIsNotCachedAndShared();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id).toString();
    }
}
