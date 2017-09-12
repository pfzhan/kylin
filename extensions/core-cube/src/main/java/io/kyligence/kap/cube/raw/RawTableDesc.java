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

package io.kyligence.kap.cube.raw;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity implements IEngineAware {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RawTableDesc.class);
    public static final String RAW_TABLE_DESC_RESOURCE_ROOT = "/raw_table_desc";

    @JsonProperty("name")
    private String name;
    @JsonProperty("is_draft")
    private boolean isDraft;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("columns")
    private List<RawTableColumnDesc> columns = new ArrayList<>();
    @JsonProperty("engine_type")
    private int engineType;
    @JsonProperty("storage_type")
    private int storageType;
    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @JsonProperty("raw_table_mapping")
    private RawTableMappingDesc rawTableMapping;

    // computed
    private KylinConfig config;
    private DataModelDesc model;
    private Map<TblColRef, RawTableColumnDesc> columnMap;
    private LinkedHashSet<TblColRef> shardbyColumns;
    private Set<TblColRef> fuzzyColumns;
    private LinkedHashSet<TblColRef> sortbyColumns;
    private RawToGridTableMapping rawToGTMapping;
    private LinkedHashSet<RawTableColumnDesc> sortbyColumnDescs;

    private List<TblColRef> columnsInOrder;
    private List<RawTableColumnDesc> columnDescsInOrder;

    private Map<Integer, Integer> origin2OrderMapping;

    // for Jackson
    public RawTableDesc() {
    }

    public static RawTableDesc getCopyOf(RawTableDesc desc) {
        RawTableDesc rawTableDesc = new RawTableDesc();
        rawTableDesc.setName(desc.getName());
        rawTableDesc.setDraft(desc.isDraft());
        rawTableDesc.setModelName(desc.getModelName());
        rawTableDesc.setOriginColumns(desc.getOriginColumns());
        rawTableDesc.setAutoMergeTimeRanges(desc.getAutoMergeTimeRanges());
        rawTableDesc.setRawTableMapping(desc.rawTableMapping);
        rawTableDesc.setEngineType(desc.getEngineType());
        rawTableDesc.setStorageType(desc.getStorageType());
        rawTableDesc.updateRandomUuid();
        rawTableDesc.init(desc.getConfig());
        return rawTableDesc;
    }

    public Set<TblColRef> getFuzzyColumns() {
        return fuzzyColumns;
    }

    // validate the first sortby column is data/time/integerDim encoding
    public void validate() {
        if (sortbyColumns.isEmpty()) {
            throw new IllegalStateException(this + " missing sortby column");
        }

        TblColRef firstSorted = sortbyColumns.iterator().next();

        // FIXME: Dirty code, check encoding in string
        String encoding = columnMap.get(firstSorted).getEncoding();
        if (!encoding.startsWith("integer") && !encoding.equalsIgnoreCase("date")
                && !encoding.equalsIgnoreCase("time")) {
            throw new IllegalStateException(
                    "first sortby column's encoding is" + encoding + ", it should be integer, date or time");
        }

        if (shardbyColumns.size() > 1) {
            throw new IllegalStateException(
                    "Only one shardby column is supported. Now shardby columns are " + shardbyColumns);
        }
    }

    public TblColRef getFirstSortbyColumn() {
        if (sortbyColumns.size() < 1) {
            throw new IllegalStateException(this + " missing sortby column");
        }

        return sortbyColumns.iterator().next();
    }

    public Collection<TblColRef> getSortbyColumns() {
        if (sortbyColumns.size() < 1) {
            throw new IllegalStateException(this + " missing sortby column");
        }

        return sortbyColumns;
    }

    public List<TblColRef> getNonSortbyColumns() {
        List<TblColRef> cols = new ArrayList<>();
        for (RawTableColumnDesc colDesc : columns) {
            if (sortbyColumns.contains(colDesc.getColumn()))
                continue;
            cols.add(colDesc.getColumn());
        }
        return cols;
    }
    
    public Collection<RawTableColumnDesc> getSortbyColumnDescs() {
        if (sortbyColumnDescs.size() < 1) {
            throw new IllegalStateException(this + " missing sortby column");
        }

        return sortbyColumnDescs;
    }

    public List<RawTableColumnDesc> getNonSortbyColumnDescs() {
        List<RawTableColumnDesc> cols = new ArrayList<>();
        for (RawTableColumnDesc colDesc : columns) {
            if (sortbyColumnDescs.contains(colDesc))
                continue;
            cols.add(colDesc);
        }
        return cols;
    }

    public Boolean isShardby(TblColRef colRef) {
        if (shardbyColumns.isEmpty()) {
            return true;
        }
        return shardbyColumns.contains(colRef);
    }

    public Boolean isNeedFuzzyIndex(TblColRef colRef) {
        return fuzzyColumns.contains(colRef);
    }

    public int getEstimateRowSize() {
        int size = 0;
        for (RawTableColumnDesc col : columns) {
            size += col.getColumn().getType().getStorageBytesEstimate();
        }
        return size;
    }

    public Boolean isSortby(TblColRef colRef) {
        return sortbyColumns.contains(colRef);
    }

    public List<Pair<String, Integer>> getEncodings() {
        List<TblColRef> columnsInOrder = getColumnsInOrder();
        Preconditions.checkNotNull(columnsInOrder);
        Preconditions.checkArgument(columnsInOrder.size() != 0);
        return Lists.transform(columnsInOrder, new Function<TblColRef, Pair<String, Integer>>() {
            @Nullable
            @Override
            public Pair<String, Integer> apply(@Nullable TblColRef input) {
                RawTableColumnDesc rawTableColumnDesc = columnMap.get(input);
                Preconditions.checkNotNull(rawTableColumnDesc);
                return Pair.newPair(rawTableColumnDesc.getEncoding(), rawTableColumnDesc.getEncodingVersion());
            }
        });
    }

    public List<Pair<String, Integer>> getOriginEncodings() {
        List<RawTableColumnDesc> columnsOrigin = getOriginColumns();
        Preconditions.checkNotNull(columnsOrigin);
        Preconditions.checkArgument(columnsOrigin.size() != 0);

        return Lists.transform(columnsOrigin, new Function<RawTableColumnDesc, Pair<String, Integer>>() {
            @Nullable
            @Override
            public Pair<String, Integer> apply(@Nullable RawTableColumnDesc input) {
                Preconditions.checkNotNull(input);
                return Pair.newPair(input.getEncoding(), input.getEncodingVersion());
            }
        });
    }

    public List<RawTableColumnDesc> getOriginColumns() {
        return this.columns == null ? null : Collections.unmodifiableList(this.columns);
    }

    public void setOriginColumns(List<RawTableColumnDesc> columns) {
        this.columns = columns;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
        return RAW_TABLE_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public List<TblColRef> getColumnsInOrder() {
        if (columnsInOrder == null) {
            columnsInOrder = Lists.newArrayList();
            columnsInOrder.addAll(getSortbyColumns());
            columnsInOrder.addAll(getNonSortbyColumns());
        }
        return columnsInOrder;
    }
    
    public List<RawTableColumnDesc> getColumnDescsInOrder() {
        if (columnDescsInOrder == null) {
            columnDescsInOrder = Lists.newArrayList();
            columnDescsInOrder.addAll(getSortbyColumnDescs());
            columnDescsInOrder.addAll(getNonSortbyColumnDescs());
        }
        return columnDescsInOrder;
    }

    // ============================================================================

    public KylinConfig getConfig() {
        return config;
    }

    public DataModelDesc getModel() {
        return model;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDraft() {
        return isDraft;
    }

    public void setDraft(boolean isDraft) {
        this.isDraft = isDraft;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        RawTableDesc other = (RawTableDesc) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    public long[] getAutoMergeTimeRanges() {
        return autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(long[] autoMergeTimeRanges) {
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    @Override
    public String toString() {
        return "RawTableDesc [name=" + name + "]";
    }

    @Override
    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        this.engineType = engineType;
    }

    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType = storageType;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public RawTableMappingDesc getRawTableMapping() {
        return rawTableMapping;
    }

    public void setRawTableMapping(RawTableMappingDesc rawTableMapping) {
        this.rawTableMapping = rawTableMapping;
    }

    // if no shardby columns set, all columns as shardby
    public Collection<TblColRef> getShardbyColumns() {
        if (shardbyColumns.isEmpty()) {
            return getColumnsInOrder();
        }
        return shardbyColumns;
    }

    void init(KylinConfig config) {
        MetadataManager metaMgr = MetadataManager.getInstance(config);

        this.config = config;
        this.model = metaMgr.getDataModelDesc(modelName);
        this.columnMap = Maps.newHashMap();
        this.shardbyColumns = new LinkedHashSet<>();
        this.fuzzyColumns = Sets.newHashSet();
        this.sortbyColumns = new LinkedHashSet<>();
        this.origin2OrderMapping = new HashMap<>();
        this.sortbyColumnDescs = new LinkedHashSet<>();

        for (RawTableColumnDesc colDesc : columns) {
            colDesc.init(model);
            if (colDesc.isShardby()) {
                shardbyColumns.add(colDesc.getColumn());
            }
            if (colDesc.getFuzzyIndex()) {
                fuzzyColumns.add(colDesc.getColumn());
            }
            if (colDesc.isSortby()) {
                sortbyColumns.add(colDesc.getColumn());
                sortbyColumnDescs.add(colDesc);
            }
            columnMap.put(colDesc.getColumn(), colDesc);
        }

        int colIdx = 0;
        int sortbyIdx = 0;
        int nonSortbyIdx = this.sortbyColumns.size();

        for (RawTableColumnDesc colDesc : columns) {
            if (colDesc.isSortby()) {
                this.origin2OrderMapping.put(colIdx, sortbyIdx);
                sortbyIdx++;
            } else {
                this.origin2OrderMapping.put(colIdx, nonSortbyIdx);
                nonSortbyIdx++;
            }
            colIdx++;
        }

        if (rawTableMapping != null) {
            rawTableMapping.init(this);
        } else {
            // For raw tables with no info about raw table mapping: 
            // distribute all columns into separate column families since legacy raw table do not handle raw table mapping.  
            rawTableMapping = new RawTableMappingDesc();
            rawTableMapping.initAsSeparatedColumns(this);
        }
        initColumnReferenceToColumnFamily();

        this.validate();
    }

    public RawToGridTableMapping getRawToGridTableMapping() {
        if (rawToGTMapping == null) {
            rawToGTMapping = new RawToGridTableMapping(this);
        }
        return rawToGTMapping;
    }

    public Map<Integer, Integer> getOrigin2OrderMapping() {
        return origin2OrderMapping;
    }

    private void initColumnReferenceToColumnFamily() {
        if (columns == null || columns.size() == 0)
            return;

        Map<String, RawTableColumnDesc> columnLookup = new HashMap<String, RawTableColumnDesc>();
        for (RawTableColumnDesc c : columns) {
            columnLookup.put(c.getName(), c);
        }
        Map<String, Integer> columnIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < columns.size(); i++)
            columnIndexLookup.put(columns.get(i).getName(), i);

        BitSet checkEachColumnExist = new BitSet();
        Set<String> columnSet = Sets.newHashSet();
        for (RawTableColumnFamilyDesc cf : getRawTableMapping().getColumnFamily()) {
            String[] columnRefs = cf.getColumnRefs();
            RawTableColumnDesc[] columnDescs = new RawTableColumnDesc[columnRefs.length];
            int[] columnIndex = new int[columnRefs.length];
            int lastColumnIndex = -1;
            for (int i = 0; i < columnRefs.length; i++) {
                columnDescs[i] = columnLookup.get(columnRefs[i]);
                checkState(columnDescs[i] != null, "column desc at (%s) is null", i);
                columnIndex[i] = columnIndexLookup.get(columnRefs[i]);
                checkState(columnIndex[i] >= 0, "column index at (%s) not positive", i);                
                
                checkState(!columnSet.contains(columnRefs[i]), "column (%s) duplicates", columnRefs[i]);
                columnSet.add(columnRefs[i]);
                
                checkState(columnIndex[i] > lastColumnIndex, "column (%s) is not in order", columnRefs[i]);
                lastColumnIndex = columnIndex[i];

                checkEachColumnExist.set(columnIndex[i]);
            }
            cf.setColumns(columnDescs);
            cf.setColumnIndex(columnIndex);
        }

        for (int i = 0; i < columns.size(); i++) {
            checkState(checkEachColumnExist.get(i), "column (%s) does not exist in column family, or column duplicates",
                    columns.get(i));
        }
    }

}
