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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity implements IEngineAware {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RawTableDesc.class);
    public static final String RAW_TABLE_DESC_RESOURCE_ROOT = "/raw_table_desc";

    public static final String INDEX_DISCRETE = "discrete";
    public static final String INDEX_FUZZY = "fuzzy";
    public static final String INDEX_SORTED = "sorted";

    public static final String RAWTABLE_ENCODING_VAR = "var";
    public static final String RAWTABLE_ENCODING_ORDEREDBYTES = "orderedbytes";

    @JsonProperty("name")
    private String name;
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

    // computed
    private KylinConfig config;
    private DataModelDesc model;
    private Map<TblColRef, RawTableColumnDesc> columnMap;
    private HashSet<TblColRef> fuzzyColumnSet;

    // for Jackson
    public RawTableDesc() {
    }

    public HashSet<TblColRef> getFuzzyColumnSet() {
        return fuzzyColumnSet;
    }

    // validate there's at least one ordered column
    public void validate() {
        getOrderedColumn();
    }

    public TblColRef getOrderedColumn() {
        for (RawTableColumnDesc colDesc : columns) {
            if (INDEX_SORTED.equals(colDesc.getIndex()))
                return colDesc.getColumn();
        }

        DataModelDesc model = getModel();
        if (model.getPartitionDesc().isPartitioned())
            return model.getPartitionDesc().getPartitionDateColumnRef();

        throw new IllegalStateException(this + " missing ordered column");
    }

    public List<TblColRef> getColumnsExcludingOrdered() {
        List<TblColRef> cols = new ArrayList<>();
        for (RawTableColumnDesc colDesc : columns) {
            if (INDEX_SORTED.equals(colDesc.getIndex()))
                continue;
            cols.add(colDesc.getColumn());
        }
        return cols;
    }

    public Boolean isNeedFuzzyIndex(TblColRef colRef) {
        return fuzzyColumnSet.contains(colRef);
    }

    public int getEstimateRowSize() {
        int size = 0;
        for (RawTableColumnDesc col : columns) {
            size += col.getColumn().getType().getStorageBytesEstimate();
        }
        return size;
    }

    public List<Pair<String, Integer>> getEncodings() {
        List<TblColRef> columnsInOrder = getColumns();
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

    public List<TblColRef> getColumns() {
        List<TblColRef> result = Lists.newArrayList();
        TblColRef ordered = getOrderedColumn();
        if (ordered != null) {
            result.add(ordered);
        }
        result.addAll(getColumnsExcludingOrdered());
        return result;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
        return RAW_TABLE_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    void init(KylinConfig config) {
        MetadataManager metaMgr = MetadataManager.getInstance(config);

        this.config = config;
        this.model = metaMgr.getDataModelDesc(modelName);
        this.columnMap = Maps.newHashMap();
        this.fuzzyColumnSet = Sets.newHashSet();

        for (RawTableColumnDesc colDesc : columns) {
            colDesc.init(model);
            if (colDesc.getFuzzyIndex()) {
                fuzzyColumnSet.add(colDesc.getColumn());
            }
            columnMap.put(colDesc.getColumn(), colDesc);
        }

        this.validate();
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
}
