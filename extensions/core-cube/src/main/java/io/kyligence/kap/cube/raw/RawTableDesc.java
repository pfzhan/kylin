package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity implements IEngineAware {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RawTableDesc.class);
    public static final String RAW_TABLE_DESC_RESOURCE_ROOT = "/raw_table_desc";

    public static final String INDEX_DISCRETE = "discrete";
    public static final String INDEX_FUZZY = "fuzzy";
    public static final String INDEX_SORTED = "sorted";
    public static final String ENCODING_VAR = "var";

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
    private Map<TblColRef, RawTableColumnDesc> columnMap;
    private HashSet<TblColRef> fuzzyColumnSet;

    // for Jackson
    public RawTableDesc() {
    }

    public HashSet<TblColRef> getFuzzyColumnSet() {
        return fuzzyColumnSet;
    }

    public TblColRef getOrderedColumn() {
        for (RawTableColumnDesc colDesc : columns) {
            if (INDEX_SORTED.equals(colDesc.getIndex()))
                return colDesc.getColumn().getRef();
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
            cols.add(colDesc.getColumn().getRef());
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

    public boolean isNeedIndex(TblColRef col) {
        RawTableColumnDesc desc = columnMap.get(col);
        return desc.getIndex() != null;
    }

    public boolean isVaryLength(TblColRef col) {
        RawTableColumnDesc desc = columnMap.get(col);
        return ENCODING_VAR.equals(desc.getEncoding());
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
        fuzzyColumnSet = new HashSet<>();

        this.config = config;
        this.columnMap = Maps.newHashMap();

        for (RawTableColumnDesc colDesc : columns) {
            colDesc.init(metaMgr);
            if (colDesc.getFuzzyIndex()) {
                fuzzyColumnSet.add(colDesc.getColumn().getRef());
            }
            columnMap.put(colDesc.getColumn().getRef(), colDesc);
        }
    }

    // ============================================================================

    public KylinConfig getConfig() {
        return config;
    }

    public DataModelDesc getModel() {
        return MetadataManager.getInstance(config).getDataModelDesc(modelName);
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
