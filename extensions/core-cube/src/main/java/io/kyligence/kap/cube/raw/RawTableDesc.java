package io.kyligence.kap.cube.raw;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity {
    public static final String RAW_TABLE_DESC_RESOURCE_ROOT = "/raw_table_desc";

    public static final String INDEX_DISCRETE = "discrete";
    public static final String INDEX_SORTED = "sorted";
    public static final String ENCODING_VAR = "var";
    
    @JsonProperty("name")
    private String name;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("columns")
    private List<RawTableColumnDesc> columns;
    
    // computed
    private KylinConfig config;
    private DataModelDesc model;
    private Map<TblColRef, RawTableColumnDesc> columnMap;
    
    // for Jackson
    public RawTableDesc() {
    }

    // for debug/test, create a mockup RawTable from DataModel
    public RawTableDesc(CubeDesc cubeDesc) {
        this.name = cubeDesc.getName();
        this.modelName = cubeDesc.getModelName();
        
        this.config = cubeDesc.getConfig();
        this.model = cubeDesc.getModel();
        this.columnMap = Maps.newHashMap();
        
        MetadataManager metaMgr = MetadataManager.getInstance(model.getConfig());
        TableDesc factTable = model.getFactTableDesc();
        
        for (ModelDimensionDesc dim : model.getDimensions()) {
            TableDesc dimTable = metaMgr.getTableDesc(dim.getTable());
            for (String col : dim.getColumns()) {
                TblColRef colRef = dimTable.findColumnByName(col).getRef();
                String index = guessColIndex(cubeDesc, colRef);
                String encoding = guessColEncoding(cubeDesc, colRef);
                RawTableColumnDesc rawColDesc = new RawTableColumnDesc(colRef.getColumnDesc(), index, encoding);
                columnMap.put(colRef, rawColDesc);
            }
        }
        
        for (String col : model.getMetrics()) {
            TblColRef colRef = factTable.findColumnByName(col).getRef();
            String index = null;
            String encoding = ENCODING_VAR;
            RawTableColumnDesc rawColDesc = new RawTableColumnDesc(colRef.getColumnDesc(), index, encoding);
            columnMap.put(colRef, rawColDesc);
        }
        
        this.columns = Lists.newArrayList(columnMap.values());
    }

    private String guessColIndex(CubeDesc cubeDesc, TblColRef colRef) {
        PartitionDesc partDesc = cubeDesc.getModel().getPartitionDesc();
        if (partDesc != null && colRef.equals(partDesc.getPartitionDateColumnRef()))
            return INDEX_SORTED;
        else
            return INDEX_DISCRETE;
    }

    private String guessColEncoding(CubeDesc cubeDesc, TblColRef colRef) {
        RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(colRef);
        if (colDesc == null)
            return ENCODING_VAR;
        else
            return colDesc.getEncoding();
    }

    public TblColRef getOrderedColumn() {
        for (RawTableColumnDesc colDesc : columns) {
            if (INDEX_SORTED.equals(colDesc.getIndex()))
                return colDesc.getColumn().getRef();
        }
        return null;
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
        return Lists.newArrayList(columnMap.keySet());
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
        
        for (RawTableColumnDesc colDesc : columns) {
            colDesc.init(metaMgr);
            columnMap.put(colDesc.getColumn().getRef(), colDesc);
        }
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

}
