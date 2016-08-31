package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(RawTableDesc.class);
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
    @JsonProperty("fuzzy_columns")
    private Set<String> fuzzyColumnSet;

    // computed
    private KylinConfig config;
    private DataModelDesc model;
    private Map<TblColRef, RawTableColumnDesc> columnMap;

    // for Jackson
    public RawTableDesc() {
    }

    // for debug/test, create a mockup RawTable from DataModel
    public RawTableDesc(CubeDesc cubeDesc) {
        this.updateRandomUuid();
        this.name = cubeDesc.getName();
        this.modelName = cubeDesc.getModelName();

        this.config = cubeDesc.getConfig();
        this.model = cubeDesc.getModel();
        this.columnMap = Maps.newHashMap();

        this.columns = Lists.newArrayList();
        this.fuzzyColumnSet = getFuzzyColumnSet(cubeDesc);

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
                columns.add(rawColDesc);
                logger.info("Column name: {}", colRef.getName());
            }
        }

        for (String col : model.getMetrics()) {
            TblColRef colRef = factTable.findColumnByName(col).getRef();
            if (!columnMap.containsKey(colRef)) {
                String index = null;
                String encoding = ENCODING_VAR;
                RawTableColumnDesc rawColDesc = new RawTableColumnDesc(colRef.getColumnDesc(), index, encoding);
                columnMap.put(colRef, rawColDesc);
                columns.add(rawColDesc);
                logger.info("Column name: {}", colRef.getName());
            }
        }

        int lookupLength = model.getLookups().length;
        for (int i = 0; i < lookupLength; i++) {
            JoinDesc join = model.getLookups()[i].getJoin();
            for (TblColRef primary : join.getPrimaryKeyColumns()) {
                if (!columnMap.containsKey(primary)) {
                    String index = null;
                    String encoding = ENCODING_VAR;
                    RawTableColumnDesc rawColDesc = new RawTableColumnDesc(primary.getColumnDesc(), index, encoding);
                    columnMap.put(primary, rawColDesc);
                    columns.add(rawColDesc);
                    logger.info("Column name: {}", primary.getName());
                }
            }

            for (TblColRef foreign : join.getForeignKeyColumns()) {
                if (!columnMap.containsKey(foreign)) {
                    String index = null;
                    String encoding = ENCODING_VAR;
                    RawTableColumnDesc rawColDesc = new RawTableColumnDesc(foreign.getColumnDesc(), index, encoding);
                    columnMap.put(foreign, rawColDesc);
                    columns.add(rawColDesc);
                    logger.info("Column name: {}", foreign.getName());
                }
            }
        }
    }

    public static Set<String> getFuzzyColumnSet(CubeDesc cube) {
        String fuzzyColumns = cube.getOverrideKylinProps().get("kylin.rawtable.fuzzy_columns");

        if (fuzzyColumns == null) {
            return Sets.newHashSet();
        }

        return Sets.newHashSet(fuzzyColumns.split(","));
    }

    private String guessColIndex(CubeDesc cubeDesc, TblColRef colRef) {
        PartitionDesc partDesc = cubeDesc.getModel().getPartitionDesc();
        if (partDesc != null && colRef.equals(partDesc.getPartitionDateColumnRef()))
            return INDEX_SORTED;
        else
            return INDEX_DISCRETE;
    }

    private String guessColEncoding(CubeDesc cubeDesc, TblColRef colRef) {
        RowKeyColDesc colDesc = null;
        try {
            colDesc = cubeDesc.getRowkey().getColDesc(colRef);
        } catch (NullPointerException ex) {
            // getColDesc throws NPE if colRef not found
        }
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
        return fuzzyColumnSet.contains(colRef.getName());
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

    @Override
    public String toString() {
        return "RawTableDesc [name=" + name + "]";
    }

}
