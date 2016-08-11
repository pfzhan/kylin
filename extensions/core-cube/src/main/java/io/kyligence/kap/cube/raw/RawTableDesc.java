package io.kyligence.kap.cube.raw;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableDesc extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(RawTableDesc.class);

    @JsonProperty("name")
    private String name;
    @JsonProperty("model")
    private DataModelDesc model;
    @JsonProperty("columns")
    private List<ColumnDesc> columns;
    @JsonProperty("indexed_columns")
    private List<ColumnDesc> indexedColumns;
    @JsonProperty("vary_length_columns")
    private List<ColumnDesc> varyLengthColumns;
    @JsonProperty("dimensions")
    private List<ColumnDesc> dimensions;
    @JsonProperty("order_column")
    private List<ColumnDesc> orderedColumn;


    private HashMap<String, TableDesc> tableDict;
    private TableDesc factTable;
    private Set<ColumnDesc> indexedColumnSet;
    private Set<ColumnDesc> varyLengthColumnSet;

    // datamodel to rawtable 1-to-1 mapping
    private static HashMap<DataModelDesc, RawTableDesc> rawTableDescCache;

    public RawTableDesc(CubeInstance cubeInstance) {
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        DataModelDesc dataModelDesc = cubeDesc.getModel();

        this.name = dataModelDesc.getName();
        this.model = dataModelDesc;
        initColumns(model.getDimensions(), model.getMetrics());
        indexedColumns = new ArrayList<>();
        indexedColumns.addAll(columns);

        orderedColumn.add(columns.get(0));
        orderedColumn.add(columns.get(1));

        varyLengthColumns = new ArrayList<>();
        for (ColumnDesc column : columns) {
            if (!orderedColumn.contains(column)) {
                varyLengthColumns.add(column);
            }
        }

        initTableDict();
        initIndexedSet();
        initVaryLengthSet();
    }

    public static RawTableDesc getInstance(CubeDesc cubeDesc) {
        if (rawTableDescCache == null) {
            rawTableDescCache = new HashMap<>();
        }

        DataModelDesc modelDesc = cubeDesc.getModel();
        if (rawTableDescCache.containsKey(modelDesc)) {
            return rawTableDescCache.get(modelDesc);
        }

        return null;
    }

    public static void putInstance(DataModelDesc modelDesc, RawTableDesc rawTableDesc) {
        if (rawTableDescCache == null) {
            rawTableDescCache = new HashMap<>();
        }

        rawTableDescCache.put(modelDesc, rawTableDesc);
    }

    public List<ColumnDesc> getOrderedColumn() {
        return orderedColumn;
    }

    public void setOrderedColumn(List<ColumnDesc> orderedColumn) {
        this.orderedColumn = orderedColumn;
    }

    public void setModel(DataModelDesc model) {
        this.model = model;
    }

    public void setIndexedColumns(List<ColumnDesc> indexedColumns) {
        this.indexedColumns = indexedColumns;
    }

    public void setVaryLengthColumns(List<ColumnDesc> varyLengthColumns) {
        this.varyLengthColumns = varyLengthColumns;
    }


    public boolean isNeedIndex(ColumnDesc column) {
        return indexedColumnSet.contains(column);
    }

    public boolean isVaryLength(ColumnDesc column) {
        return varyLengthColumnSet.contains(column);
    }

    public DataModelDesc getModel() {
        return model;
    }

    public List<ColumnDesc> getColumns() {
        return columns;
    }

    public List<ColumnDesc> getIndexedColumns() {
        return indexedColumns;
    }

    public List<ColumnDesc> getDimensions() {
        return dimensions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void init() {
        initTableDict();
        initIndexedSet();
        initVaryLengthSet();
        initColumns(model.getDimensions(), model.getMetrics());

        putInstance(model, this);
    }

    private void initColumns(List<ModelDimensionDesc> mdd, String[] metrix) {
        columns = new ArrayList<>();
        dimensions = new ArrayList<>();

        for (ModelDimensionDesc m : mdd) {
            String[] dims = m.getColumns();
            TableDesc table = tableDict.get(m.getTable());
            for (String d : dims) {
                ColumnDesc column = table.findColumnByName(d);
                columns.add(column);

                // for columns in modle dimensions and not varylength, it's real dimensions
                if (!isVaryLength(column)) {
                    dimensions.add(column);
                }
            }
        }

        for (String m: metrix) {
            columns.add(factTable.findColumnByName(m));
        }
    }

    private void initIndexedSet() {
        indexedColumnSet = new HashSet<>();
        for (ColumnDesc c: indexedColumns) {
            indexedColumnSet.add(c);
        }
    }

    private void initVaryLengthSet() {
        varyLengthColumnSet = new HashSet<>();
        for (ColumnDesc c: varyLengthColumns) {
            varyLengthColumnSet.add(c);
        }
    }

    private void initTableDict() {
        tableDict = new HashMap<>();
        factTable = model.getFactTableDesc();
        List<TableDesc> lookupTable = model.getLookupTableDescs();

        tableDict.put(factTable.getName(), factTable);
        for (TableDesc l : lookupTable) {
            tableDict.put(l.getName(), l);
        }
    }

    @Override
    public int hashCode() {
        return model.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RawTableDesc rawTableDesc = (RawTableDesc) o;

        if (!name.equals(rawTableDesc.name))
            return false;

        if (!getModel().equals(rawTableDesc.getModel()))
            return false;

        return true;
    }
}
