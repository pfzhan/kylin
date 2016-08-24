package io.kyligence.kap.cube.raw;

import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableColumnDesc {

    @JsonProperty("table")
    private String tableName;
    @JsonProperty("column")
    private String columnName;
    @JsonProperty("index")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String index;
    @JsonProperty("encoding")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String encoding;

    // computed
    private ColumnDesc column;

    // for Jackson
    public RawTableColumnDesc() {
    }

    // for test
    public RawTableColumnDesc(ColumnDesc col, String index, String encoding) {
        this.tableName = col.getTable().getIdentity();
        this.columnName = col.getName();
        this.index = index;
        this.encoding = encoding;

        this.column = col;
    }

    void init(MetadataManager metaMgr) {
        tableName = tableName.toUpperCase();
        columnName = columnName.toUpperCase();
        column = metaMgr.getColumnDesc(tableName + "." + columnName);
    }

    // ============================================================================

    public ColumnDesc getColumn() {
        return column;
    }

    void setColumn(ColumnDesc column) {
        this.column = column;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getIndex() {
        return index;
    }

    public String getEncoding() {
        return encoding;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RawTableColumnDesc other = (RawTableColumnDesc) obj;

        if (tableName == null) {
            if (other.tableName!= null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;

        if (columnName == null) {
            if (other.columnName != null)
                return false;
        } else if (!columnName.equals(other.columnName))
            return false;

        return true;
    }
}
