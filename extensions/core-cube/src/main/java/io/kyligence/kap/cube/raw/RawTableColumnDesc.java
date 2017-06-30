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

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableColumnDesc {

    public static final String INDEX_DISCRETE = "discrete";
    public static final String INDEX_FUZZY = "fuzzy";
    public static final String INDEX_SORTED = "sorted";
    public static final String RAWTABLE_ENCODING_VAR = "var";
    public static final String RAWTABLE_ENCODING_ORDEREDBYTES = "orderedbytes";

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
    @JsonProperty("encoding_version")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private int encodingVersion = 1;
    @JsonProperty("is_shardby")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean shardby;
    @JsonProperty("is_sortby")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean sortby;

    // computed
    private TblColRef column;

    // for Jackson
    public RawTableColumnDesc() {
    }

    void init(DataModelDesc model) {
        tableName = tableName.toUpperCase();
        columnName = columnName.toUpperCase();
        column = model.findColumn(tableName, columnName);
        
        // normalize tableName & columnName
        tableName = column.getTableAlias();
        columnName = column.getName();

        // backward compatibility
        if (shardby == null) {
            shardby = index.equals(INDEX_SORTED) ? true : false;
        }
        if (sortby == null) {
            sortby = index.equals(INDEX_SORTED) ? true : false;
        }
    }

    // ============================================================================

    public TblColRef getColumn() {
        return column;
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

    // only for test
    void setIndex(String index) {
        this.index = index;
    }

    public String getEncoding() {
        return encoding;
    }

    void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public int getEncodingVersion() {
        return encodingVersion;
    }

    public Boolean getFuzzyIndex() {
        return INDEX_FUZZY.equals(index);
    }

    public Boolean isShardby() {
        return shardby == null ? false : shardby;
    }

    void setShardby(Boolean shardby) {
        this.shardby = shardby;
    }

    public Boolean isSortby() {
        return sortby == null ? false : sortby;
    }

    // test only
    void setSortby(Boolean sortby) {
        this.sortby = sortby;
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
            if (other.tableName != null)
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
