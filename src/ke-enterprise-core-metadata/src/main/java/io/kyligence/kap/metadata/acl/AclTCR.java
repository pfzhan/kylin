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

package io.kyligence.kap.metadata.acl;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class AclTCR extends RootPersistentEntity {

    //wrap read only aclTCR

    private String resourceName;

    public void init(String resourceName) {
        this.resourceName = resourceName;
    }

    @JsonProperty
    private Table table = null;

    @Override
    public String resourceName() {
        return resourceName;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isAuthorized(String dbTblName) {
        final Table table = this.table;
        if (Objects.isNull(table)) {
            return true;
        }
        return table.containsKey(dbTblName);
    }

    public boolean isAuthorized(String dbTblName, String columnName) {
        final Table table = this.table;
        if (Objects.isNull(table)) {
            return true;
        }
        if (!table.containsKey(dbTblName)) {
            return false;
        }
        if (Objects.isNull(table.get(dbTblName)) || Objects.isNull(table.get(dbTblName).getColumn())) {
            return true;
        }
        return table.get(dbTblName).getColumn().contains(columnName);
    }


    public boolean isColumnAuthorized(String columnIdentity) {
        int sepIdx = columnIdentity.lastIndexOf('.');
        return isAuthorized(columnIdentity.substring(0, sepIdx), columnIdentity.substring(sepIdx+1));
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Table extends TreeMap<String, ColumnRow> {

        // # { DB.TABLE1: { "columns": ["COL1","COL2","COL3"], "rows":{COL1:["A","B","C"]} } } #
        public Table() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnRow {

        @JsonProperty
        private Column column = null;

        @JsonProperty
        private Row row = null;

        @JsonProperty("column_sensitive_data_mask")
        private List<SensitiveDataMask> columnSensitiveDataMask = null;

        @JsonProperty("dependent_columns")
        private List<DependentColumn> dependentColumns = null;

        @JsonProperty("like_row")
        private Row likeRow = null;

        @JsonProperty("row_filter")
        private List<FilterGroup> rowFilter = null;

        public void setRowFilter(List<FilterGroup> rowFilter) {
            this.rowFilter = rowFilter;
        }

        public List<FilterGroup> getRowFilter() {
            return rowFilter;
        }

        public void setLikeRow(Row likeRow) {
            this.likeRow = likeRow;
        }

        public Row getLikeRow() {
            return likeRow;
        }

        public Column getColumn() {
            return column;
        }

        public void setColumn(Column column) {
            this.column = column;
        }

        public Row getRow() {
            return row;
        }

        public void setRow(Row row) {
            this.row = row;
        }

        public Map<String, SensitiveDataMask> getColumnSensitiveDataMaskMap() {
            Map<String, SensitiveDataMask> maskMap = new HashMap<>();
            if (columnSensitiveDataMask != null) {
                for (SensitiveDataMask mask : columnSensitiveDataMask) {
                    maskMap.put(mask.getColumn(), mask);
                }
            }
            return maskMap;
        }

        public List<SensitiveDataMask> getColumnSensitiveDataMask() {
            return columnSensitiveDataMask;
        }

        public void setColumnSensitiveDataMask(List<SensitiveDataMask> columnSensitiveDataMask) {
            this.columnSensitiveDataMask = columnSensitiveDataMask;
        }

        public void setDependentColumns(List<DependentColumn> dependentColumns) {
            this.dependentColumns = dependentColumns;
        }

        public List<DependentColumn> getDependentColumns() {
            return dependentColumns;
        }

        public Map<String, Collection<DependentColumn>> getDependentColMap() {
            Map<String, Collection<DependentColumn>> map = new HashMap<>();
            if (dependentColumns != null) {
                for (DependentColumn dependentColumn : dependentColumns) {
                    map.putIfAbsent(dependentColumn.getColumn(), new LinkedList<>());
                    map.get(dependentColumn.getColumn()).add(dependentColumn);
                }
            }
            return map;
        }

        public boolean isAllRowGranted() {
            return MapUtils.isEmpty(row) && MapUtils.isEmpty(likeRow)
                    && CollectionUtils.isEmpty(rowFilter);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Column extends TreeSet<String> {

        // ["COL1", "COL2", "COL3"]
        public Column() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    @Setter
    @Getter
    public static class FilterItems {
        @JsonProperty("in_items")
        private TreeSet<String> inItems = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        @JsonProperty("like_items")
        private TreeSet<String> likeItems = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        @JsonProperty
        private OperatorType type = OperatorType.AND;

        // For deserialization
        public FilterItems() {}

        public FilterItems(TreeSet<String> inItems, TreeSet<String> likeItems, OperatorType type) {
            this.inItems = inItems;
            this.likeItems = likeItems;
            this.type = type;
        }

        public static FilterItems merge(FilterItems item1, FilterItems item2) {
            item1.inItems.addAll(item2.inItems);
            item1.likeItems.addAll(item2.likeItems);
            return item1;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Filters extends TreeMap<String, FilterItems> {

        // ["COL1", "COL2", "COL3"]
        public Filters() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    @Setter
    @Getter
    public static class FilterGroup {
        @JsonProperty("is_group")
        private boolean group;

        @JsonProperty
        private OperatorType type = OperatorType.AND;

        @JsonProperty
        private Filters filters = new Filters();
    }

    public enum OperatorType {
        AND, OR;

        private static final Set<String> validValues = Arrays.stream(OperatorType.values()).map(Enum::name)
                .collect(Collectors.toSet());

        private static void validateValue(String value) {
            if (!validValues.contains(value.toUpperCase(Locale.ROOT))) {
                throw new KylinException(INVALID_PARAMETER,
                        "Invalid value in parameter \"type\". The value should be \"AND\" or \"OR\".");
            }
        }

        public static OperatorType stringToEnum(String value) {
            value = value.toUpperCase(Locale.ROOT);
            validateValue(value);
            return OperatorType.valueOf(value);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Row extends TreeMap<String, RealRow> {

        // # { COL1: [ "A", "B", "C" ] } #
        public Row() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class RealRow extends TreeSet<String> {

        // ["A", "B", "C"]
        public RealRow() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    /**
     *  One column can have both equal condition row acl and like condition row acl.
     *  E.g. where COUNTRY_NAME in ('China', 'America') or COUNTRY_NAME like 'China%'.
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnRealRows {

        @JsonProperty
        private String dbTblColName = null;

        @JsonProperty
        private RealRow realRow = null;

        @JsonProperty
        private RealRow realLikeRow = null;

        public ColumnRealRows() {}

        public ColumnRealRows(String dbTblColName, RealRow realRow, RealRow realLikeRow) {
            this.dbTblColName = dbTblColName;
            this.realRow = realRow;
            this.realLikeRow = realLikeRow;
        }

        public RealRow getRealRow() {
            return realRow;
        }

        public RealRow getRealLikeRow() {
            return realLikeRow;
        }
    }
}
