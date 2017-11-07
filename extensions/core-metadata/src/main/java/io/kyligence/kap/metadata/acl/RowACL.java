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

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.common.obf.IKeep;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RowACL extends RootPersistentEntity implements IKeep {
    @JsonProperty("tableRowCondsWithUser")
    private RowACLEntry tableRowCondsWithUser = new RowACLEntry();

    @JsonProperty("tableRowCondsWithGroup")
    private RowACLEntry tableRowCondsWithGroup = new RowACLEntry();

    private RowACLEntry currentEntry(String type) {
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            return tableRowCondsWithUser;
        } else {
            return tableRowCondsWithGroup;
        }
    }

    Map<String, String> getQueryUsedTblToConds(String project, String name, String type) {
        return currentEntry(type).getQueryUsedTblToConds(project, name);
    }

    // only for frontend to display {userOrGroup:ColumnToConds}
    public Map<String, ColumnToConds> getColumnToCondsByTable(String table, String type) {
        return currentEntry(type).getColumnToCondsByTable(table);
    }

    public boolean contains(String name, String type) {
        return currentEntry(type).containsKey(name);
    }

    RowACL add(String name, String table, ColumnToConds columnToConds, String type) {
        currentEntry(type).add(name, table, columnToConds);
        return this;
    }

    RowACL update(String name, String table, ColumnToConds columnToConds, String type) {
        currentEntry(type).update(name, table, columnToConds);
        return this;
    }

    RowACL delete(String name, String table, String type) {
        currentEntry(type).delete(name, table);
        return this;
    }

    RowACL delete(String name, String type) {
        currentEntry(type).delete(name);
        return this;
    }

    RowACL deleteByTbl(String table) {
        tableRowCondsWithUser.deleteByTbl(table);
        tableRowCondsWithGroup.deleteByTbl(table);
        return this;
    }

    int size() {
        return tableRowCondsWithUser.size() + tableRowCondsWithGroup.size();
    }

    int size(String type) {
        return currentEntry(type).size();
    }

    static Map<String, String> getColumnWithType(String project, String table) {
        Map<String, String> columnWithType = new HashMap<>();
        TableDesc tableDesc = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(table, project);
        ColumnDesc[] columns = tableDesc.getColumns();
        for (ColumnDesc column : columns) {
            columnWithType.put(column.getName(), column.getTypeName());
        }
        return columnWithType;
    }

    static String concatConds(ColumnToConds condsWithCol, Map<String, String> columnWithType) {
        StrBuilder result = new StrBuilder();
        int j = 0;
        for (String col : condsWithCol.keySet()) {
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type not found");
            List<RowACL.Cond> conds = condsWithCol.getCondsByColumn(col);
            for (int i = 0; i < conds.size(); i++) {
                String parsedCond = conds.get(i).toString(col, type);
                if (conds.size() == 1) {
                    result.append(parsedCond);
                    continue;
                }
                if (i == 0) {
                    result.append("(").append(parsedCond).append(" OR ");
                    continue;
                }
                if (i == conds.size() - 1) {
                    result.append(parsedCond).append(")");
                    continue;
                }
                result.append(parsedCond).append(" OR ");
            }
            if (j != condsWithCol.size() - 1) {
                result.append(" AND ");
            }
            j++;
        }
        return result.toString();
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class RowACLEntry extends HashMap<String, TableToRowConds> implements Serializable, IKeep {

        private Map<String, String> getQueryUsedTblToConds(String project, String name) {
            Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            TableToRowConds tableToRowConds = super.get(name);
            if (tableToRowConds == null || tableToRowConds.isEmpty()) {
                return result;
            }

            for (String tbl : tableToRowConds.keySet()) {
                Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, tbl));
                ColumnToConds columnToConds = tableToRowConds.getColumnToCondsByTable(tbl);
                result.put(tbl, concatConds(columnToConds, columnWithType));
            }
            return result;
        }

        private Map<String, ColumnToConds> getColumnToCondsByTable(String table) {
            Map<String, ColumnToConds> results = new HashMap<>();
            for (String Identifiers : super.keySet()) {
                ColumnToConds columnToConds = super.get(Identifiers).getColumnToCondsByTable(table);
                if (!columnToConds.isEmpty()) {
                    results.put(Identifiers, columnToConds);
                }
            }
            return ImmutableMap.copyOf(results);
        }

        private boolean containsKey(String name) {
            return super.containsKey(name);
        }

        private void add(String name, String table, ColumnToConds columnToConds) {
            TableToRowConds tableToRowConds = super.get(name);
            if (tableToRowConds == null) {
                tableToRowConds = new TableToRowConds();
            }
            validateACLNotExists(name, table, tableToRowConds);
            putRowACLEntry(name, table, columnToConds, tableToRowConds);
        }

        private void update(String name, String table, ColumnToConds columnToConds) {
            TableToRowConds tableToRowConds = super.get(name);
            validateACLExists(name, table);
            putRowACLEntry(name, table, columnToConds, tableToRowConds);
        }

        private void putRowACLEntry(String name, String table, ColumnToConds columnToConds, TableToRowConds tableToRowConds) {
            tableToRowConds.put(table, columnToConds);
            super.put(name, tableToRowConds);
        }

        private void delete(String name, String table) {
            validateACLExists(name, table);
            TableToRowConds tableRowConds = super.get(name);
            tableRowConds.removeByTbl(table);
            if (tableRowConds.isEmpty()) {
                super.remove(name);
            }
        }

        private void delete(String name) {
            validateACLExists(name);
            super.remove(name);
        }

        private void deleteByTbl(String table) {
            Iterator<Map.Entry<String, TableToRowConds>> it = super.entrySet().iterator();
            while (it.hasNext()) {
                TableToRowConds tableToRowConds = it.next().getValue();
                tableToRowConds.removeByTbl(table);
                if (tableToRowConds.isEmpty()) {
                    it.remove();
                }
            }
        }

        private void validateACLNotExists(String name, String table, TableToRowConds tableToRowConds) {
            if (tableToRowConds.containTbl(table)) {
                throw new RuntimeException(
                        "Operation fail, user:" + name + ", table:" + table + " already has row ACL!");
            }
        }

        private void validateACLExists(String name, String table) {
            TableToRowConds tableRowConds = super.get(name);
            if (tableRowConds == null) {
                throw new RuntimeException(
                        "Operation fail, user:" + name + " not have any row acl conds!");
            }
            if (!tableRowConds.containTbl(table)) {
                throw new RuntimeException(
                        "Operation fail, table:" + table + " not have any row acl conds!");
            }
        }

        private void validateACLExists(String name) {
            if (!super.containsKey(name)) {
                throw new RuntimeException(
                        "Operation fail, user:" + name + " not have any row acl conds!");
            }
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class TableToRowConds implements Serializable, IKeep {
        //{T1:columnToConds1}, {T2:columnToConds2}
        @JsonProperty("rowCondsWithTable")
        private CaseInsensitiveStringMap<ColumnToConds> tableToRowConds = new CaseInsensitiveStringMap<>();

        private boolean isEmpty() {
            return tableToRowConds.isEmpty();
        }

        private boolean containTbl(String table) {
            return tableToRowConds.containsKey(table);
        }

        private Set<String> keySet() {
            return tableToRowConds.keySet();
        }

        private ColumnToConds getColumnToCondsByTable(String table) {
            ColumnToConds columnToConds = tableToRowConds.get(table);
            if (columnToConds == null) {
                columnToConds = new ColumnToConds();
            }
            return columnToConds;
        }

        private void put(String key, ColumnToConds value) {
            tableToRowConds.put(key, value);
        }

        private void removeByTbl(String table) {
            tableToRowConds.remove(table);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnToConds implements Serializable, IKeep {
        //all row conds in the table, for example:C1:{cond1, cond2},C2{cond1, cond3}, immutable
        @JsonProperty("condsWithColumn")
        private CaseInsensitiveStringMap<List<Cond>> columnToConds = new CaseInsensitiveStringMap<>();

        //just for json deserialization
        public ColumnToConds() {
        }

        public ColumnToConds(Map<String, List<Cond>> columnToConds) {
            this.columnToConds = new CaseInsensitiveStringMap<>();
            this.columnToConds.putAll(columnToConds);
        }

        public int size() {
            return columnToConds.size();
        }

        public boolean isEmpty() {
            return columnToConds.isEmpty();
        }

        public List<Cond> getCondsByColumn(String col) {
            List<Cond> conds = columnToConds.get(col);
            if (conds == null) {
                conds = new ArrayList<>();
            }
            return ImmutableList.copyOf(conds);
        }

        public Set<String> keySet() {
            return ImmutableSet.copyOf(columnToConds.keySet());
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Cond implements Serializable, IKeep {
        public enum IntervalType implements Serializable, IKeep {
            OPEN,            //(a,b) = {x | a < x < b}
            CLOSED,          //[a,b] = {x | a <= x <= b}
            LEFT_INCLUSIVE,  //[a,b) = {x | a <= x < b}
            RIGHT_INCLUSIVE, //(a,b] = {x | a < x <= b}
        }

        @JsonProperty
        private IntervalType type;

        @JsonProperty()
        private String leftExpr;

        @JsonProperty()
        private String rightExpr;

        //just for json deserialization
        public Cond() {
        }

        public Cond(IntervalType type, String leftExpr, String rightExpr) {
            this.type = type;
            this.leftExpr = leftExpr;
            this.rightExpr = rightExpr;
        }

        public Cond(String value) {
            this.type = IntervalType.CLOSED;
            this.leftExpr = this.rightExpr = value;
        }

        String toString(String column, String columnType) {
            Pair<String, String> op = getOp(this.type);
            String leftValue = trim(leftExpr, columnType);
            String rightValue = trim(rightExpr, columnType);

            if (leftValue == null && rightValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<" + rightValue + ")";
                } else if (type == IntervalType.RIGHT_INCLUSIVE) {
                    return "(" + column + "<=" + rightValue + ")";
                } else {
                    throw new RuntimeException("error expr");
                }
            }

            if (rightValue == null && leftValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + ">" + leftValue + ")";
                } else if (type == IntervalType.LEFT_INCLUSIVE) {
                    return "(" + column + ">=" + leftValue + ")";
                } else {
                    throw new RuntimeException("error expr");
                }
            }

            if ((leftValue == null && rightValue == null) || leftValue.equals(rightValue)) {
                if (type == IntervalType.CLOSED) {
                    return "(" + column + "=" + leftValue + ")";
                }
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<>" + leftValue + ")";
                }
            }
            return "(" + column + op.getFirst() + leftValue + " AND " + column + op.getSecond() + rightValue + ")";
        }

        //add cond with single quote and escape single quote
        static String trim(String expr, String type) {
            if (expr == null) {
                return null;
            }
            if (type.startsWith("varchar") || type.equals("string") || type.equals("char")) {
                expr = expr.replaceAll("'", "''");
                expr = "'" + expr + "'";
            }
            if (type.equals("date")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                expr = "DATE '" + expr + "'";
            }
            if (type.equals("timestamp") || type.equals("datetime")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                expr = "TIMESTAMP '" + expr + "'";
            }
            if (type.equals("time")) {
                final int TIME_START_POS = 11; //"1970-01-01 ".length() = 11
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                expr = sdf.format(new Date(Long.valueOf(expr)));
                //transform "1970-01-01 00:00:59" into "00:00:59"
                expr = "TIME '" + expr.substring(TIME_START_POS, expr.length()) + "'";
            }
            return expr;
        }

        private static Pair<String, String> getOp(Cond.IntervalType type) {
            switch (type) {
                case OPEN:
                    return Pair.newPair(">", "<");
                case CLOSED:
                    return Pair.newPair(">=", "<=");
                case LEFT_INCLUSIVE:
                    return Pair.newPair(">=", "<");
                case RIGHT_INCLUSIVE:
                    return Pair.newPair(">", "<=");
                default:
                    throw new RuntimeException("error, unknown type for condition");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Cond cond = (Cond) o;

            if (type != cond.type) return false;
            if (leftExpr != null ? !leftExpr.equals(cond.leftExpr) : cond.leftExpr != null) return false;
            return rightExpr != null ? rightExpr.equals(cond.rightExpr) : cond.rightExpr == null;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (leftExpr != null ? leftExpr.hashCode() : 0);
            result = 31 * result + (rightExpr != null ? rightExpr.hashCode() : 0);
            return result;
        }
    }
}