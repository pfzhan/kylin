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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RowACL extends RootPersistentEntity implements IKeep {

    //For frontend: USER:{DB.TABLE1:{COLUMN1:{a,b,c}, COLUMN2:{d,e,f}}, DB.TABLE2:{COLUMN1:{a,b,c}, COLUMN3:{m,n}}}
    @JsonProperty()
    private Map<String, TableRowCondList> tableRowCondsWithUser;

    RowACL() {
        tableRowCondsWithUser = new HashMap<>();
    }

    public Map<String, TableRowCondList> getTableRowCondsWithUser() {
        return tableRowCondsWithUser;
    }

    public RowACL add(String username, String table, Map<String, List<Cond>> condsWithColumn) {
        if (tableRowCondsWithUser == null) {
            tableRowCondsWithUser = new HashMap<>();
        }

        TableRowCondList rowCondsWithTable = tableRowCondsWithUser.get(username);
        if (rowCondsWithTable == null) {
            rowCondsWithTable = new TableRowCondList();
        }

        validateNotExists(username, table, rowCondsWithTable);
        putRowConds(username, table, condsWithColumn, rowCondsWithTable);
        return this;
    }

    public RowACL update(String username, String table, Map<String, List<Cond>> condsWithColumn) {
        if (tableRowCondsWithUser == null) {
            tableRowCondsWithUser = new HashMap<>();
        }

        TableRowCondList rowCondsWithTable = tableRowCondsWithUser.get(username);

        validateExists(username, table, rowCondsWithTable);
        putRowConds(username, table, condsWithColumn, rowCondsWithTable);
        return this;
    }

    private void putRowConds(String username, String table, Map<String, List<Cond>> condsWithColumn, TableRowCondList rowCondsWithTable) {
        RowCondList rowCondList = new RowCondList(condsWithColumn);
        rowCondsWithTable.putRowCondlist(table, rowCondList);
        tableRowCondsWithUser.put(username, rowCondsWithTable);
    }

    public RowACL delete(String username, String table) {
        validateRowCondsExists(username, table);
        TableRowCondList tableRowConds = tableRowCondsWithUser.get(username);
        tableRowConds.removeTbl(table);
        if (tableRowConds.isEmpty()) {
            tableRowCondsWithUser.remove(username);
        }
        return this;
    }

    RowACL deleteByUser(String username) {
        validateUserHasRowACL(username);
        tableRowCondsWithUser.remove(username);
        return this;
    }

    RowACL deleteByTbl(String table) {
        Iterator<Map.Entry<String, TableRowCondList>> it = tableRowCondsWithUser.entrySet().iterator();
        while (it.hasNext()) {
            TableRowCondList tableRowCondList = it.next().getValue();
            tableRowCondList.removeTbl(table);
            if (tableRowCondList.isEmpty()) {
                it.remove();
            }
        }
        return this;
    }

    private void validateNotExists(String username, String table, TableRowCondList tableRowCondList) {
        if (!tableRowCondList.getRowCondListByTable(table).isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + ", table:" + table + " already in row cond list!");
        }
    }

    private void validateExists(String username, String table, TableRowCondList tableRowCondList) {
        if (tableRowCondList == null || tableRowCondList.isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + " not found in row cond list!");
        }

        if (tableRowCondList.getRowCondListByTable(table).isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + ", column:" + table + " not found in row cond list!");
        }
    }

    private void validateRowCondsExists(String username, String table) {
        TableRowCondList tableRowConds = tableRowCondsWithUser.get(username);
        if (tableRowConds == null) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + " not have any row acl conds!");
        }
        if (tableRowConds.getRowCondListByTable(table).isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, table:" + table + " not have any row acl conds!");
        }
    }

    private void validateUserHasRowACL(String username) {
        TableRowCondList tableRowConds = tableRowCondsWithUser.get(username);
        if (tableRowConds == null) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + " not have any row acl conds!");
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class TableRowCondList implements Serializable, IKeep {
        //all row conds in the table
        @JsonProperty()
        private Map<String, RowCondList> rowCondsWithTable; //T1:rowCondList1, T2:rowCondList2

        private TableRowCondList() {
            rowCondsWithTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        public int size() {
            return rowCondsWithTable.size();
        }

        public boolean isEmpty() {
            return rowCondsWithTable.isEmpty();
        }

        Set<String> keySet() {
            return rowCondsWithTable.keySet();
        }

        RowCondList getRowCondListByTable(String table) {
            RowCondList rowCondList = rowCondsWithTable.get(table);
            if (rowCondList == null) {
                rowCondList = new RowCondList();
            }
            return rowCondList;
        }

        void putRowCondlist(String key, RowCondList value) {
            rowCondsWithTable.put(key, value);
        }

        void removeTbl(String table) {
            rowCondsWithTable.remove(table);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class RowCondList implements Serializable, IKeep {
        //all row conds in the table
        @JsonProperty()
        private Map<String, List<Cond>> condsWithColumn; //C1:{cond1, cond2},C2{cond1, cond3}

        private RowCondList() {
            this.condsWithColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        RowCondList(Map<String, List<Cond>> condsWithColumn) {
            this.condsWithColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.condsWithColumn.putAll(condsWithColumn);
        }

        public int size() {
            return condsWithColumn.size();
        }

        boolean isEmpty() {
            return condsWithColumn.isEmpty();
        }

        List<Cond> getCondsByColumn(String column) {
            return condsWithColumn.get(column);
        }

        Map<String, List<Cond>> getCondsWithColumn() {
            if (condsWithColumn == null) {
                condsWithColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            }
            return condsWithColumn;
        }

        public Set<String> keySet() {
            return condsWithColumn.keySet();
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