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

import io.kyligence.kap.common.obf.IKeep;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RowACL extends RootPersistentEntity implements IKeep {
    //cuz row acl is complicated, so stored two data, one for frontend to display, another one for backend to use to filter query
    //For frontend.USER :{DB.TABLE1:{COLUMN1:{a,b,c}, COLUMN2:{d,e,f}}, DB.TABLE2:{COLUMN1:{a,b,c}, COLUMN3:{m,n}}}
    @JsonProperty()
    private Map<String, TableRowCondList> tableRowCondsWithUser;
    // For backend query row filter used
    @JsonProperty()
    private Map<String, QueryUsedCondList> queryUsedCondsWithTable;

    RowACL() {
        tableRowCondsWithUser = new HashMap<>();
        queryUsedCondsWithTable = new HashMap<>();
    }

    public Map<String, TableRowCondList> getTableRowCondsWithUser() {
        return tableRowCondsWithUser;
    }

    Map<String, QueryUsedCondList> getQueryUsedConds() {
        return queryUsedCondsWithTable;
    }

    //user1:{col1:[a, b, c], col2:[d]}
    public Map<String, Map<String, List<String>>> getRowCondListByTable(String table) {
        Map<String, Map<String, List<String>>> results = new HashMap<>();
        for (String user : tableRowCondsWithUser.keySet()) {
            TableRowCondList tableRowCondList = tableRowCondsWithUser.get(user);
            RowCondList rowCondListByTable = tableRowCondList.getRowCondListByTable(table);
            if (!rowCondListByTable.isEmpty()) {
                results.put(user, rowCondListByTable.condsWithColumn);
            }
        }
        return results;
    }

    public Map<String, String> getQueryUsedCondsByUser(String username) {
        QueryUsedCondList queryUsedCondList = queryUsedCondsWithTable.get(username);
        if (queryUsedCondList == null) {
            queryUsedCondList = new QueryUsedCondList();
        }
        return queryUsedCondList.getConcatedCondsWithTable();
    }

    public RowACL add(String username, String table, Map<String, List<String>> condsWithColumn, Map<String, String> columnWithType) {
        if (tableRowCondsWithUser == null) {
            tableRowCondsWithUser = new HashMap<>();
        }

        TableRowCondList rowCondsWithTable = tableRowCondsWithUser.get(username);
        if (rowCondsWithTable == null) {
            rowCondsWithTable = new TableRowCondList();
        }

        validateNotExists(username, table, rowCondsWithTable);

        putRowConds(username, table, condsWithColumn, rowCondsWithTable);
        putQueryUsedCond(username, table, columnWithType);
        return this;
    }

    public RowACL update(String username, String table, Map<String, List<String>> condsWithColumn, Map<String, String> columnWithType) {
        if (tableRowCondsWithUser == null) {
            tableRowCondsWithUser = new HashMap<>();
        }

        TableRowCondList rowCondsWithTable = tableRowCondsWithUser.get(username);
        if (rowCondsWithTable == null || rowCondsWithTable.isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + " not found in row cond list!");
        }

        validateExists(username, table, rowCondsWithTable);

        putRowConds(username, table, condsWithColumn, rowCondsWithTable);
        putQueryUsedCond(username, table, columnWithType);
        return this;
    }

    private void putRowConds(String username, String table, Map<String, List<String>> condsWithColumn, TableRowCondList rowCondsWithTable) {
        RowCondList rowCondList = new RowCondList(condsWithColumn);
        rowCondsWithTable.putRowCondlist(table, rowCondList);
        tableRowCondsWithUser.put(username, rowCondsWithTable);
    }

    public RowACL delete(String username, String table) {
        validateRowCondsExists(username, table);
        TableRowCondList tableRowConds = tableRowCondsWithUser.get(username);
        QueryUsedCondList queryUsedConds = queryUsedCondsWithTable.get(username);
        tableRowConds.removeTbl(table);
        if (tableRowConds.isEmpty()) {
            tableRowCondsWithUser.remove(username);
        }
        queryUsedConds.removeTbl(table);
        if (queryUsedConds.isEmpty()) {
            queryUsedCondsWithTable.remove(username);
        }
        return this;
    }

    public RowACL delete(String username) {
        validateUserHasRowACL(username);
        tableRowCondsWithUser.remove(username);
        queryUsedCondsWithTable.remove(username);
        return this;
    }

    public RowACL deleteByTbl(String table) {
        Iterator<Map.Entry<String, TableRowCondList>> it = tableRowCondsWithUser.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TableRowCondList> entry = it.next();
            TableRowCondList tableRowCondList = entry.getValue();
            tableRowCondList.removeTbl(table);
            if (tableRowCondList.isEmpty()) {
                it.remove();
            }
        }

        Iterator<Map.Entry<String, QueryUsedCondList>> it2 = queryUsedCondsWithTable.entrySet().iterator();
        while (it2.hasNext()) {
            Map.Entry<String, QueryUsedCondList> entry2 = it2.next();
            QueryUsedCondList queryUsedCondList = entry2.getValue();
            queryUsedCondList.removeTbl(table);
            if (queryUsedCondList.isEmpty()) {
                it2.remove();
            }
        }

        return this;
    }

    private void putQueryUsedCond(String username, String table, Map<String, String> columnWithType) {
        if (queryUsedCondsWithTable == null) {
            queryUsedCondsWithTable = new HashMap<>();
        }

        TableRowCondList tableRowCondList = Preconditions.checkNotNull(tableRowCondsWithUser.get(username),
                "no row cond list, save row conditions for query failed!");

        QueryUsedCondList queryUsedConds = queryUsedCondsWithTable.get(username);
        if (queryUsedConds == null) {
            queryUsedConds = new QueryUsedCondList();
        }
        Map<String, List<String>> condsWithColumn = tableRowCondList.getRowCondListByTable(table).getCondsWithColumn();
        queryUsedConds.putConcatedConds(table, concatConds(condsWithColumn, columnWithType));
        queryUsedCondsWithTable.put(username, queryUsedConds);
    }

    static String concatConds(Map<String, List<String>> condsWithColumn, Map<String, String> columnWithType) {
        Map<String, List<String>> trimedCondsWithColumn = trimConds(condsWithColumn, columnWithType);
        StringBuilder result = new StringBuilder();
        int j = 0;
        for (String col : trimedCondsWithColumn.keySet()) {
            List<String> conds = trimedCondsWithColumn.get(col);

            for (int i = 0; i < conds.size(); i++) {
                String cond = conds.get(i);
                if (conds.size() == 1) {
                    result.append(col).append("=").append(cond);
                    continue;
                }
                if (i == 0) {
                    result.append("(").append(col).append("=").append(cond);
                    continue;
                }
                if (i == conds.size() - 1) {
                    result.append(" OR ").append(col).append("=").append(cond).append(")");
                    continue;
                }
                result.append(" OR ").append(col).append("=").append(cond);
            }

            if (j != trimedCondsWithColumn.size() - 1) {
                result.append(" AND ");
            }
            j++;
        }
        return result.toString();
    }

    //add cond with single quote and escape single quote
    static Map<String, List<String>> trimConds(Map<String, List<String>> condsWithCol, Map<String, String> columnWithType) {
        Map<String, List<String>> result = new HashMap<>();
        for (String col : condsWithCol.keySet()) {
            List<String> conds = Lists.newArrayList(condsWithCol.get(col));
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type note found");
            trimStringType(conds, type);
            trimDateType(conds, type);
            result.put(col, conds);
        }
        return result;
    }

    private static void trimDateType(List<String> conds, String type) {
        if (isDateType(type)) {
            for (int i = 0; i < conds.size(); i++) {
                String cond = conds.get(i);
                if (type.equals("date")) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    cond = sdf.format(new Date(Long.valueOf(cond)));
                    cond = "DATE '" + cond + "'";
                }
                if (type.equals("timestamp") || type.equals("datetime")) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    cond = sdf.format(new Date(Long.valueOf(cond)));
                    cond = "TIMESTAMP '" + cond + "'";
                }
                if (type.equals("time")) {
                    final int TIME_START_POS = 11; //"1970-01-01 ".length() = 11
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    cond = sdf.format(new Date(Long.valueOf(cond)));
                    //transform "1970-01-01 00:00:59" into "00:00:59"
                    cond = "TIME '" + cond.substring(TIME_START_POS, cond.length()) + "'";
                }
                conds.set(i, cond);
            }
        }
    }

    private static boolean isDateType(String type) {
        final Set<String> DATETIME_FAMILY = Sets.newHashSet("date", "time", "datetime", "timestamp");
        return DATETIME_FAMILY.contains(type);
    }

    private static void trimStringType(List<String> conds, String type) {
        if (type.startsWith("varchar") || type.equals("string") || type.equals("char")) {
            for (int i = 0; i < conds.size(); i++) {
                String cond = conds.get(i);
                cond = cond.replaceAll("'", "''");
                cond = "'" + cond + "'";
                conds.set(i, cond);
            }
        }
    }

    private void validateNotExists(String username, String table, TableRowCondList tableRowCondList) {
        if (!tableRowCondList.getRowCondListByTable(table).isEmpty()) {
            throw new RuntimeException(
                    "Operation fail, user:" + username + ", table:" + table + " already in row cond list!");
        }
    }

    private void validateExists(String username, String table, TableRowCondList tableRowCondList) {
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

        public RowCondList removeTbl(String table) {
            return rowCondsWithTable.remove(table);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class RowCondList implements Serializable, IKeep {
        //all row conds in the table
        @JsonProperty()
        private Map<String, List<String>> condsWithColumn; //C1:{cond1, cond2},C2{cond1, cond3}

        private RowCondList() {
            this.condsWithColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        RowCondList(Map<String, List<String>> condsWithColumn) {
            this.condsWithColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.condsWithColumn.putAll(condsWithColumn);
        }

        public int size() {
            return condsWithColumn.size();
        }

        private boolean isEmpty() {
            return condsWithColumn.isEmpty();
        }

        List<String> getCondsByColumn(String column) {
            return condsWithColumn.get(column);
        }

        Map<String, List<String>> getCondsWithColumn() {
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
    static class QueryUsedCondList implements Serializable, IKeep {
        @JsonProperty()
        private Map<String, String> concatedCondsWithTable; //TABLE1: C1 = A OR C1 = B AND C2 = C

        QueryUsedCondList() {
            concatedCondsWithTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        public int size() {
            return concatedCondsWithTable.size();
        }

        public boolean isEmpty() {
            return concatedCondsWithTable.isEmpty();
        }

        Map<String, String> getConcatedCondsWithTable() {
            return concatedCondsWithTable;
        }

        String getConcatedCondsByTable(String table) {
            return concatedCondsWithTable.get(table);
        }

        public String removeTbl(String table) {
            return concatedCondsWithTable.remove(table);
        }

        void putConcatedConds(String table, String splicedConds) {
            concatedCondsWithTable.put(table, splicedConds);
        }
    }
}