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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/*remember add         Preconditions.checkNotNull(columnName, "columnName is null");
        Preconditions.checkState(tableIdentity.equals(tableIdentity.trim()),
                "tableIdentity of ComputedColumnDesc has heading/tailing whitespace");
*/
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RowACL extends RootPersistentEntity {
    //cuz row acl is complicated, so stored two data, one for frontend to display, another one for backend to use to filter query
    //For frontend.USER :{DB.TABLE1:{COLUMN1:{a,b,c}, COLUMN2:{d,e,f}}, DB.TABLE2:{COLUMN1:{a,b,c}, COLUMN3:{m,n}}}
    @JsonProperty()
    private Map<String, TableRowCondList> tableRowCondsWithUser;
    // For backend query row filter used
    @JsonProperty()
    private Map<String, QueryUsedCondList> queryUsedCondsWithTable;

    public RowACL() {
        tableRowCondsWithUser = new HashMap<>();
        queryUsedCondsWithTable = new HashMap<>();
    }

    public Map<String, TableRowCondList> getTableRowCondsWithUser() {
        return tableRowCondsWithUser;
    }

    public Map<String, QueryUsedCondList> getQueryUsedConds() {
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
        return queryUsedCondList.getSplicedCondsWithTable();
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
        tableRowConds.remove(table);
        queryUsedConds.remove(table);
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
        queryUsedConds.putSplicedConds(table, concatConds(condsWithColumn, columnWithType));
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
                if (i == 0) {
                    result.append(col).append("=").append(cond);
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
    private static  Map<String, List<String>> trimConds(Map<String, List<String>> condsWithCol, Map<String, String> columnWithType) {
        Map<String, List<String>> result = new HashMap<>();
        for (String col : condsWithCol.keySet()) {
            List<String> conds = condsWithCol.get(col);
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type note found");
            if (type.startsWith("varchar") || type.equals("string") || type.equals("char")) {
                List<String> trimedConds = new ArrayList<>();
                for (String cond : conds) {
                    cond = cond.replaceAll("'", "''");
                    cond = "'" + cond + "'";
                    trimedConds.add(cond);
                }
                result.put(col, trimedConds);
                continue;
            }
            result.put(col, conds);
        }
        return result;
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

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class TableRowCondList implements Serializable {
        //all row conds in the table
        @JsonProperty()
        private Map<String, RowCondList> rowCondsWithTable; //t1:rowCondList1, t2:rowCondList2

        private TableRowCondList() {
            rowCondsWithTable = new HashMap<>();
        }

        public int size() {
            return rowCondsWithTable.size();
        }

        public boolean isEmpty() {
            return rowCondsWithTable.isEmpty();
        }

        public RowCondList getRowCondListByTable(String table) {
            RowCondList rowCondList = rowCondsWithTable.get(table);
            if (rowCondList == null) {
                rowCondList = new RowCondList();
            }
            return rowCondList;
        }

        public RowCondList putRowCondlist(String key, RowCondList value) {
            return rowCondsWithTable.put(key, value);
        }

        public RowCondList remove(String table) {
            return rowCondsWithTable.remove(table);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class RowCondList implements Serializable {
        //all row conds in the table
        @JsonProperty()
        private Map<String, List<String>> condsWithColumn; //c1:{cond1, cond2},c2{cond1, cond3}

        private RowCondList() {
            this.condsWithColumn = new HashMap<>();
        }

        public RowCondList(Map<String, List<String>> condsWithColumn) {
            this.condsWithColumn = condsWithColumn;
        }

        public int size() {
            return condsWithColumn.size();
        }

        private boolean isEmpty() {
            return condsWithColumn.isEmpty();
        }

        private boolean containsKey(String key) {
            return condsWithColumn.containsKey(key);
        }

        List<String> getCondsByColumn(String key) {
            return condsWithColumn.get(key);
        }

        public Map<String, List<String>> getCondsWithColumn() {
            if (condsWithColumn == null) {
                condsWithColumn = new HashMap<>();
            }
            return condsWithColumn;
        }

        void put(String column, List<String> conds) {
            condsWithColumn.put(column, conds);
        }

        public Set<String> keySet() {
            return condsWithColumn.keySet();
        }

        void remove(String key) {
            condsWithColumn.remove(key);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    static class QueryUsedCondList {
        @JsonProperty()
        private Map<String, String> splicedCondsWithTable; //TABLE1: C1 = A OR C1 = B AND C2 = C

        public QueryUsedCondList() {
            splicedCondsWithTable = new HashMap<>();
        }

        public int size() {
            return splicedCondsWithTable.size();
        }

        public boolean isEmpty() {
            return splicedCondsWithTable.isEmpty();
        }

        public boolean containsKey(String table) {
            return splicedCondsWithTable.containsKey(table);
        }

        public Map<String, String> getSplicedCondsWithTable() {
            return splicedCondsWithTable;
        }

        public String getSplicedCondsByTable(String table) {
            return splicedCondsWithTable.get(table);
        }

        public String remove(String table) {
            return splicedCondsWithTable.remove(table);
        }

        public String putSplicedConds(String table, String splicedConds) {
            return splicedCondsWithTable.put(table, splicedConds);
        }
    }
}