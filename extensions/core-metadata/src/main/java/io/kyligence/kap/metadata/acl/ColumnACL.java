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

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnACL extends RootPersistentEntity {
    @JsonProperty()
    private Map<String, ColumnBlackList> userColumnBlackList; // USER :{DB.TABLE1:{COLUMN1, COLUMN2}, DB.TABLE2:{COLUMN1, COLUMN3}}

    public ColumnACL() {
        userColumnBlackList = new HashMap<>();
    }

    public Map<String, ColumnBlackList> getUserColumnBlackList() {
        return userColumnBlackList;
    }

    // TABLE :{USER1:[COLUMN1, COLUMN2], USER2:[COLUMN1, COLUMN3]}, only for frontend to display
    public Map<String, List<String>> getColumnBlackListByTable(String table) {
        Map<String, List<String>> results = new HashMap<>();
        for (String user : userColumnBlackList.keySet()) {
            ColumnBlackList columnsWithTable = userColumnBlackList.get(user);
            List<String> columns = columnsWithTable.getColumnBlackListByTable(table);
            if (columns != null && columns.size() > 0) {
                results.put(user, columns);
            }
        }
        return results;
    }

    // USER1:{[DB.TABLE.COLUMN1, DB.TABLE.COLUMN2], DB.TABLE.COLUMN1, DB.TABLE.COLUMN3]}, only for column filter to use to intercept query.
    public List<String> getColumnBlackListByUser(String username) {
        ColumnBlackList columnBlackList = userColumnBlackList.get(username);
        if (columnBlackList == null) {
            columnBlackList = new ColumnBlackList();
        }
        return columnBlackList.getColumnsWithTablePrefix();
    }
    
    public ColumnACL add(String username, String table, List<String> columns) {
        if (userColumnBlackList == null) {
            userColumnBlackList = new HashMap<>();
        }

        if (columns.size() == 0) {
            return this;
        }

        ColumnBlackList columnBlackList = userColumnBlackList.get(username);

        if (columnBlackList == null) {
            columnBlackList = new ColumnBlackList();
            userColumnBlackList.put(username, columnBlackList);
        }

        if (columnBlackList.containsKey(table)) {
            throw new RuntimeException("Operation fail, user:" + username + " already in table's columns blacklist!");
        }

        columnBlackList.putColumnsToTable(table, columns);
        return this;
    }

    public ColumnACL update(String username, String table, List<String> columns) {
        if (userColumnBlackList == null) {
            userColumnBlackList = new HashMap<>();
        }

        if (columns.size() == 0) {
            return this;
        }

        ColumnBlackList columnBlackList = userColumnBlackList.get(username);

        if (columnBlackList == null || (!columnBlackList.containsKey(table))) {
            throw new RuntimeException("Operation fail, user:" + username + " not found in table's columns blacklist!");
        }

        columnBlackList.putColumnsToTable(table, columns);
        return this;
    }

    public ColumnACL delete(String username, String table) {
        if (isColumnInBlackList(username, table)) {
            throw new RuntimeException("Operation fail, user:" + username + " is not found in column black list");
        }
        ColumnBlackList columnBlackList = userColumnBlackList.get(username);
        columnBlackList.remove(table);
        return this;
    }

    private boolean isColumnInBlackList(String username, String table) {
        return  userColumnBlackList == null
                || userColumnBlackList.get(username) == null
                || (!userColumnBlackList.get(username).containsKey(table));
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
     static class ColumnBlackList implements Serializable {
        @JsonProperty()
        Map<String, List<String>> columnsWithTable; //{DB.TABLE1:[COL1, COL2]}

        private ColumnBlackList() {
            this.columnsWithTable = new HashMap<>();
        }

        public int size() {
            return columnsWithTable.size();
        }

        private boolean containsKey(String column) {
            return columnsWithTable.containsKey(column);
        }

        List<String> getColumnBlackListByTable(String table) {
            return columnsWithTable.get(table);
        }

        private void putColumnsToTable(String table, List<String> columns) {
            columnsWithTable.put(table, columns);
        }

        private void remove(String table) {
            columnsWithTable.remove(table);
        }

        List<String> getColumnsWithTablePrefix() {
            List<String> result = new ArrayList<>();
            for (String tbl : columnsWithTable.keySet()) {
                List<String> cols = columnsWithTable.get(tbl);
                for (String col : cols) {
                    result.add(tbl + "." + col);
                }
            }
            return result;
        }
    }
}