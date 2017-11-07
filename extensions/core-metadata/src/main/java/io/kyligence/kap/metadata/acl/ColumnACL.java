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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.CaseInsensitiveStringSet;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeep;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnACL extends RootPersistentEntity implements IKeep {
    @JsonProperty()
    private ColumnACLEntry userColumnBlackList = new ColumnACLEntry();
    @JsonProperty()
    private ColumnACLEntry groupColumnBlackList = new ColumnACLEntry();

    private ColumnACLEntry currentEntry(String type) {
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            return userColumnBlackList;
        } else {
            return groupColumnBlackList;
        }
    }

    // USER1:[DB.TABLE.COLUMN1, DB.TABLE.COLUMN2], only for column interceptor to use to intercept query.
    public Set<String> getColumnBlackList(String username, Set<String> groups) {
        Set<String> tableBlackList = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableBlackList.addAll(userColumnBlackList.getColumnBlackList(username));
        //if user is in group, add group's black list
        for (String group : groups) {
            tableBlackList.addAll(groupColumnBlackList.getColumnBlackList(group));
        }
        return tableBlackList;
    }

    // TABLE :{USER1:[COLUMN1, COLUMN2], USER2:[COLUMN1, COLUMN3]}, only for frontend to display
    public Map<String, Set<String>> getColumnBlackListByTable(String table, String type) {
        return currentEntry(type).getColumnBlackListByTable(table);
    }

    //get available users only for frontend to select to add column ACL.
    public List<String> getCanAccessList(String table, Set<String> allIdentifiers, String type) {
        return currentEntry(type).getCanAccessList(table, allIdentifiers);
    }

    public boolean contains(String name, String type) {
        return currentEntry(type).containsKey(name);
    }

    public int size() {
        return userColumnBlackList.size() + groupColumnBlackList.size();
    }

    public int size(String type) {
        return currentEntry(type).size();
    }

    public ColumnACL add(String username, String table, Set<String> columns, String type) {
        currentEntry(type).add(username, table, columns);
        return this;
    }

    public ColumnACL update(String username, String table, Set<String> columns, String type) {
        currentEntry(type).update(username, table, columns);
        return this;
    }

    public ColumnACL delete(String username, String table, String type) {
        currentEntry(type).delete(username, table);
        return this;
    }

    public ColumnACL delete(String username, String type) {
        currentEntry(type).delete(username);
        return this;
    }

    ColumnACL deleteByTbl(String table) {
        userColumnBlackList.deleteByTbl(table);
        groupColumnBlackList.deleteByTbl(table);
        return this;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class ColumnACLEntry extends HashMap<String, ColumnBlackList> implements Serializable, IKeep {

        private Set<String> getColumnBlackList(String name) {
            ColumnBlackList columnBlackList = super.get(name);
            if (columnBlackList == null) {
                columnBlackList = new ColumnBlackList();
            }
            return columnBlackList.getColumnsWithTblPrefix();
        }

        // TABLE :{USER1:[COLUMN1, COLUMN2], USER2:[COLUMN1, COLUMN3]}, only for frontend to display
        private Map<String, Set<String>> getColumnBlackListByTable(String table) {
            Map<String, Set<String>> results = new HashMap<>();
            for (String user : super.keySet()) {
                ColumnBlackList columnsWithTbl = super.get(user);
                Set<String> columns = columnsWithTbl.getColumnBlackListByTbl(table);
                if (columns.size() > 0) {
                    results.put(user, columns);
                }
            }
            return results;
        }

        private List<String> getCanAccessList(String table, Set<String> allIdentifiers) {
            List<String> whiteList = Lists.newArrayList(allIdentifiers);
            Set<String> blocked = getColumnBlackListByTable(table).keySet();
            whiteList.removeAll(blocked);
            return whiteList;
        }

        private void add(String username, String table, Set<String> columns) {
            if (columns.size() == 0) {
                return;
            }

            ColumnBlackList columnBlackList = super.get(username);
            if (columnBlackList == null) {
                columnBlackList = new ColumnBlackList();
                super.put(username, columnBlackList);
            }
            validateACLNotExists(username, table, columnBlackList);
            columnBlackList.putColumns(table, columns);
        }

        private void update(String username, String table, Set<String> columns) {
            if (columns.size() == 0) {
                return;
            }

            ColumnBlackList columnsWithTbl = super.get(username);
            validateACLExists(username, table, columnsWithTbl);
            columnsWithTbl.putColumns(table, columns);
        }

        private void delete(String username, String table) {
            ColumnBlackList columnsWithTbl = super.get(username);
            validateACLExists(username, table, columnsWithTbl);
            columnsWithTbl.removeByTbl(table);
            if (columnsWithTbl.isEmpty()) {
                super.remove(username);
            }
        }

        private void delete(String username) {
            if (super.get(username) == null) {
                throw new RuntimeException("Operation fail, user:" + username + " is not found in column black list");
            }
            super.remove(username);
        }

        private void deleteByTbl(String table) {
            Iterator<Map.Entry<String, ColumnBlackList>> it = super.entrySet().iterator();
            while (it.hasNext()) {
                ColumnBlackList columnBlackList = it.next().getValue();
                columnBlackList.removeByTbl(table);
                if (columnBlackList.isEmpty()) {
                    it.remove();
                }
            }
        }

        private void validateACLNotExists(String username, String table, ColumnBlackList columnBlackList) {
            if (columnBlackList.containsTbl(table)) {
                throw new RuntimeException("Operation fail, user:" + username + " already in table's columns blacklist!");
            }
        }

        private void validateACLExists(String username, String table, ColumnBlackList columnBlackList) {
            if (columnBlackList == null || (!columnBlackList.containsTbl(table))) {
                throw new RuntimeException("Operation fail, user:" + username + " has no column ACL");
            }
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class ColumnBlackList implements Serializable, IKeep {
        //{DB.TABLE1:[COL1, COL2]}
        @JsonProperty()
        private CaseInsensitiveStringMap<CaseInsensitiveStringSet> columnsWithTable = new CaseInsensitiveStringMap<>();

        private boolean isEmpty() {
            return columnsWithTable.isEmpty();
        }

        private boolean containsTbl(String table) {
            return columnsWithTable.containsKey(table);
        }

        private Set<String> getColumnBlackListByTbl(String table) {
            Set<String> blackList = columnsWithTable.get(table);
            if (blackList == null) {
                return new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            }
            Set<String> copy = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            copy.addAll(blackList);
            return copy;
        }

        private void putColumns(String table, Set<String> columns) {
            columnsWithTable.put(table, new CaseInsensitiveStringSet(columns));
        }

        private void removeByTbl(String table) {
            columnsWithTable.remove(table);
        }

        private Set<String> getColumnsWithTblPrefix() {
            Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (String tbl : columnsWithTable.keySet()) {
                Set<String> cols = columnsWithTable.get(tbl);
                for (String col : cols) {
                    result.add(tbl + "." + col);
                }
            }
            return result;
        }
    }
}