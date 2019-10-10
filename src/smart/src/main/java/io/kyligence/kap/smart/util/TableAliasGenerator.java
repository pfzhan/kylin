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

package io.kyligence.kap.smart.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.model.NTableMetadataManager;

// Utility to generate shortest readable table alias
public class TableAliasGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TableAliasGenerator.class);

    public static TableAliasDict generateNewDict(String[] tableNames) {
        String[] sortedNames = new HashSet<String>(Arrays.asList(tableNames)).toArray(new String[0]);
        Arrays.sort(sortedNames);
        Map<String, List<String>> schemaMap = new HashMap<>();
        for (String tableIdentity : sortedNames) {
            if (StringUtils.isEmpty(tableIdentity)) {
                continue;
            }

            String schema = "N/A";
            int spliterIndex = tableIdentity.indexOf('.');
            if (spliterIndex >= 0) {
                schema = tableIdentity.substring(0, spliterIndex);
            }
            String table = tableIdentity.substring(spliterIndex + 1);

            if (!schemaMap.containsKey(schema)) {
                schemaMap.put(schema, new ArrayList<String>());
            }
            schemaMap.get(schema).add(table);
        }

        Map<String, String> schemaDict = schemaMap.size() > 1 ? quickDict(schemaMap.keySet().toArray(new String[0]))
                : null;
        Map<String, String> dict = new HashMap<>();
        for (Entry<String, List<String>> tables : schemaMap.entrySet()) {
            String schema = tables.getKey();
            Map<String, String> tableDict = quickDict(tables.getValue().toArray(new String[0]));
            for (Entry<String, String> tableAlias : tableDict.entrySet()) {
                String table = tableAlias.getKey();
                String alias = tableAlias.getValue();
                if (schemaDict != null) {
                    alias = schemaDict.get(schema) + "_" + alias;
                }
                if (!schema.equals("N/A")) {
                    table = schema + "." + table;
                }
                dict.put(table, alias);
            }
        }

        return new TableAliasDict(dict);
    }

    public static TableAliasDict generateCommonDictForSpecificModel(KylinConfig config, String project) {
        Map<String, TableDesc> allTablesMap = NTableMetadataManager.getInstance(config, project).getAllTablesMap();
        return generateNewDict(allTablesMap.keySet().toArray(new String[0]));
    }

    private static Map<String, String> quickDict(String[] sourceNames) {
        Map<String, String> dict = new HashMap<>();
        for (String name : sourceNames) {
            for (int i = 1; i <= name.length(); i++) {
                // TODO refine logic later
                String aliasCandidate = name.substring(0, i);
                if (dict.containsValue(aliasCandidate)) {
                    continue;
                } else {
                    dict.put(name, aliasCandidate);
                    break;
                }
            }
        }
        return dict;
    }

    public static class TableAliasDict {
        private Map<String, String> alias2TblName;
        private Map<String, String> tblName2Alias;

        public TableAliasDict() {
            this.alias2TblName = new HashMap<>();
            this.tblName2Alias = new HashMap<>();
        }

        public TableAliasDict(Map<String, String> dict) {
            this();
            for (Entry<String, String> pair : dict.entrySet()) {
                addPair(pair.getKey(), pair.getValue());
            }
        }

        public void addPair(String tableName, String alias) {
            if (alias2TblName.containsKey(alias)) {
                if (alias2TblName.get(alias).equals(tableName)) {
                    logger.debug("Table alias pair already defined: {} => {}", tableName, alias);
                    return;
                } else {
                    logger.debug("Alias {} is used by another table", alias);
                    return;
                }
            }
            if (tblName2Alias.containsKey(tableName)) {
                logger.debug("Table {} has been assigned another alias", tableName);
                return;
            }
            alias2TblName.put(alias, tableName);
            tblName2Alias.put(tableName, alias);
        }

        public String getAlias(String tableName) {
            return tblName2Alias.get(tableName);
        }

        public String getTableName(String alias) {
            return alias2TblName.get(alias);
        }

        public String getHierachyAlias(String[] tables) {
            if (tables.length == 0) {
                return "";
            }
            StringBuilder hAlias = new StringBuilder(getAlias(tables[0]));
            for (int i = 1; i < tables.length; i++) {
                hAlias.append("_").append(getAlias(tables[i]));
            }
            return hAlias.toString();
        }
    }
}
