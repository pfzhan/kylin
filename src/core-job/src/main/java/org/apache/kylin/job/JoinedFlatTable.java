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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;

public class JoinedFlatTable {

    private static final String DATABASE_AND_TABLE = "%s.%s";

    private static final String QUOTE = "`";
    private static final String UNDER_LINE = "_";
    private static final String DOT = ".";

    private JoinedFlatTable() {
    }

    private static String quote(String identifier) {
        return QUOTE + identifier + QUOTE;
    }

    private static String colName(TblColRef col) {
        return col.getTableAlias() + UNDER_LINE + col.getName();
    }

    private static String quotedTable(TableDesc table) {
        if (table.getCaseSensitiveDatabase().equals("null")) {
            return quote(table.getCaseSensitiveName().toUpperCase());
        }
        return String.format(DATABASE_AND_TABLE, quote(table.getCaseSensitiveDatabase().toUpperCase()),
                quote(table.getCaseSensitiveName().toUpperCase()));
    }

    private static String quotedColExpressionInSourceDB(NDataModel modelDesc, TblColRef col) {
        if (!col.getColumnDesc().isComputedColumn()) {
            return quote(col.getTableAlias()) + DOT + quote(col.getName());
        } else {
            return quoteIdentifierInSqlExpr(modelDesc, col.getColumnDesc().getComputedColumnExpr(), QUOTE);
        }
    }

    private static String appendEffectiveColumnsStatement(NDataModel modelDesc, boolean singleLine) {
        final String sep = getSepBySingleLineTag(singleLine);

        StringBuilder subSql = new StringBuilder();
        for (Map.Entry<Integer, TblColRef> tableRefEntry : modelDesc.getEffectiveCols().entrySet()) {
            TblColRef col = tableRefEntry.getValue();
            if (subSql.length() > 0) {
                subSql.append(",").append(sep);
            }

            String colName = colName(col);
            subSql.append(quotedColExpressionInSourceDB(modelDesc, col)).append(" as ").append(quote(colName));
        }

        return subSql.toString();
    }

    private static String appendWhereStatement(NDataModel modelDesc, boolean singleLine) {
        final String sep = getSepBySingleLineTag(singleLine);

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("1=1").append(sep);

        if (StringUtils.isNotEmpty(modelDesc.getFilterCondition())) {
            String quotedFilterCondition = quoteIdentifierInSqlExpr(modelDesc, modelDesc.getFilterCondition(), QUOTE);
            whereBuilder.append(" AND (").append(quotedFilterCondition).append(") ").append(sep); // -> filter condition contains special character may cause bug
        }

        return whereBuilder.toString();
    }

    private static String appendJoinStatement(NDataModel modelDesc, boolean singleLine) {
        final String sep = getSepBySingleLineTag(singleLine);

        StringBuilder subSql = new StringBuilder();

        Set<TableRef> dimTableCache = new HashSet<>();
        TableRef rootTable = modelDesc.getRootFactTable();
        subSql.append(quotedTable(modelDesc.getRootFactTable().getTableDesc())).append(" as ")
                .append(quote(rootTable.getAlias())).append(" ").append(sep);

        for (JoinTableDesc lookupDesc : modelDesc.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (checkJoinDesc(join)) {
                continue;
            }
            String joinType = join.getType().toUpperCase();
            TableRef dimTable = lookupDesc.getTableRef();
            if (dimTableCache.contains(dimTable)) {
                continue;
            }
            subSql.append(joinType).append(" JOIN ").append(quotedTable(dimTable.getTableDesc())).append(" as ")
                    .append(quote(dimTable.getAlias())).append(sep);
            subSql.append("ON ");

            if (Objects.nonNull(join.getNonEquiJoinCondition())) {
                subSql.append(quoteIdentifierInSqlExpr(modelDesc, join.getNonEquiJoinCondition().getExpr(), QUOTE));
            } else {
                TblColRef[] pk = join.getPrimaryKeyColumns();
                TblColRef[] fk = join.getForeignKeyColumns();
                if (pk.length != fk.length) {
                    throw new RuntimeException(String.format("Invalid join condition of lookup table: %s", lookupDesc));
                }

                for (int i = 0; i < pk.length; i++) {
                    if (i > 0) {
                        subSql.append(" AND ");
                    }
                    subSql.append(quotedColExpressionInSourceDB(modelDesc, fk[i])).append("=")
                            .append(quotedColExpressionInSourceDB(modelDesc, pk[i]));
                }
            }
            subSql.append(sep);
            dimTableCache.add(dimTable);
        }
        return subSql.toString();
    }

    private static String getSepBySingleLineTag(boolean singleLine) {
        return singleLine ? " " : "\n";
    }

    public static String generateSelectDataStatement(NDataModel modelDesc, boolean singleLine) {
        final String sep = getSepBySingleLineTag(singleLine);

        StringBuilder sql = new StringBuilder("SELECT ").append(sep);
        String columnsStatement = appendEffectiveColumnsStatement(modelDesc, singleLine);
        sql.append(columnsStatement.endsWith(sep) ? columnsStatement : columnsStatement + sep);
        sql.append("FROM ").append(sep);
        String joinStatement = appendJoinStatement(modelDesc, singleLine);
        sql.append(joinStatement.endsWith(sep) ? joinStatement : joinStatement + sep);
        sql.append("WHERE ").append(sep);
        sql.append(appendWhereStatement(modelDesc, singleLine));
        return sql.toString();
    }

    private static boolean checkJoinDesc(JoinDesc join) {
        return join == null || join.getType().equals("");
    }

    private static String getColumnAlias(String tableName, String columnName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        if (!colToAliasMap.containsKey(columnName)) {
            return null;
        }
        return colToAliasMap.get(columnName);
    }

    private static boolean columnHasAlias(String tableName, String columnName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        return colToAliasMap.containsKey(columnName);
    }

    private static Map<String, String> getColToColAliasMapInTable(String tableName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        if (tableToColumnsMap.containsKey(tableName)) {
            return tableToColumnsMap.get(tableName);
        }
        return Maps.newHashMap();
    }

    private static Set<String> listColumnsInTable(String tableName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        return colToAliasMap.keySet();
    }

    @VisibleForTesting
    static String quoteIdentifier(String sqlExpr, String quotation, String identifier,
            List<String> identifierPatterns) {
        String quotedIdentifier = quotation + identifier.trim() + quotation;

        for (String pattern : identifierPatterns) {
            Matcher matcher = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(sqlExpr);
            if (matcher.find()) {
                sqlExpr = matcher.replaceAll("$1" + quotedIdentifier + "$3");
            }
        }
        return sqlExpr;
    }

    private static boolean isIdentifierNeedToQuote(String sqlExpr, String identifier, List<String> identifierPatterns) {
        if (StringUtils.isBlank(sqlExpr) || StringUtils.isBlank(identifier)) {
            return false;
        }

        for (String pattern : identifierPatterns) {
            if (Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(sqlExpr).find()) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    static List<String> getTableNameOrAliasPatterns(String tableName) {
        Preconditions.checkNotNull(tableName);
        // Pattern must contain three regex groups, and place identifier in sec group ($2)
        List<String> patterns = Lists.newArrayList();
        patterns.add("([+\\-*/%&|^=><\\s,(])(" + tableName.trim() + ")(\\.)");
        patterns.add("([\\.\\s])(" + tableName.trim() + ")([,\\s)])");
        patterns.add("(^)(" + tableName.trim() + ")([\\.])");
        return patterns;
    }

    @VisibleForTesting
    static List<String> getColumnNameOrAliasPatterns(String colName) {
        Preconditions.checkNotNull(colName);
        // Pattern must contain three regex groups, and place identifier in sec group ($2)
        List<String> patterns = Lists.newArrayList();
        patterns.add("([\\.\\s(])(" + colName.trim() + ")([+\\-*/%&|^=><\\s,)]|$)");
        patterns.add("(^)(" + colName.trim() + ")([+\\-*/%&|^=><\\s,)])");
        return patterns;
    }

    private static Map<String, Map<String, String>> buildTableToColumnsMap(NDataModel modelDesc) {
        Map<String, Map<String, String>> map = Maps.newHashMap();
        Set<TblColRef> colRefs = modelDesc.getEffectiveCols().values();
        for (TblColRef colRef : colRefs) {
            String colName = colRef.getName();
            String tableName = colRef.getTableRef().getTableName();
            String colAlias = colRef.getTableAlias() + "_" + colRef.getName();
            if (map.containsKey(tableName)) {
                map.get(tableName).put(colName, colAlias);
            } else {
                Map<String, String> colToAliasMap = Maps.newHashMap();
                colToAliasMap.put(colName, colAlias);
                map.put(tableName, colToAliasMap);
            }
        }
        return map;
    }

    private static Map<String, String> buildTableToTableAliasMap(NDataModel modelDesc) {
        Map<String, String> map = Maps.newHashMap();
        Set<TblColRef> colRefs = modelDesc.getEffectiveCols().values();
        for (TblColRef colRef : colRefs) {
            String tableName = colRef.getTableRef().getTableName();
            String alias = colRef.getTableAlias();
            map.put(tableName, alias);
        }
        return map;
    }

    /**
     * Used for quote identifiers in Sql Filter Expression & Computed Column Expression for flat table
     * @param modelDesc
     * @param sqlExpr
     * @param quotation
     * @return
     */
    @VisibleForTesting
    static String quoteIdentifierInSqlExpr(NDataModel modelDesc, String sqlExpr, String quotation) {
        Map<String, String> tabToAliasMap = buildTableToTableAliasMap(modelDesc);
        Map<String, Map<String, String>> tabToColsMap = buildTableToColumnsMap(modelDesc);

        boolean tableMatched = false;
        for (Map.Entry<String, String> tableEntry : tabToAliasMap.entrySet()) {
            List<String> tabPatterns = getTableNameOrAliasPatterns(tableEntry.getKey());
            if (isIdentifierNeedToQuote(sqlExpr, tableEntry.getKey(), tabPatterns)) {
                sqlExpr = quoteIdentifier(sqlExpr, quotation, tableEntry.getKey(), tabPatterns);
                tableMatched = true;
            }

            String tabAlias = tableEntry.getValue();
            List<String> tabAliasPatterns = getTableNameOrAliasPatterns(tabAlias);
            if (isIdentifierNeedToQuote(sqlExpr, tabAlias, tabAliasPatterns)) {
                sqlExpr = quoteIdentifier(sqlExpr, quotation, tabAlias, tabAliasPatterns);
                tableMatched = true;
            }

            if (!tableMatched) {
                continue;
            }

            Set<String> columns = listColumnsInTable(tableEntry.getKey(), tabToColsMap);
            for (String column : columns) {
                List<String> colPatterns = getColumnNameOrAliasPatterns(column);
                if (isIdentifierNeedToQuote(sqlExpr, column, colPatterns)) {
                    sqlExpr = quoteIdentifier(sqlExpr, quotation, column, colPatterns);
                }
                if (columnHasAlias(tableEntry.getKey(), column, tabToColsMap)) {
                    String colAlias = getColumnAlias(tableEntry.getKey(), column, tabToColsMap);
                    List<String> colAliasPattern = getColumnNameOrAliasPatterns(colAlias);
                    if (isIdentifierNeedToQuote(sqlExpr, colAlias, colAliasPattern)) {
                        sqlExpr = quoteIdentifier(sqlExpr, quotation, colAlias, colPatterns);
                    }
                }
            }

            tableMatched = false; //reset
        }
        return sqlExpr;
    }
}
