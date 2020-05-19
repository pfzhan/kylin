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

package io.kyligence.kap.query.security;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class TableViewPrepender implements QueryUtil.IQueryTransformer, IPushDownConverter, IKeep {
    private static final int WITH_CLAUSE_LENGTH = 4;

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KylinConfig.getInstanceFromEnv().isAclTCREnabled() || hasAdminPermission())
            return sql;

        val filters = getAclTCRManager(project).getTableColumnConcatWhereCondition(getUserName(), getGroups());
        return prependTableViews(sql, defaultSchema, filters, ArrayListMultimap.create());
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema, boolean isPrepare) {
        Map<String, String> filters = Maps.newHashMap();
        Multimap<String, String> authorizedColumns = ArrayListMultimap.create();
        if (KylinConfig.getInstanceFromEnv().isAclTCREnabled() && !hasAdminPermission()) {
            filters = getAclTCRManager(project).getTableColumnConcatWhereCondition(getUserName(), getGroups());
            authorizedColumns = getAclTCRManager(project).getAuthorizedColumnsGroupByTable(getUserName(), getGroups());
        }

        // todo: group cc by table after cc redesign
        return prependTableViews(originSql, defaultSchema, filters, authorizedColumns);
    }

    private static String getUserName() {
        return QueryContext.current().getUsername();
    }

    private static Set<String> getGroups() {
        return QueryContext.current().getGroups();
    }

    private static boolean hasAdminPermission() {
        val context = QueryContext.current();
        if (Objects.isNull(context) || Objects.isNull(context.getGroups())) {
            return false;
        }
        return context.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals) || context.isHasAdminPermission();
    }

    private AclTCRManager getAclTCRManager(String project) {
        return AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    private static Map<String, String> getAllUsedTableViews(Set<String> usedTables, Map<String, String> filters, Multimap<String, String> columns) {
        val tableViews = Maps.<String, String>newHashMap();

        val allTables = Sets.<String>newHashSet();
        allTables.addAll(filters.keySet());
        allTables.addAll(columns.keySet());
        usedTables.retainAll(allTables);
        usedTables.forEach(tableIdentity -> {
            val sb = new StringBuilder("SELECT ");
            sb.append(constructSelectColumns(columns.get(tableIdentity)));
            sb.append(" FROM ");
            String[] tableIdentitySplit = tableIdentity.split("\\.");
            String database = tableIdentitySplit[0];
            if (database.equalsIgnoreCase("DEFAULT")) {
                sb.append("\"DEFAULT\"" + "." + tableIdentity.split("\\.")[1]);
            } else {
                sb.append(tableIdentity);
            }
            sb.append(constructFilterCondition(filters.get(tableIdentity)));
            tableViews.put(tableIdentity, sb.toString());
        });

        return tableViews;
    }

    private static String constructSelectColumns(Collection<String> columns) {
        if (columns == null || CollectionUtils.isEmpty(columns))
            return "*";

        return StringUtils.join(columns, ", ");
    }

    private static String constructFilterCondition(String filterCond) {
        if (StringUtils.isEmpty(filterCond))
            return "";

        return " WHERE " + filterCond;
    }

    public static String prependTableViews(String sql, String defaultSchema,
                                           Map<String, String> filters,
                                           Multimap<String, String> columns) {
        if (filters.isEmpty() && columns.isEmpty()) {
            return sql;
        }

        // parse sql first
        val tableIdentifierFinder = new TableIdentifierFinder(sql);
        val allTableIdentifiers = tableIdentifierFinder.getTableIdentifiers();
        val withClauses = tableIdentifierFinder.getWithClauses();

        // extract needed info
        val usedTables = Sets.<String>newHashSet();
        val realTableIdentifiers = Lists.<SqlIdentifier>newArrayList();
        collectNeededInfo(defaultSchema, allTableIdentifiers, withClauses, usedTables, realTableIdentifiers);

        val tableViews = getAllUsedTableViews(usedTables, filters, columns);
        if (tableViews.isEmpty())
            return sql;

        // replace table alias on original sql
        val replacedResult = replaceSqlBody(defaultSchema, sql, realTableIdentifiers, withClauses, tableViews);

        // prepend table views
        val replacedSql = replacedResult.getFirst();
        val tableAliasMap = replacedResult.getSecond();
        val withClausePrefix = generateWithClausePrefix(tableViews, tableAliasMap, withClauses.keySet());
        val sb = new StringBuilder();
        sb.append(withClausePrefix);
        if (withClauses.isEmpty()) {
            sb.append(" ");
            sb.append(replacedSql);
        } else {
            sb.append(", ");
            sb.append(replacedSql.trim().substring(WITH_CLAUSE_LENGTH, replacedSql.length()));
        }

        return sb.toString();
    }

    private static String generateWithClausePrefix(Map<String, String> tableViews, Map<String, String> tableAliasMap, Set<String> withAlias) {
        val sb = new StringBuilder("WITH ");
        for (Map.Entry<String, String> tableView : tableViews.entrySet()) {
            val tableIdentity = tableView.getKey();
            val selectQuery = tableView.getValue();

            var alias = tableAliasMap.get(tableIdentity);
            if (alias == null) {
                val tableName = tableIdentity.split("\\.")[1];
                if (withAlias.contains(tableName)) {
                    alias = getRandomAlias(tableName);
                } else {
                    alias = tableName;
                }
            }
            sb.append(alias);
            sb.append(" AS (");
            sb.append(selectQuery);
            sb.append("), ");
        }

        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    private static Pair<String, Map<String, String>> replaceSqlBody(String defaultSchema, String sql, 
                                                                    List<SqlIdentifier> realTableIdentifiers,
                                                                    Map<String, List<SqlIdentifier>> withClauses,
                                                                    Map<String, String> tableViews) {
        // 1. contains two tables with same table name but in different schema
        // 2. with clause has the same alias with table view
        val sqlIdentifierWithSameTableName = findSameTableNameInDifferentSchema(defaultSchema, realTableIdentifiers, tableViews);
        val conflictAliasOnWith = findExistedTableNameAliasInWithClause(defaultSchema, withClauses, tableViews.keySet());

        val tableIdentifiersToBeReplaced = Sets.<SqlIdentifier>newHashSet();
        tableIdentifiersToBeReplaced.addAll(sqlIdentifierWithSameTableName);
        tableIdentifiersToBeReplaced.addAll(conflictAliasOnWith);

        if (tableIdentifiersToBeReplaced.isEmpty())
            return Pair.newPair(sql, Maps.<String, String>newHashMap());

        val sortedResult = tableIdentifiersToBeReplaced.stream().map(tobeReplaced -> {
            Pair<Integer, Integer> position = CalciteParser.getReplacePos(tobeReplaced, sql);
            return Pair.newPair(tobeReplaced, position);
        }).sorted((o1, o2) -> o2.getSecond().getFirst().compareTo(o1.getSecond().getFirst())).collect(Collectors.toList());

        var result = sql;
        val tableAliasMap = Maps.<String, String>newHashMap();

        for (Pair<SqlIdentifier, Pair<Integer, Integer>> toBeReplaced : sortedResult) {
            val tableIdentifier = toBeReplaced.getFirst();
            val position = toBeReplaced.getSecond();
            val tableInfo = getTableInfo(defaultSchema, tableIdentifier);
            if (tableInfo == null)
                continue;

            val tableIdentity = tableInfo.getFirst() + "." + tableInfo.getSecond();
            tableAliasMap.putIfAbsent(tableIdentity, getRandomAlias(tableInfo.getSecond()));
            val start = position.getFirst();
            val end = position.getSecond();
            result = result.substring(0, start) + tableAliasMap.get(tableIdentity) + result.substring(end);
            log.debug("Table {} is using alias {}", tableIdentity, tableAliasMap.get(tableIdentity));
        }

        return Pair.newPair(result, tableAliasMap);
    }

    private static List<SqlIdentifier> findExistedTableNameAliasInWithClause(String defaultSchema,
                                                                             Map<String, List<SqlIdentifier>> withClauses,
                                                                             Set<String> tableViewIdentities) {
        if (withClauses.isEmpty())
            return Lists.newArrayList();

        val tableNames = tableViewIdentities.stream()
                .map(tableIdentity -> tableIdentity.split("\\.")[1])
                .collect(Collectors.toSet());
        val tableToBeReplaced = Lists.<SqlIdentifier>newArrayList();

        for (Map.Entry<String, List<SqlIdentifier>> withClause : withClauses.entrySet()) {
            val alias = withClause.getKey();
            if (!tableNames.contains(alias))
                continue;

            for (SqlIdentifier tableIdentifier : withClause.getValue()) {
                val tableInfo = getTableInfo(defaultSchema, tableIdentifier);
                if (tableInfo == null)
                    continue;

                val database = tableInfo.getFirst();
                val tableName = tableInfo.getSecond();
                val tableIdentity = database + "." + tableName;
                if (tableViewIdentities.contains(tableIdentity) && tableName.equalsIgnoreCase(alias)) {
                    tableToBeReplaced.add(tableIdentifier);
                }
            }
        }

        return tableToBeReplaced;
    }

    private static List<SqlIdentifier> findSameTableNameInDifferentSchema(String defaultSchema,
                                                                          List<SqlIdentifier> tableIdentifiers,
                                                                          Map<String, String> tableViews) {
        val tableToBeReplaced = Lists.<SqlIdentifier>newArrayList();
        val tableNameMap = Maps.<String, Map<String, List<SqlIdentifier>>>newHashMap();
        for (SqlIdentifier tableIdentifier : tableIdentifiers) {
            val tableInfo = getTableInfo(defaultSchema, tableIdentifier);

            if (tableInfo != null) {
                val database = tableInfo.getFirst();
                val tableName = tableInfo.getSecond();
                tableNameMap.putIfAbsent(tableName, Maps.newHashMap());
                tableNameMap.get(tableName).putIfAbsent(database, Lists.newArrayList());
                tableNameMap.get(tableName).get(database).add(tableIdentifier);
            }
        }

        for (Map.Entry<String, Map<String, List<SqlIdentifier>>> entry : tableNameMap.entrySet()) {
            val tableName = entry.getKey();
            val tables = entry.getValue();

            if (tables.size() < 2)
                continue;

            log.debug("There are {} databases have a table named {}", tables.size(), tableName);
            for (Map.Entry<String, List<SqlIdentifier>> schemaEntry : tables.entrySet()) {
                val database = schemaEntry.getKey();
                val tableIdentity = database + "." + tableName;
                if (tableViews.containsKey(tableIdentity)) {
                    tableToBeReplaced.addAll(schemaEntry.getValue());
                }
            }
        }

        return tableToBeReplaced;
    }

    private static Pair<String, String> getTableInfo(String defaultSchema, SqlIdentifier tableIdentifier) {
        val names = tableIdentifier.names;
        String database = null;
        String tableName = null;
        if (names.size() == 1) {
            database = defaultSchema;
            tableName = names.get(0);
        } else if (names.size() == 2) {
            database = names.get(0);
            tableName = names.get(1);
        }

        if (database != null && tableName != null) {
            return new Pair<>(database.toUpperCase(), tableName.toUpperCase());
        }

        return null;
    }

    private static void collectNeededInfo(String defaultSchema,
                                          List<SqlIdentifier> tableIdentifiers,
                                          Map<String, List<SqlIdentifier>> withClauses,
                                          Set<String> queryUsedTables,
                                          List<SqlIdentifier> realTableIdentifiers) {
        for (SqlIdentifier tableIdentifier : tableIdentifiers) {
            val tableInfo = getTableInfo(defaultSchema, tableIdentifier);
            if (tableInfo == null)
                continue;

            val database = tableInfo.getFirst();
            val tableName = tableInfo.getSecond();
            if (!withClauses.containsKey(tableName)) {
                String tableIdentity = database + "." + tableName;
                queryUsedTables.add(tableIdentity);
                realTableIdentifiers.add(tableIdentifier);
            }
        }

        for (Map.Entry<String, List<SqlIdentifier>> withClause : withClauses.entrySet()) {
            val withAlias = withClause.getKey();
            var tableIdentifiersInWith = withClause.getValue();
            val realTableIdentifiersInWith = Lists.<SqlIdentifier>newArrayList();

            val allAliasExceptCurrent = Sets.newHashSet(withClauses.keySet());
            allAliasExceptCurrent.remove(withAlias);
            for (SqlIdentifier tableIdentifier : tableIdentifiersInWith) {
                val tableInfo = getTableInfo(defaultSchema, tableIdentifier);
                if (tableInfo == null)
                    continue;

                val database = tableInfo.getFirst();
                val tableName = tableInfo.getSecond();
                if (!allAliasExceptCurrent.contains(tableName)) {
                    String tableIdentity = database + "." + tableName;
                    queryUsedTables.add(tableIdentity);
                    realTableIdentifiers.add(tableIdentifier);
                    realTableIdentifiersInWith.add(tableIdentifier);
                }
            }

            withClauses.put(withAlias, realTableIdentifiersInWith);
        }
    }

    private static String getRandomAlias(String tableName) {
        return tableName + "_" + RandomStringUtils.random(5, true, true);
    }

    private static class TableIdentifierFinder extends SqlBasicVisitor<SqlNode> {
        private SqlNode basicNode;
        private List<SqlIdentifier> tableIdentifiers = Lists.newArrayList();
        private Map<String, List<SqlIdentifier>> withClauses = Maps.newHashMap();

        public TableIdentifierFinder(String sql) {
            try {
                this.basicNode = CalciteParser.parse(sql);
            } catch (SqlParseException e) {
                throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid", e);
            }
            basicNode.accept(this);
        }

        public TableIdentifierFinder(SqlNode sqlNode) {
            this.basicNode = sqlNode;
            basicNode.accept(this);
        }

        public List<SqlIdentifier> getTableIdentifiers() {
            return tableIdentifiers;
        }

        public Map<String, List<SqlIdentifier>> getWithClauses() {
            return withClauses;
        }

        @Override
        public SqlNode visit(SqlCall sqlCall) {
            if (sqlCall instanceof SqlSelect) {
                SqlNode sqlFrom = ((SqlSelect) sqlCall).getFrom();
                if (sqlFrom == null)
                    return null;

                switch (sqlFrom.getKind()) {
                    case AS:
                        val sqlFromOperand = ((SqlCall) sqlFrom).operand(0);
                        sqlFromOperand.accept(this);
                        break;
                    case JOIN:
                        val sqlJoin = (SqlJoin) sqlFrom;
                        sqlJoin.getLeft().accept(this);
                        sqlJoin.getRight().accept(this);
                        break;
                    case VALUES:
                    case OVER:
                        break;
                    default:
                        tableIdentifiers.add((SqlIdentifier) sqlFrom);
                        break;
                }
            } else if (sqlCall instanceof SqlWithItem) {
                val sqlWithItem = (SqlWithItem) sqlCall;
                withClauses.put(sqlWithItem.name.toString(), new TableIdentifierFinder(sqlWithItem.query).getTableIdentifiers());
            } else {
                sqlCall.getOperator().acceptCall(this, sqlCall);
            }

            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier sqlIdentifier) {
            tableIdentifiers.add(sqlIdentifier);
            return null;
        }
    }
}
