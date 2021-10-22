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

package io.kyligence.kap.query.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;

/**
 * Transform "WITH AS ... SELECT" SQL to SQL with subquery 
 *
 * E.G.
 * Original Sql:
 *     with T1 as (...)
 *     select * from T1
 *
 * Will be transformed to:
 *     SELECT * FROM (...) AS T1
 *
 * For preparedStatement, the "?" count and position maybe changed after transformed
 * So the preparedStatement parameters should also be transformed
 *
 */
public class WithToSubQueryTransformer implements QueryUtil.IQueryTransformer, IKeep {
    private static final Logger logger = LoggerFactory.getLogger(WithToSubQueryTransformer.class);

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        if (!KapConfig.getInstanceFromEnv().enableReplaceDynamicParams()) {
            // when dynamic params close, '?' count may inconsistent with params count after transform with to subquery
            return originSql;
        }
        try {
            SqlWithMatcher matcher = new SqlWithMatcher(originSql);
            SqlNode sqlNode = getSqlNode(originSql);
            sqlNode.accept(matcher);
            if (matcher.isHasSqlWith()) {
                return transformSqlWith(originSql, matcher.sqlWithPositions);
            }
            return originSql;
        } catch (Exception e) {
            logger.error("Something unexpected while transform with to SubQuery, return original query", e);
            return originSql;
        }
    }

    private String transformSqlWith(String originSql, Map<Pair<Integer, Integer>, SqlWith> sqlWithPositions)
            throws SqlParseException {
        SqlNode sqlNode = getSqlNode(originSql);
        Map<String, String> aliasToSubQueryMap = parseSqlWithTableAliasMap(sqlWithPositions);
        String replacedSql = replaceTableAliasToSubQueryInSql(originSql, sqlNode, aliasToSubQueryMap);
        String correctSql = subSqlWithListInReplacedSql(replacedSql);
        return normSql(getSqlNode(correctSql).toString());
    }

    /**
     *  Fetch SqlWithList, return a map with the table alias name as the key, the corresponding sub-query as the value
     *
     *  E.G.
     *  If the sql is
     *      with T1 as (sub-query1), T2 as (sub-query2), T3 as (sub-query3) select ...
     *
     *  The map will be returned as below:
     *  SqlWithAliasMap:
     *     T1 -> sub-query1
     *     T2 -> sub-query2
     *     T3 -> sub-query3
     *
     *  If the previous table alias has been referenced in later sub-queries in SqlWithList,
     *  they will also be replaced by the sub-query which the table alias referenced to.
     */
    private Map<String, String> parseSqlWithTableAliasMap(Map<Pair<Integer, Integer>, SqlWith> sqlWithPositions)
            throws SqlParseException {
        Map<String, String> aliasMap = new LinkedHashMap<>();

        SortedSet<Pair<Integer, Integer>> sortedPositions = new TreeSet<>(Comparator.comparingInt(Pair::getFirst));
        sortedPositions.addAll(sqlWithPositions.keySet());

        for (Pair<Integer, Integer> pos : sortedPositions) {
            SqlWith sqlWith = sqlWithPositions.get(pos);
            parseSqlWithAliasMapImpl(sqlWith, aliasMap);
        }

        return aliasMap;
    }

    private void parseSqlWithAliasMapImpl(SqlWith sqlWith, Map<String, String> aliasMap) throws SqlParseException {
        List<SqlNode> withList = sqlWith.withList.getList();
        for (SqlNode withNode : withList) {
            SqlWithItem with = (SqlWithItem) withNode;
            String alias = with.name.toString();
            String subQuery = processSqlWithQueryItemIfNeeded(with.query.toString(), aliasMap);
            aliasMap.put(alias, subQuery);
        }
    }

    private String processSqlWithQueryItemIfNeeded(String query, Map<String, String> aliasMap)
            throws SqlParseException {
        SqlNode sqlNode = getSqlNode(normSql(query));
        return replaceTableAliasToSubQueryInSql(normSql(query), sqlNode, aliasMap);
    }

    /**
     * Replace the table alias which defined in SqlWithList to the sub-query in sql
     */
    private String replaceTableAliasToSubQueryInSql(String sql, SqlNode sqlNode,
                                                    Map<String, String> aliasToSubQueryMap) {

        SqlWithAliasPositionFinder positionFinder = new SqlWithAliasPositionFinder(aliasToSubQueryMap, sql);
        sqlNode.accept(positionFinder);

        Map<Pair<Integer, Integer>, SqlIdentifier> positions = positionFinder.getPositions();
        SortedSet<Pair<Integer, Integer>> sortedPositions = new TreeSet<>((o1, o2) -> o2.getFirst() - o1.getFirst());
        sortedPositions.addAll(positions.keySet());

        for (Pair<Integer, Integer> position : sortedPositions) {
            SqlIdentifier sqlIdentifier = positions.get(position);
            String queryToReplace = aliasToSubQueryMap.get(sqlIdentifier.toString());
            sql = replace(sql, position.getFirst(), position.getSecond(), queryToReplace, sqlIdentifier.toString());
        }

        return normSql(sql);
    }

    private String replace(String sql, int start, int end, String query, String aliasName) {
        String newSql = sql.substring(0, start - 1) + "\n(" + query + ") as " + aliasName + "\n" + sql.substring(end);
        return normSql(newSql);
    }

    /**
     * Remove the SqlWithList part in Sql, only keep the query part
     */
    private String subSqlWithListInReplacedSql(String sql) throws SqlParseException {
        SqlWithMatcher matcher = new SqlWithMatcher(sql);
        SqlNode sqlNode = getSqlNode(sql);
        sqlNode.accept(matcher);
        Set<Pair<Integer, Integer>> positionsSet = matcher.getSqlWithListPositions();
        List<Pair<Integer, Integer>> positions = new ArrayList<>(positionsSet);
        positions.sort(((o1, o2) -> o2.getFirst() - o1.getFirst()));
        for (Pair<Integer, Integer> pos : positions) {
            sql = subSqlWithInSql(sql, pos.getFirst(), pos.getSecond());
        }
        return sql;
    }

    private String subSqlWithInSql(String sql, int start, int end) {
        String newSql = sql.substring(0, start) + sql.substring(end);
        return normSql(newSql);
    }

    private SqlNode getSqlNode(String sql) throws SqlParseException {
        return CalciteParser.parse(sql);
    }

    private String normSql(String sql) {
        return sql.replace("`", "");
    }

    /**
     * SqlWithMatcher visits SqlNode
     * To find:
     * 1. if the sql contains any SqlWith
     * 2. find positions of SqlWiths inside the sql
     */
    static class SqlWithMatcher extends AbstractSqlVisitor {
        private boolean hasSqlWith = false;
        private Set<Pair<Integer, Integer>> sqlWithListPositions = new LinkedHashSet<>();
        private Set<Pair<Integer, Integer>> questionMarkPositions = new LinkedHashSet<>();
        private Map<Pair<Integer, Integer>, SqlWith> sqlWithPositions = new HashMap<>();

        public SqlWithMatcher(String originSql) {
            super(originSql);
        }

        @Override
        public void questionMarkFound(SqlDynamicParam questionMark) {
            Pair<Integer, Integer> pos = CalciteParser.getReplacePos(questionMark, originSql);
            questionMarkPositions.add(pos);
        }

        @Override
        public void sqlWithFound(SqlWith sqlWith) {
            Pair<Integer, Integer> withPos = CalciteParser.getReplacePos(sqlWith, originSql);
            sqlWithPositions.put(withPos, sqlWith);

            Pair<Integer, Integer> withListPos = CalciteParser.getReplacePos(sqlWith.withList, originSql);
            Pair<Integer, Integer> bodyPos = CalciteParser.getReplacePos(sqlWith.body, originSql);
            sqlWithListPositions.add(new Pair<>(withListPos.getFirst(), bodyPos.getFirst()));
            visitInSqlWithList(sqlWith.withList);
            hasSqlWith = true;

            SqlNode sqlWithQuery = sqlWith.body;
            sqlWithQuery.accept(this);
        }

        public boolean isHasSqlWith() {
            return hasSqlWith;
        }

        public Set<Pair<Integer, Integer>> getSqlWithListPositions() {
            return sqlWithListPositions;
        }

    }

    /**
     * SqlWithAliasPositionFinder visits SqlNode 
     * To find all SqlIdentifiers (table alias defined in SqlWith) and their position
     */
    static class SqlWithAliasPositionFinder extends AbstractSqlVisitor {
        Map<String, String> aliasToSubQueryMap;
        private Map<Pair<Integer, Integer>, SqlIdentifier> positions = new LinkedHashMap<>();

        public SqlWithAliasPositionFinder(Map<String, String> aliasToSubQueryMap, String originSql) {
            super(originSql);
            this.aliasToSubQueryMap = aliasToSubQueryMap;
        }

        @Override
        public void visitInSqlFrom(SqlNode from) {
            if (from instanceof SqlWith) {
                sqlWithFound((SqlWith) from);
            } else if (isAs(from)) {
                visitInAsNode((SqlBasicCall) from);
            } else if (from instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) from;
                visitInSqlJoin(join);
            } else if (from instanceof SqlIdentifier) {
                SqlIdentifier ide = (SqlIdentifier) from;
                parseSqlIdentifierPosition(ide);
            } else {
                from.accept(this);
            }
        }

        @Override
        public void visitInSqlNode(SqlNode node) {
            if (node == null)
                return;
            if (node instanceof SqlIdentifier) {
                parseSqlIdentifierPosition((SqlIdentifier) node);
            } else if (node instanceof SqlWith) {
                sqlWithFound((SqlWith) node);
            } else if (node instanceof SqlNodeList) {
                visitInSqlNodeList((SqlNodeList) node);
            } else if (node instanceof SqlCase) {
                visitInSqlCase((SqlCase) node);
            } else if (isSqlBasicCall(node)) {
                visitInSqlBasicCall((SqlBasicCall) node);
            } else {
                node.accept(this);
            }
        }

        private void parseSqlIdentifierPosition(SqlIdentifier identifier) {
            if (aliasToSubQueryMap.containsKey(identifier.toString())) {
                Pair<Integer, Integer> pos = CalciteParser.getReplacePos(identifier, originSql);
                positions.put(pos, identifier);
            }
        }

        public Map<Pair<Integer, Integer>, SqlIdentifier> getPositions() {
            return positions;
        }
    }
}
