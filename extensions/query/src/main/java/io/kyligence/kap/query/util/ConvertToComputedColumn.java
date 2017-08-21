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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import io.kyligence.kap.common.obf.IKeep;

public class ConvertToComputedColumn implements QueryUtil.IQueryTransformer, IKeep {

    private static final Logger logger = LoggerFactory.getLogger(ConvertToComputedColumn.class);

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        try {

            String sql = originSql;
            if (project == null || sql == null) {
                return sql;
            }

            MetadataManager metadataManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
            List<DataModelDesc> dataModelDescs = metadataManager.getModels(project);

            List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
            QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, defaultSchema);
            boolean isSqlChanged = false;

            for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
                if (isSqlChanged) {
                    selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
                    isSqlChanged = false;
                }

                SqlSelect sqlSelect = selectOrOrderbys.get(i) instanceof SqlSelect ? ((SqlSelect) selectOrOrderbys.get(i))
                    : (SqlSelect) (((SqlOrderBy) selectOrOrderbys.get(i)).query);
                for (int j = 0; j < dataModelDescs.size(); j++) {
                    QueryAliasMatchInfo info = queryAliasMatcher.match(dataModelDescs.get(j), sqlSelect);
                    if (info == null) {
                        continue;
                    }

                    List<ComputedColumnDesc> computedColumns = getSortedComputedColumnWithModel(dataModelDescs.get(j));
                    String s = replaceComputedColumn(sql, selectOrOrderbys.get(i), computedColumns, info);
                    if (!StringUtils.equals(sql, s)) {
                        isSqlChanged = true;
                        sql = s;
                        //a subquery can only match one model's cc, 
                        //will continue to next subquery after SUCCESSFULLY apply a model's cc
                        break;
                    }
                }
            }

            return sql;

        } catch (Exception e) {
            logger.error(
                    "Something unexpected while ConvertToComputedColumn transforming the query, return original query",
                    e);
            return originSql;
        }
    }

    static String replaceComputedColumn(String inputSql, SqlCall selectOrOrderby, List<ComputedColumnDesc> computedColumns,
            QueryAliasMatchInfo queryAliasMatchInfo) {

        if (computedColumns == null || computedColumns.isEmpty()) {
            return inputSql;
        }

        String result = inputSql;
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp = new ArrayList<>();

        for (ComputedColumnDesc cc : computedColumns) {
            List<SqlNode> matchedNodes;
            try {
                matchedNodes = getMatchedNodes(selectOrOrderby, cc.getExpression(), queryAliasMatchInfo);
            } catch (SqlParseException e) {
                logger.debug("Convert to computedColumn Fail,parse sql fail ", e);
                return inputSql;
            }
            for (SqlNode node : matchedNodes) {
                Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(node, inputSql);
                int start = startEndPos.getLeft();
                int end = startEndPos.getRight();

                boolean conflict = false;
                for (int i = 0; i < toBeReplacedExp.size(); i++) {
                    Pair<Integer, Integer> replaced = toBeReplacedExp.get(i).getRight();
                    if (!(replaced.getLeft() >= end || replaced.getRight() <= start)) {
                        //overlap with chosen areas
                        conflict = true;
                    }
                }

                if (conflict) {
                    continue;
                }

                toBeReplacedExp.add(Pair.of(cc, Pair.of(start, end)));
            }

        }

        Collections.sort(toBeReplacedExp, new Comparator<Pair<ComputedColumnDesc, Pair<Integer, Integer>>>() {
            @Override
            public int compare(Pair<ComputedColumnDesc, Pair<Integer, Integer>> o1,
                    Pair<ComputedColumnDesc, Pair<Integer, Integer>> o2) {
                return o2.getRight().getLeft().compareTo(o1.getRight().getLeft());
            }
        });

        //replace user's input sql
        for (Pair<ComputedColumnDesc, Pair<Integer, Integer>> toBeReplaced : toBeReplacedExp) {
            Pair<Integer, Integer> startEndPos = toBeReplaced.getRight();
            int start = startEndPos.getLeft();
            int end = startEndPos.getRight();
            ComputedColumnDesc cc = toBeReplaced.getLeft();
            String alias = queryAliasMatchInfo.getAliasMapping().inverse().get(cc.getTableAlias());

            logger.debug("Computed column: " + cc.getColumnName() + " matching " + inputSql.substring(start, end)
                    + " at [" + start + "," + end + "] " + " using alias in query: " + alias);
            result = result.substring(0, start) + alias + "." + cc.getColumnName() + result.substring(end);
        }
        return result;
    }

    //Return matched node's position and its alias(if exists).If can not find matches, return an empty list
    private static List<SqlNode> getMatchedNodes(SqlCall selectOrOrderby, String ccExp,
            QueryAliasMatchInfo queryAliasMatchInfo) throws SqlParseException {
        if (ccExp == null || ccExp.equals("")) {
            return new ArrayList<>();
        }
        ArrayList<SqlNode> matchedNodes = new ArrayList<>();
        SqlNode ccNode = CalciteParser.getExpNode(ccExp);
        List<SqlNode> inputNodes = getInputTreeNodes(selectOrOrderby);

        // find whether user input sql's tree node equals computed columns's define expression
        for (SqlNode inputNode : inputNodes) {
            if (ExpressionComparator.isNodeEqual(inputNode, ccNode, queryAliasMatchInfo)) {
                matchedNodes.add(inputNode);
            }
        }
        return matchedNodes;
    }

    private static List<SqlNode> getInputTreeNodes(SqlCall selectOrOrderby) throws SqlParseException {
        SqlTreeVisitor stv = new SqlTreeVisitor();
        selectOrOrderby.accept(stv);
        return stv.getSqlNodes();
    }

    private static String getTableAlias(SqlNode node) {
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            return getTableAlias(call.getOperandList());
        }
        if (node instanceof SqlIdentifier) {
            StringBuilder alias = new StringBuilder("");
            ImmutableList<String> names = ((SqlIdentifier) node).names;
            if (names.size() >= 2) {
                for (int i = 0; i < names.size() - 1; i++) {
                    alias.append(names.get(i)).append(".");
                }
            }
            return alias.toString();
        }
        if (node instanceof SqlNodeList) {
            return "";
        }
        if (node instanceof SqlLiteral) {
            return "";
        }
        return "";
    }

    private static String getTableAlias(List<SqlNode> operands) {
        for (SqlNode operand : operands) {
            return getTableAlias(operand);
        }
        return "";
    }

    //match longer expressions first
    private List<ComputedColumnDesc> getSortedComputedColumnWithModel(DataModelDesc dataModelDesc) {
        return getCCListSortByLength(dataModelDesc.getComputedColumnDescs());
    }

    static List<ComputedColumnDesc> getCCListSortByLength(List<ComputedColumnDesc> computedColumns) {
        if (computedColumns == null || computedColumns.isEmpty()) {
            return null;
        }

        Ordering<ComputedColumnDesc> ordering = Ordering.<String> from(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.compare(o1.replaceAll("\\s*", "").length(), o2.replaceAll("\\s*", "").length());
            }
        }).reverse().nullsLast().onResultOf(new Function<ComputedColumnDesc, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ComputedColumnDesc input) {
                return input.getExpression();
            }
        });

        return ordering.immutableSortedCopy(computedColumns);
    }

    static class SqlTreeVisitor implements SqlVisitor<SqlNode> {
        private List<SqlNode> sqlNodes;

        SqlTreeVisitor() {
            this.sqlNodes = new ArrayList<>();
        }

        List<SqlNode> getSqlNodes() {
            return sqlNodes;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            sqlNodes.add(nodeList);
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                node.accept(this);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            sqlNodes.add(literal);
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            sqlNodes.add(call);
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            sqlNodes.add(id);
            return null;
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            return null;
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return null;
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }
    }
}
