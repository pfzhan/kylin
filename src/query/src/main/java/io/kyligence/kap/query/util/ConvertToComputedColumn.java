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

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConvertToComputedColumn implements QueryUtil.IQueryTransformer, IKeep {

    private static final String CONVERT_TO_CC_ERROR_MSG = "Something unexpected while ConvertToComputedColumn transforming the query, return original query.";

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        try {
            return transformImpl(originSql, project, defaultSchema);
        } catch (Exception e) {
            if (e instanceof org.apache.calcite.sql.parser.SqlParseException) {
                log.warn(CONVERT_TO_CC_ERROR_MSG, Throwables.getRootCause(e));
            } else {
                log.warn(CONVERT_TO_CC_ERROR_MSG, e);
            }

            return originSql;
        }
    }

    public String transformImpl(String originSql, String project, String defaultSchema) throws SqlParseException {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<NDataModel> dataModelDescs = dataflowManager.listUnderliningDataModels();
        return transformImpl(originSql, project, defaultSchema, dataModelDescs);
    }

    public String transformImpl(String originSql, String project, NDataModel dataModelDesc, String defaultSchema)
            throws SqlParseException {
        return transformImpl(originSql, project, defaultSchema, Lists.newArrayList(dataModelDesc));
    }

    private String transformImpl(String originSql, String project, String defaultSchema,
            List<NDataModel> dataModelDescs) throws SqlParseException {

        if (project == null || originSql == null) {
            return originSql;
        }

        return transformImpl(originSql, new QueryAliasMatcher(project, defaultSchema), dataModelDescs);
    }

    private String transformImpl(String originSql, QueryAliasMatcher queryAliasMatcher, List<NDataModel> dataModelDescs)
            throws SqlParseException {
        if (!KapConfig.getInstanceFromEnv().isImplicitComputedColumnConvertEnabled()) {
            return originSql;
        }

        String sql = originSql;
        if (queryAliasMatcher == null || sql == null) {
            return sql;
        }

        int recursionTimes = 0;
        int maxRecursionTimes = KapConfig.getInstanceFromEnv().getComputedColumnMaxRecursionTimes();

        while ((recursionTimes++) < maxRecursionTimes) {
            Pair<String, Boolean> result = transformImplRecursive(sql, queryAliasMatcher, dataModelDescs, false);
            sql = result.getFirst();
            boolean recursionCompleted = result.getSecond();
            if (recursionCompleted) {
                break;
            }
        }

        Pair<String, Boolean> result = transformImplRecursive(sql, queryAliasMatcher, dataModelDescs, true);
        sql = result.getFirst();

        return sql;
    }

    private Pair<String, Boolean> transformImplRecursive(String sql, QueryAliasMatcher queryAliasMatcher,
            List<NDataModel> dataModelDescs, boolean replaceCcName) throws SqlParseException {
        boolean recursionCompleted = true;
        List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
        Pair<String, Integer> choiceForCurrentSubquery = null; //<new sql, number of changes by the model>

        for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
            if (choiceForCurrentSubquery != null) { //last selectOrOrderby had a matched
                selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
                choiceForCurrentSubquery = null;
            }

            SqlCall selectOrOrderby = selectOrOrderbys.get(i);

            ComputedColumnReplacer rewriteChecker = new ComputedColumnReplacer(queryAliasMatcher, dataModelDescs,
                    recursionCompleted, choiceForCurrentSubquery, selectOrOrderby);
            rewriteChecker.replace(sql, replaceCcName);
            recursionCompleted = rewriteChecker.isRecursionCompleted();
            choiceForCurrentSubquery = rewriteChecker.getChoiceForCurrentSubquery();

            if (choiceForCurrentSubquery != null) {
                sql = choiceForCurrentSubquery.getFirst();
            }
        }

        return Pair.newPair(sql, recursionCompleted);
    }

    static Pair<String, Integer> replaceComputedColumn(String inputSql, SqlCall selectOrOrderby,
            List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo) {
        return replaceComputedColumn(inputSql, selectOrOrderby, computedColumns, queryAliasMatchInfo, false);
    }

    private static Pair<String, Integer> replaceComputedColumn(String inputSql, SqlCall selectOrOrderby,
            List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo, boolean replaceCcName) {

        if (computedColumns == null || computedColumns.isEmpty()) {
            return Pair.newPair(inputSql, 0);
        }

        String result = inputSql;
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp;
        try {
            toBeReplacedExp = matchComputedColumn(inputSql, selectOrOrderby, computedColumns, queryAliasMatchInfo,
                    replaceCcName);
        } catch (Exception e) {
            log.debug("Convert to computedColumn Fail,parse sql fail ", e);
            return Pair.newPair(inputSql, 0);
        }

        toBeReplacedExp.sort((o1, o2) -> o2.getSecond().getFirst().compareTo(o1.getSecond().getFirst()));

        //replace user's input sql
        for (Pair<ComputedColumnDesc, Pair<Integer, Integer>> toBeReplaced : toBeReplacedExp) {
            Pair<Integer, Integer> startEndPos = toBeReplaced.getSecond();
            int start = startEndPos.getFirst();
            int end = startEndPos.getSecond();
            ComputedColumnDesc cc = toBeReplaced.getFirst();
            String alias = queryAliasMatchInfo.getAliasMapping().inverse().get(cc.getTableAlias());

            if (alias == null) {
                throw new IllegalStateException(cc.getExpression() + " expression of cc " + cc.getFullName()
                        + " is found in query but its table ref " + cc.getTableAlias() + " is missing in query");
            }

            String expr = inputSql.substring(start, end);
            String ccColumnName = replaceCcName ? cc.getInternalCcName() : cc.getColumnName();
            log.debug("Computed column: {} matching {} at [{},{}] using alias in query: {}", cc.getColumnName(), expr,
                    start, end, alias);
            result = result.substring(0, start) + alias + "." + ccColumnName + result.substring(end);
        }
        try {
            SqlNode inputNodes = CalciteParser.parse(inputSql);
            int cntNodesBefore = getInputTreeNodes((SqlCall) inputNodes).size();
            SqlNode resultNodes = CalciteParser.parse(result);
            int cntNodesAfter = getInputTreeNodes((SqlCall) resultNodes).size();
            return Pair.newPair(result, cntNodesBefore - cntNodesAfter);
        } catch (SqlParseException e) {
            log.debug("Convert to computedColumn Fail, parse result sql fail: {}", result, e);
            return Pair.newPair(inputSql, 0);
        }
    }

    private static List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> matchComputedColumn(String inputSql,
            SqlCall selectOrOrderby, List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo,
            boolean replaceCcName) {
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp = new ArrayList<>();
        for (ComputedColumnDesc cc : computedColumns) {
            List<SqlNode> matchedNodes;
            matchedNodes = getMatchedNodes(selectOrOrderby, replaceCcName ? cc.getFullName() : cc.getExpression(),
                    queryAliasMatchInfo);

            for (SqlNode node : matchedNodes) {
                Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(node, inputSql);
                int start = startEndPos.getFirst();
                int end = startEndPos.getSecond();

                boolean conflict = false;
                for (val pair : toBeReplacedExp) {
                    Pair<Integer, Integer> replaced = pair.getSecond();
                    if (!(replaced.getFirst() >= end || replaced.getSecond() <= start)) {
                        // overlap with chosen areas
                        conflict = true;
                    }
                }
                if (conflict) {
                    continue;
                }
                toBeReplacedExp.add(Pair.newPair(cc, Pair.newPair(start, end)));
            }
        }
        return toBeReplacedExp;
    }

    //Return matched node's position and its alias(if exists).If can not find matches, return an empty list
    private static List<SqlNode> getMatchedNodes(SqlCall selectOrOrderby, String ccExp, QueryAliasMatchInfo matchInfo) {
        if (ccExp == null || ccExp.equals(StringUtils.EMPTY)) {
            return Collections.emptyList();
        }
        ArrayList<SqlNode> matchedNodes = new ArrayList<>();
        SqlNode ccExpressionNode = CalciteParser.getExpNode(ccExp);
        List<SqlNode> inputNodes = getInputTreeNodes(selectOrOrderby);

        // find whether user input sql's tree node equals computed columns's define expression
        for (SqlNode inputNode : inputNodes) {
            if (ExpressionComparator.isNodeEqual(inputNode, ccExpressionNode, matchInfo,
                    new AliasDeduceImpl(matchInfo))) {
                matchedNodes.add(inputNode);
            }

        }
        return matchedNodes;
    }

    private static List<SqlNode> getInputTreeNodes(SqlCall selectOrOrderby) {
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
            StringBuilder alias = new StringBuilder();
            ImmutableList<String> names = ((SqlIdentifier) node).names;
            if (names.size() >= 2) {
                for (int i = 0; i < names.size() - 1; i++) {
                    alias.append(names.get(i)).append(".");
                }
            }
            return alias.toString();
        }
        if (node instanceof SqlNodeList) {
            return StringUtils.EMPTY;
        }
        if (node instanceof SqlLiteral) {
            return StringUtils.EMPTY;
        }
        return StringUtils.EMPTY;
    }

    private static String getTableAlias(List<SqlNode> operands) {
        if (operands.isEmpty()) {
            return StringUtils.EMPTY;
        }
        return getTableAlias(operands.get(0));
    }

    static List<ComputedColumnDesc> getCCListSortByLength(List<ComputedColumnDesc> computedColumns) {
        if (computedColumns == null || computedColumns.isEmpty()) {
            return Lists.newArrayList();
        }

        Ordering<ComputedColumnDesc> ordering = Ordering.from(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.compare(o1.replaceAll("\\s*", StringUtils.EMPTY).length(),
                        o2.replaceAll("\\s*", StringUtils.EMPTY).length());
            }
        }).reverse().nullsLast().onResultOf(new Function<ComputedColumnDesc, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ComputedColumnDesc input) {
                return input == null ? null : input.getExpression();
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
            if (call.getOperator() instanceof SqlAsOperator) {
                call.getOperator().acceptCall(this, call, true, SqlBasicVisitor.ArgHandlerImpl.<SqlNode> instance());
                return null;
            }

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

    private class ComputedColumnReplacer {
        private QueryAliasMatcher queryAliasMatcher;
        private List<NDataModel> dataModels;
        private boolean recursionCompleted;
        private Pair<String, Integer> choiceForCurrentSubquery;
        private SqlCall selectOrOrderby;

        ComputedColumnReplacer(QueryAliasMatcher queryAliasMatcher, List<NDataModel> dataModels,
                boolean recursionCompleted, Pair<String, Integer> choiceForCurrentSubquery, SqlCall selectOrOrderby) {
            this.queryAliasMatcher = queryAliasMatcher;
            this.dataModels = dataModels;
            this.recursionCompleted = recursionCompleted;
            this.choiceForCurrentSubquery = choiceForCurrentSubquery;
            this.selectOrOrderby = selectOrOrderby;
        }

        boolean isRecursionCompleted() {
            return recursionCompleted;
        }

        Pair<String, Integer> getChoiceForCurrentSubquery() {
            return choiceForCurrentSubquery;
        }

        public void replace(String sql, boolean replaceCcName) {
            SqlSelect sqlSelect = KapQueryUtil.extractSqlSelect(selectOrOrderby);
            if (sqlSelect == null) {
                return;
            }

            //give each data model a chance to rewrite, choose the model that generates most changes
            for (NDataModel model : dataModels) {
                QueryAliasMatchInfo info = queryAliasMatcher.match(model, sqlSelect);
                if (info == null) {
                    continue;
                }

                List<ComputedColumnDesc> computedColumns = getSortedComputedColumnWithModel(model);
                Pair<String, Integer> ret = replaceComputedColumn(sql, selectOrOrderby, computedColumns, info,
                        replaceCcName);

                if (replaceCcName && !sql.equals(ret.getFirst())) {
                    choiceForCurrentSubquery = ret;
                } else {
                    if (ret.getSecond() == 0) {
                        continue;
                    }
                    if (choiceForCurrentSubquery == null || ret.getSecond() > choiceForCurrentSubquery.getSecond()) {
                        choiceForCurrentSubquery = ret;
                        recursionCompleted = false;
                    }
                }
            }
        }

        //match longer expressions first
        private List<ComputedColumnDesc> getSortedComputedColumnWithModel(NDataModel dataModelDesc) {
            return getCCListSortByLength(dataModelDesc.getComputedColumnDescs());
        }
    }
}
