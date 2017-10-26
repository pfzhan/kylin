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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.KapModel;

//very similar to ConvertToComputedColumn in structure, maybe we should extract a common base class?
public class RestoreFromComputedColumn implements IPushDownConverter {

    //find child inner select first
    static class ColumnUsagesFinder extends SqlBasicVisitor<SqlNode> {
        private List<SqlIdentifier> usages;
        private Set<String> columnNames;

        ColumnUsagesFinder(Set<String> columnNames) {
            this.columnNames = columnNames;
            this.usages = Lists.newArrayList();
        }

        public List<SqlIdentifier> getUsages() {
            return usages;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (columnNames.contains(id.names.get(id.names.size() - 1))) {
                this.usages.add(id);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {

            //skip the part after AS
            if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                SqlNode[] operands = ((SqlBasicCall) call).getOperands();
                if (operands != null && operands.length == 2) {
                    operands[0].accept(this);
                }

                return null;
            }

            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }

            return null;
        }

        public static List<SqlIdentifier> getColumnUsages(SqlCall selectOrOrderby, Set<String> columnNames)
                throws SqlParseException {
            ColumnUsagesFinder sqlSubqueryFinder = new ColumnUsagesFinder(columnNames);
            selectOrOrderby.accept(sqlSubqueryFinder);
            return sqlSubqueryFinder.getUsages();
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(RestoreFromComputedColumn.class);

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        try {

            String sql = originSql;
            if (project == null || sql == null) {
                return sql;
            }

            DataModelManager metadataManager = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
            List<DataModelDesc> dataModelDescs = metadataManager.getModels(project);

            List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
            Pair<String, Integer> choiceForCurrentSubquery = null; //<new sql, number of changes by the model>

            QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, defaultSchema);

            for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
                if (choiceForCurrentSubquery != null) { //last selectOrOrderby had a matched
                    selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
                    choiceForCurrentSubquery = null;
                }

                SqlCall selectOrOrderby = selectOrOrderbys.get(i);
                SqlSelect sqlSelect = null;

                if (selectOrOrderby instanceof SqlSelect) {
                    sqlSelect = (SqlSelect) selectOrOrderby;
                } else if (selectOrOrderby instanceof SqlOrderBy) {
                    SqlOrderBy sqlOrderBy = ((SqlOrderBy) selectOrOrderby);
                    if (sqlOrderBy.query instanceof SqlSelect) {
                        sqlSelect = (SqlSelect) sqlOrderBy.query;
                    } else if (sqlOrderBy.query.getKind().equals(SqlKind.UNION)) {
                        continue;
                    }
                } else if (selectOrOrderby.getKind().equals(SqlKind.UNION)) {
                    continue;
                }

                //give each data model a chance to rewrite, choose the model that generates most changes
                for (int j = 0; j < dataModelDescs.size(); j++) {
                    KapModel model = (KapModel) dataModelDescs.get(j);
                    QueryAliasMatchInfo info = queryAliasMatcher.match(model, sqlSelect);
                    if (info == null) {
                        continue;
                    }

                    Pair<String, Integer> ret = replaceComputedColumn(sql, selectOrOrderby, model, info);

                    if (ret.getSecond() == 0)
                        continue;

                    if ((choiceForCurrentSubquery == null)
                            || (ret.getSecond() > choiceForCurrentSubquery.getSecond())) {
                        choiceForCurrentSubquery = ret;
                    }
                }

                if (choiceForCurrentSubquery != null) {
                    sql = choiceForCurrentSubquery.getFirst();
                }
            }
            return sql;

        } catch (Exception e) {
            logger.debug(
                    "Something unexpected while RestoreFromComputedColumn transforming the query, return original query",
                    e);
            return originSql;
        }

    }

    /**
     * return the replaced sql, and the count of changes in the replaced sql
     */
    static Pair<String, Integer> replaceComputedColumn(String inputSql, SqlCall selectOrOrderby,
            KapModel dataModelDesc, QueryAliasMatchInfo queryAliasMatchInfo) throws SqlParseException {

        String result = inputSql;

        Set<String> ccColumnNames = dataModelDesc.getComputedColumnNames();
        List<SqlIdentifier> columnUsages = ColumnUsagesFinder.getColumnUsages(selectOrOrderby, ccColumnNames);
        if (columnUsages.size() == 0) {
            return Pair.newPair(inputSql, 0);
        }

        CalciteParser.descSortByPosition(columnUsages);

        List<Pair<String, Pair<Integer, Integer>>> toBeReplacedUsages = Lists.newArrayList();

        for (SqlIdentifier columnUsage : columnUsages) {
            String columnName = Iterables.getLast(columnUsage.names);
            //TODO cannot do this check because cc is not visible on schema without solid realizations
            // after this is done, constrains on #1932 can be relaxed

            //            TblColRef tblColRef = QueryAliasMatchInfo.resolveTblColRef(queryAliasMatchInfo.getQueryAlias(), columnName);
            //            //for now, must be fact table
            //            Preconditions.checkState(tblColRef.getTableRef().getTableIdentity()
            //                    .equals(dataModelDesc.getRootFactTable().getTableIdentity()));
            ComputedColumnDesc computedColumnDesc = dataModelDesc.findCCByCCColumnName(columnName);
            String replaced = replaceAliasInExpr(computedColumnDesc.getExpression(), queryAliasMatchInfo);

            Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(columnUsage, inputSql);
            int start = startEndPos.getFirst();
            int end = startEndPos.getSecond();
            toBeReplacedUsages.add(Pair.newPair(replaced, Pair.newPair(start, end)));
        }

        Collections.sort(toBeReplacedUsages, new Comparator<Pair<String, Pair<Integer, Integer>>>() {
            @Override
            public int compare(Pair<String, Pair<Integer, Integer>> o1, Pair<String, Pair<Integer, Integer>> o2) {
                return o2.getSecond().getFirst().compareTo(o1.getSecond().getFirst());
            }
        });

        //check
        Pair<String, Pair<Integer, Integer>> last = null;
        for (Pair<String, Pair<Integer, Integer>> toBeReplaced : toBeReplacedUsages) {
            if (last != null) {
                Preconditions.checkState(last.getSecond().getFirst() > toBeReplaced.getSecond().getSecond(),
                        "Positions for two column usage has overlaps");
            }
            Pair<Integer, Integer> startEndPos = toBeReplaced.getSecond();
            int start = startEndPos.getFirst();
            int end = startEndPos.getSecond();

            String replacement = toBeReplaced.getFirst();

            logger.debug("Column Usage at [" + start + "," + end + "] " + " is replaced by  " + replacement);
            result = result.substring(0, start) + replacement + result.substring(end);
            last = toBeReplaced;
        }

        return Pair.newPair(result, toBeReplacedUsages.size());
    }

    /**
     * The computed column expression is defined based on alias in model, e.g. BUYER_COUNTRY.x + BUYER_ACCOUNT.y
     * however user query might user a different alias, say bc.x + ba.y
     */
    static String replaceAliasInExpr(String expr, QueryAliasMatchInfo queryAliasMatchInfo) {
        String prefix = "select ";
        String suffix = " from t";
        String sql = prefix + expr + suffix;
        SqlNode sqlNode = CalciteParser.getOnlySelectNode(sql);

        final Set<SqlIdentifier> s = Sets.newHashSet();
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkState(id.names.size() == 2);
                s.add(id);
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        List<SqlIdentifier> sqlIdentifiers = Lists.newArrayList(s);

        CalciteParser.descSortByPosition(sqlIdentifiers);

        for (SqlIdentifier sqlIdentifier : sqlIdentifiers) {
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(sqlIdentifier, sql);
            int start = replacePos.getFirst();
            int end = replacePos.getSecond();
            String aliasInExpr = sqlIdentifier.names.get(0);
            String col = sqlIdentifier.names.get(1);
            String aliasInQuery = queryAliasMatchInfo.getAliasMapping().inverse().get(aliasInExpr);
            Preconditions.checkNotNull(aliasInQuery,
                    "match for alias " + aliasInExpr + " in expr (" + expr + ") is not found in query ");
            sql = sql.substring(0, start) + aliasInQuery + "." + col + sql.substring(end);
        }

        return sql.substring(prefix.length(), sql.length() - suffix.length());
    }

}
