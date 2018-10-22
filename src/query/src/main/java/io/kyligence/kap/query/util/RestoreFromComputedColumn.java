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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

//very similar to ConvertToComputedColumn in structure, maybe we should extract a common base class?
public class RestoreFromComputedColumn implements IPushDownConverter {

    private static final Logger logger = LoggerFactory.getLogger(RestoreFromComputedColumn.class);

    @Override
    public String convert(String originSql, String project, String defaultSchema, boolean isPrepare) {
        try {

            String sql = originSql;
            if (project == null || sql == null) {
                return sql;
            }

            Map<String, NDataModel> dataModelDescs = new LinkedHashMap<>();

            NDataModelManager metadataManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for(NDataModel modelDesc: metadataManager.getDataModels()){
                dataModelDescs.put(modelDesc.getName(), modelDesc);
            }

            return convertWithGivenModels(sql, project, defaultSchema, dataModelDescs);
        } catch (Exception e) {
            logger.debug(
                    "Something unexpected while RestoreFromComputedColumn transforming the query, return original query",
                    e);
            return originSql;
        }

    }

    public static String convertWithGivenModels(String sql, String project, String defaultSchema,
            Map<String, NDataModel> dataModelDescs) throws SqlParseException, IOException {

        List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);

        /*
            select A, sum(C) from T group by A order by SUM(C)
            topColumns: A, sum(C)
            C is not a topColumn here.
         */
        List<List<SqlNode>> topColumns = Lists.newArrayList();
        for (int i = 0; i < selectOrOrderbys.size(); i++) {

            List<SqlNode> columnList = Lists.newArrayList();

            for (SqlNode node : selectOrOrderbys.get(i).getOperandList()) {

                if (node instanceof SqlNodeList) {
                    columnList.addAll(((SqlNodeList) node).getList());
                }
            }
            topColumns.add(columnList);
        }

        Pair<String, Integer> choiceForCurrentSubquery = null; //<new sql, number of changes by the model>

        QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, defaultSchema);

        boolean recursionCompleted = false;
        int recursionTimes = 0;
        int maxRecursionTimes = KapConfig.getInstanceFromEnv().getComputedColumnMaxRecursionTimes();

        while (!recursionCompleted && (recursionTimes++) < maxRecursionTimes) {

            recursionCompleted = true;

            for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
                if (choiceForCurrentSubquery != null) { //last selectOrOrderby had a matched
                    selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
                    choiceForCurrentSubquery = null;
                }

                SqlCall selectOrOrderby = selectOrOrderbys.get(i);
                SqlSelect sqlSelect = KapQueryUtil.extractSqlSelect(selectOrOrderby);
                if (sqlSelect == null)
                    continue;

                //give each data model a chance to rewrite, choose the model that generates most changes
                for (NDataModel modelDesc : dataModelDescs.values()) {

                    NDataModel forMathModel = modelDesc;
                    if(modelDesc.isDraft() && dataModelDescs.containsKey(modelDesc.getName())){
                        forMathModel = dataModelDescs.get(modelDesc.getName());
                    }

                    QueryAliasMatchInfo info = queryAliasMatcher.match(forMathModel, sqlSelect);
                    if (info == null) {
                        continue;
                    }

                    Pair<String, Integer> ret = restoreComputedColumn(sql, selectOrOrderby, topColumns.get(i), modelDesc, info);

                    if (ret.getSecond() == 0)
                        continue;

                    if ((choiceForCurrentSubquery == null)
                            || (ret.getSecond() > choiceForCurrentSubquery.getSecond())) {
                        choiceForCurrentSubquery = ret;

                        recursionCompleted = false;
                    }
                }

                if (choiceForCurrentSubquery != null) {
                    sql = choiceForCurrentSubquery.getFirst();
                }
            }
        }
        return sql;
    }

    private static class ReplaceRange {

        String replaceExpr;

        int beginPos;
        int endPos;

        SqlIdentifier column;
    }


    /**
     * check whether it needs parenthesis
     * 1. not need if it is a top node
     * 2. not need if it is in parenthesis
     *
     * @param originSql
     * @param topColumns
     * @param replaceRange
     * @return
     */
    private static boolean needParenthesis(String originSql,  List<SqlNode> topColumns, ReplaceRange replaceRange) {

        boolean topColumn = false;

        for (SqlNode column : topColumns) {
            if (column != null && column.equalsDeep(replaceRange.column, Litmus.IGNORE)) {
                topColumn = true;
                break;
            }
        }

        if (topColumn) {
            return false;
        }

        boolean hasLeft = false;
        for (int i = replaceRange.beginPos - 1; i >= 0; i--) {

            char c = originSql.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }

            if (c == '(' || c == ',') {
                hasLeft = true;
            }
            break;
        }

        boolean hasRight = false;
        for (int i = replaceRange.endPos ; i < originSql.length(); i++) {

            char c = originSql.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }

            if (c == ')' || c == ',') {
                hasRight = true;
            }
            break;
        }

        return !(hasLeft && hasRight);

    }

    /**
     * return the replaced sql, and the count of changes in the replaced sql
     */
    static Pair<String, Integer> restoreComputedColumn(String inputSql, SqlCall selectOrOrderby, List<SqlNode> topColumns,  NDataModel dataModelDesc,
            QueryAliasMatchInfo matchInfo) throws SqlParseException {

        String result = inputSql;

        Set<String> ccColumnNames = dataModelDesc.getComputedColumnNames();
        List<SqlIdentifier> columnUsages = ColumnUsagesFinder.getColumnUsages(selectOrOrderby, ccColumnNames);
        if (columnUsages.size() == 0) {
            return Pair.newPair(inputSql, 0);
        }

        CalciteParser.descSortByPosition(columnUsages);

        List<ReplaceRange> toBeReplacedUsages = Lists.newArrayList();

        for (SqlIdentifier columnUsage : columnUsages) {
            String columnName = Iterables.getLast(columnUsage.names);
            //TODO cannot do this check because cc is not visible on schema without solid realizations
            // after this is done, constrains on #1932 can be relaxed

            //            TblColRef tblColRef = QueryAliasMatchInfo.resolveTblColRef(queryAliasMatchInfo.getQueryAlias(), columnName);
            //            //for now, must be fact table
            //            Preconditions.checkState(tblColRef.getTableRef().getTableIdentity()
            //                    .equals(dataModelDesc.getRootFactTable().getTableIdentity()));
            ComputedColumnDesc computedColumnDesc = dataModelDesc.findCCByCCColumnName(columnName);
            // The computed column expression is defined based on alias in model, e.g. BUYER_COUNTRY.x + BUYER_ACCOUNT.y
            // however user query might use a different alias, say bc.x + ba.y
            String replaced = CalciteParser.replaceAliasInExpr(computedColumnDesc.getExpression(),
                    matchInfo.getAliasMapping().inverse());

            Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(columnUsage, inputSql);
            int begin = startEndPos.getFirst();
            int end = startEndPos.getSecond();

            ReplaceRange replaceRange = new ReplaceRange();
            replaceRange.beginPos = begin;
            replaceRange.endPos = end;

            replaceRange.column = columnUsage;
            replaceRange.replaceExpr = replaced;

            toBeReplacedUsages.add(replaceRange);
        }

        Collections.sort(toBeReplacedUsages, new Comparator<ReplaceRange>() {
            @Override
            public int compare(ReplaceRange o1, ReplaceRange o2) {
                return Integer.compare(o2.beginPos, o1.beginPos);
            }
        });

        //check
        ReplaceRange last = null;
        for (ReplaceRange toBeReplaced : toBeReplacedUsages) {
            if (last != null) {
                Preconditions.checkState(last.beginPos > toBeReplaced.endPos,
                        "Positions for two column usage has overlaps");
            }

            logger.debug("Column Usage at [" + toBeReplaced.beginPos + "," + toBeReplaced.endPos + "] " + " is replaced by  " + toBeReplaced.replaceExpr);

            String pattern = (needParenthesis(inputSql, topColumns, toBeReplaced))? "{0}({1}){2}": "{0}{1}{2}";
            result = MessageFormat.format(pattern,
                    result.substring(0, toBeReplaced.beginPos), toBeReplaced.replaceExpr, result.substring(toBeReplaced.endPos));

            last = toBeReplaced;// new Pair<>(toBeReplaced.getFirst(), new Pair<>(start, end+6));// toBeReplaced;
        }

        return Pair.newPair(result, toBeReplacedUsages.size());
    }

    //find child inner select first
    static class ColumnUsagesFinder extends SqlBasicVisitor<SqlNode> {
        private List<SqlIdentifier> usages;
        private Set<String> columnNames;

        ColumnUsagesFinder(Set<String> columnNames) {
            this.columnNames = columnNames;
            this.usages = Lists.newArrayList();
        }

        public static List<SqlIdentifier> getColumnUsages(SqlCall selectOrOrderby, Set<String> columnNames)
                throws SqlParseException {
            ColumnUsagesFinder sqlSubqueryFinder = new ColumnUsagesFinder(columnNames);
            selectOrOrderby.accept(sqlSubqueryFinder);
            return sqlSubqueryFinder.getUsages();
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

    }

}
