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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.RowACLManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowFilter implements QueryUtil.IQueryTransformer, IKeep {
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        logger.info("---Start to transform SQL with row ACL---");
        Map<String, String> whereCondWithTbls = getWhereCondWithTbls(project.toUpperCase());
        if (whereCondWithTbls.isEmpty()) {
            return sql;
        }
        String sqlWithRowACL = rowFilter(defaultSchema, sql, whereCondWithTbls);
        logger.info("The SQL:\n" + sqlWithRowACL);
        logger.info("---End transform SQL with row ACL---");
        return sqlWithRowACL;
    }

    static String rowFilter(String schema, String inputSQL, Map<String, String> whereCondWithTbls) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(inputSQL)) {
            return "";
        }
        if (whereCondWithTbls.size() == 0) {
            return inputSQL;
        }

        Map<SqlSelect, List<Table>> selectClausesWithTables = getSelectClausesWithTable(inputSQL, schema);
        List<Pair<ReplacePos, String>> toBeReplacedPosAndExprs = getReplacePosWithFilterExpr(inputSQL, whereCondWithTbls, selectClausesWithTables);

        StrBuilder convertedSQL = new StrBuilder(inputSQL);
        for (Pair<ReplacePos, String> toBeReplaced : toBeReplacedPosAndExprs) {
            int start = toBeReplaced.getFirst().getStart();
            int end = toBeReplaced.getFirst().getEnd();
            convertedSQL.replace(start, end, toBeReplaced.getSecond());
        }
        return convertedSQL.toString();
    }

    // Pair<ReplacePos, String> : [replacePos : toBeReplacedExpr]
    private static List<Pair<ReplacePos, String>> getReplacePosWithFilterExpr(
            String inputSQL,
            Map<String, String> whereCondWithTbls,
            Map<SqlSelect, List<Table>> selectClausesWithTables) {

        List<Pair<ReplacePos, String>> toBeReplacedPosAndExprs = new ArrayList<>();

        for (SqlSelect select : selectClausesWithTables.keySet()) {
            ReplacePos replacePos = null;
            StringBuilder whereCond = new StringBuilder();

            List<Table> tables = selectClausesWithTables.get(select);

            //insert row ACL defined condition to user's inputSQL as where clause.
            //Will splice one select clause's all tables's row ACL conditions into one where clause
            for (int i = 0; i < tables.size(); i++) {
                Table table = tables.get(i);
                String cond = whereCondWithTbls.get(table.getName());
                if (StringUtils.isEmpty(cond)) {
                    continue;
                }

                //complete conditions in where clauses with alias
                cond = CalciteParser.insertAliasInExpr(cond, table.getAlias());
                if (i == 0) {
                    if (!select.hasWhere()) {
                        whereCond = new StringBuilder(" WHERE " + whereCond + cond);
                        //CALCITE-1973 get right node's pos instead of from's pos
                        if (select.getFrom() instanceof SqlJoin) {
                            SqlNode right = ((SqlJoin) select.getFrom()).getRight();
                            replacePos = new ReplacePos(CalciteParser.getReplacePos(right, inputSQL).getSecond() + 1);
                            continue;
                        }
                        //if inputSQL doesn't have where clause, splice where clause after from clause.
                        replacePos = new ReplacePos(CalciteParser.getReplacePos(select.getFrom(), inputSQL).getSecond());
                    } else {
                        //if inputSQL have where clause, splice row ACL conditions after where clause.
                        replacePos = new ReplacePos(CalciteParser.getReplacePos(select.getWhere(), inputSQL));
                        whereCond = new StringBuilder(inputSQL.substring(replacePos.getStart(), replacePos.getEnd()) + " AND " + cond);
                    }
                } else {
                    whereCond.append(" AND ").append(cond);
                }
            }

            if (!whereCond.toString().equals("")) {
                toBeReplacedPosAndExprs.add(Pair.newPair(replacePos, whereCond.toString()));
            }
        }

        // latter replace position in the front of the list.
        Collections.sort(toBeReplacedPosAndExprs, new Comparator<Pair<ReplacePos, String>>() {
            @Override
            public int compare(Pair<ReplacePos, String> o1, Pair<ReplacePos, String> o2) {
                return -(o1.getFirst().getEnd() - o2.getFirst().getEnd());
            }
        });

        return toBeReplacedPosAndExprs;
    }

    //selectClause1:{database.table1:alias1, database.table2:alias2}
    private static Map<SqlSelect, List<Table>> getSelectClausesWithTable(String inputSQL, String schema) {
        Map<SqlSelect, List<Table>> selectWithTables = new HashMap<>();

        for (SqlSelect select : SelectClauseFinder.getSelectClauses(inputSQL)) {
            List<Table> tablesWithAlias = NonSubqueryTablesFinder.getTablesWithAlias(select.getFrom());
            if (tablesWithAlias.size() > 0) {
                for (int i = 0; i < tablesWithAlias.size(); i++) {
                    Table table = tablesWithAlias.get(i);
                    // complete table with database schema if table hasn't
                    if (table.getName().split("\\.").length == 1) {
                        tablesWithAlias.set(i, new Table(schema + "." + table.getName(), table.getAlias()));
                    }
                }
                selectWithTables.put(select, tablesWithAlias);
            }
        }
        return selectWithTables;
    }

    /*visitor classes.Get all select nodes, include select clause in subquery*/
    static class SelectClauseFinder extends SqlBasicVisitor<SqlNode> {
        private List<SqlSelect> selects;

        SelectClauseFinder() {
            this.selects = new ArrayList<>();
        }

        private List<SqlSelect> getSelectClauses() {
            return selects;
        }

        static List<SqlSelect> getSelectClauses(String inputSQL) {
            SqlNode node = null;
            try {
                node = CalciteParser.parse(inputSQL);
            } catch (SqlParseException e) {
                throw new RuntimeException(
                        "Failed to parse SQL \'" + inputSQL + "\', please make sure the SQL is valid");
            }
            SelectClauseFinder sv = new SelectClauseFinder();
            node.accept(sv);
            return sv.getSelectClauses();
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                node.accept(this);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                selects.add(select);
            }
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }
    }

    /*visitor classes.Get select clause 's all tablesWithAlias and skip the subquery*/
    static class NonSubqueryTablesFinder extends SqlBasicVisitor<SqlNode> {
        // {table:alias,...}
        private List<Table> tablesWithAlias;

        private NonSubqueryTablesFinder() {
            this.tablesWithAlias = new ArrayList<>();
        }

        private List<Table> getTablesWithAlias() {
            return tablesWithAlias;
        }

        //please pass SqlSelect.getFrom.Pass other sql nodes will lead error.
        static List<Table> getTablesWithAlias(SqlNode node) {
            NonSubqueryTablesFinder sv = new NonSubqueryTablesFinder();
            node.accept(sv);
            return sv.getTablesWithAlias();
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            // skip subquery in the from clause
            if (call instanceof SqlSelect) {
                return null;
            }
            // for the case table alias like "from t t1".The only SqlBasicCall in from clause is "AS".
            // the instanceof SqlIdentifier is for the case that "select * from (select * from t2) t1".subquery as table.
            if (call instanceof SqlBasicCall) {
                SqlBasicCall node = (SqlBasicCall) call;
                if (node.getOperator() instanceof SqlAsOperator && node.getOperands()[0] instanceof SqlIdentifier) {
                    SqlIdentifier id0 = (SqlIdentifier) ((SqlBasicCall) call).getOperands()[0];
                    SqlIdentifier id1 = (SqlIdentifier) ((SqlBasicCall) call).getOperands()[1];
                    String table = id0.toString(); //DB.TABLE OR TABLE
                    String alais = CalciteParser.getLastNthName(id1, 1);
                    tablesWithAlias.add(new Table(table, alais));
                }
                return null;
            }
            if (call instanceof SqlJoin) {
                SqlJoin node = (SqlJoin) call;
                node.getLeft().accept(this);
                node.getRight().accept(this);
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
            // if table has no alias, will come into this method.Put table name as alias
            String table = id.toString().toUpperCase().split("\\.")[id.toString().toUpperCase().split("\\.").length - 1];
            tablesWithAlias.add(new Table(id.toString().toUpperCase(), table));
            return null;
        }
    }

    private static class ReplacePos {
        private int start;
        private int end;

        ReplacePos(Pair<Integer, Integer> pos) {
            this.start = pos.getFirst();
            this.end = pos.getSecond();
        }

        ReplacePos(int pos) {
            this.start = this.end = pos;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }

    //immutable class only for replacing Pair<String, String> for tableWithAlias
    private static class Table {
        private String name;
        private String alias;

        public Table(String name, String alias) {
            this.name = name;
            this.alias = alias;
        }

        public void setTable(Pair<String, String> tableWithAlias) {
            this.name = tableWithAlias.getFirst();
            this.alias = tableWithAlias.getSecond();
        }

        public String getName() {
            return name;
        }

        public String getAlias() {
            return alias;
        }
    }

    public String getUsername() {
        QueryContext context = QueryContext.current();
        return context.getUsername();
    }

    private Map<String, String> getWhereCondWithTbls(String project) {
        RowACLManager rowACLManager = RowACLManager.getInstance(KylinConfig.getInstanceFromEnv());
        Map<String, String> whereCondWithTbls = new HashMap<>();
        try {
            whereCondWithTbls = rowACLManager.getRowACL(project).getQueryUsedCondsByUser(getUsername());
        } catch (IOException e) {
            throw new RuntimeException("Failed to get user defined row ACL");
        }
        return whereCondWithTbls;
    }
}