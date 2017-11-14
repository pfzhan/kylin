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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.RowACLManager;

public class RowFilter implements QueryUtil.IQueryTransformer, IKeep {
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KapConfig.getInstanceFromEnv().isRowACLEnabled()) {
            return sql;
        }
        List<Map<String, String>> allWhereCondWithTbls = getAllWhereCondWithTbls(project);
        // if origin SQL has where clause, add "()", see KAP#2873
        sql = whereClauseBracketsCompletion(defaultSchema, sql, getCandidateTables(allWhereCondWithTbls));

        for (Map<String, String> whereCondWithTbls : allWhereCondWithTbls) {
            sql = rowFilter(defaultSchema, sql, whereCondWithTbls);
        }
        return sql;
    }

    private Set<String> getCandidateTables(List<Map<String, String>> allWhereCondWithTbls) {
        Set<String> candidateTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map<String, String> allWhereCondWithTbl : allWhereCondWithTbls) {
            candidateTables.addAll(allWhereCondWithTbl.keySet());
        }
        return candidateTables;
    }

    static String whereClauseBracketsCompletion(String schema, String inputSQL, Set<String> candidateTables) {
        Map<SqlSelect, List<Table>> selectClausesWithTbls = getSelectClausesWithTbls(inputSQL, schema);
        List<Pair<Integer, String>> toBeInsertedPosAndExprs = new ArrayList<>();

        for (SqlSelect select : selectClausesWithTbls.keySet()) {
            if (!select.hasWhere()) {
                continue;
            }

            for (Table table : selectClausesWithTbls.get(select)) {
                if (candidateTables.contains(table.getName())) {
                    Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(select.getWhere(), inputSQL);
                    toBeInsertedPosAndExprs.add(Pair.newPair(replacePos.getFirst(), "("));
                    toBeInsertedPosAndExprs.add(Pair.newPair(replacePos.getSecond(), ")"));
                    break;
                }
            }
        }
        return afterInsertSQL(inputSQL, toBeInsertedPosAndExprs);
    }

    static String rowFilter(String schema, String inputSQL, Map<String, String> whereCondWithTbls) {
        if (StringUtils.isEmpty(inputSQL)) {
            return "";
        }
        if (StringUtils.isEmpty(schema) || whereCondWithTbls.isEmpty()) {
            return inputSQL;
        }

        Map<SqlSelect, List<Table>> selectClausesWithTbls = getSelectClausesWithTbls(inputSQL, schema);
        List<Pair<Integer, String>> toBeInsertedPosAndExprs = getInsertPosAndExpr(inputSQL, whereCondWithTbls, selectClausesWithTbls);

        if (toBeInsertedPosAndExprs.size() > 0) {
            logger.info("\n---Start to transform SQL with row ACL, see transformed sql in the below---");
        }
        return afterInsertSQL(inputSQL, toBeInsertedPosAndExprs);
    }

    private static String afterInsertSQL(String inputSQL, List<Pair<Integer, String>> toBeInsertedPosAndExprs) {
        // latter replace position in the front of the list.
        Collections.sort(toBeInsertedPosAndExprs, new Comparator<Pair<Integer, String>>() {
            @Override
            public int compare(Pair<Integer, String> o1, Pair<Integer, String> o2) {
                return -(o1.getFirst() - o2.getFirst());
            }
        });

        StrBuilder convertedSQL = new StrBuilder(inputSQL);
        for (Pair<Integer, String> toBeInserted : toBeInsertedPosAndExprs) {
            int insertPos = toBeInserted.getFirst();
            convertedSQL.insert(insertPos, toBeInserted.getSecond());
        }
        return convertedSQL.toString();
    }

    // concat all table's row ACL defined condition and insert to user's inputSQL as where clause.
    // return [insertPos : toBeInsertExpr]
    private static List<Pair<Integer, String>> getInsertPosAndExpr(
            String inputSQL,
            Map<String, String> whereCondWithTbls,
            Map<SqlSelect, List<Table>> selectClausesWithTbls) {

        List<Pair<Integer, String>> toBeReplacedPosAndExprs = new ArrayList<>();

        for (SqlSelect select : selectClausesWithTbls.keySet()) {
            int insertPos = getInsertPos(inputSQL, select);

            //Will concat one select clause's all tables's row ACL conditions into one where clause.
            List<Table> tables = selectClausesWithTbls.get(select);
            String whereCond = getToBeInsertCond(whereCondWithTbls, select, tables);

            if (!whereCond.isEmpty()) {
                toBeReplacedPosAndExprs.add(Pair.newPair(insertPos, whereCond));
            }
        }

        return toBeReplacedPosAndExprs;
    }

    private static int getInsertPos(String inputSQL, SqlSelect select) {
        SqlNode insertAfter = getInsertAfterNode(select);
        Pair<Integer, Integer> pos = CalciteParser.getReplacePos(insertAfter, inputSQL);
        int finalPos = pos.getSecond();
        int bracketNum = 0;
        //move the pos to the rightest ")", if has.
        //bracketNum if for the situation like that: "from ( select * from t where t.a > 0 )", the rightest ")" is not belong to where clause
        for (int j = pos.getFirst() - 1; ; j--) {
            if (inputSQL.charAt(j) == ' ' || inputSQL.charAt(j) == '\t' || inputSQL.charAt(j) == '\n') {
                continue;
            } else if (inputSQL.charAt(j) == '(') {
                bracketNum++;
            } else {
                break;
            }
        }

        for (int i = pos.getSecond(); i < inputSQL.length() && bracketNum > 0; i++) {
            if (inputSQL.charAt(i) == ' ' || inputSQL.charAt(i) == '\t' || inputSQL.charAt(i) == '\n') {
                continue;
            } else if (inputSQL.charAt(i) == ')') {
                finalPos = i + 1;
                bracketNum--;
            } else {
                break;
            }
        }
        return finalPos;
    }

    private static String getToBeInsertCond(Map<String, String> whereCondWithTbls, SqlSelect select, List<Table> tables) {
        StringBuilder whereCond = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            String cond = whereCondWithTbls.get(table.getName());
            if (StringUtils.isEmpty(cond)) {
                continue;
            }

            //complete condition expr with alias
            cond = CalciteParser.insertAliasInExpr(cond, table.getAlias());
            if (i == 0 && !select.hasWhere()) {
                whereCond = new StringBuilder(" WHERE " + cond);
            } else {
                whereCond.append(" AND ").append(cond);
            }
        }
        return whereCond.toString();
    }

    private static SqlNode getInsertAfterNode(SqlSelect select) {
        SqlNode rightMost;
        if (!select.hasWhere()) {
            //CALCITE-1973 get right node's pos instead of from's pos.In KYLIN, join must have on operator
            if (select.getFrom() instanceof SqlJoin) {
                rightMost = Preconditions.checkNotNull(((SqlJoin) select.getFrom()).getCondition(),
                        "Join without \"ON\"");
            } else {
                //if inputSQL doesn't have where clause, concat where clause after from clause.
                rightMost = select.getFrom();
            }
        } else {
            rightMost = select.getWhere();
        }
        return rightMost;
    }

    //{selectClause1:[DB.TABLE1:ALIAS1, DB.TABLE2:ALIAS2]}
    private static Map<SqlSelect, List<Table>> getSelectClausesWithTbls(String inputSQL, String schema) {
        Map<SqlSelect, List<Table>> selectWithTables = new HashMap<>();

        for (SqlSelect select : SelectClauseFinder.getSelectClauses(inputSQL)) {
            List<Table> tblsWithAlias = getTblWithAlias(schema, select);
            if (tblsWithAlias.size() > 0) {
                selectWithTables.put(select, tblsWithAlias);
            }
        }
        return selectWithTables;
    }

    static List<Table> getTblWithAlias(String schema, SqlSelect select) {
        List<Table> tblsWithAlias = NonSubqueryTablesFinder.getTblsWithAlias(select.getFrom());

        // complete table with database schema if table hasn't
        for (int i = 0; i < tblsWithAlias.size(); i++) {
            Table t = tblsWithAlias.get(i);
            if (t.getName().split("\\.").length == 1) {
                tblsWithAlias.set(i, new Table(schema + "." + t.getName(), t.getAlias()));
            }
        }
        return tblsWithAlias;
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

        private List<Table> getTblsWithAlias() {
            return tablesWithAlias;
        }

        //please pass SqlSelect.getFrom.Pass other sql nodes will lead error.
        static List<Table> getTblsWithAlias(SqlNode fromNode) {
            NonSubqueryTablesFinder sv = new NonSubqueryTablesFinder();
            fromNode.accept(sv);
            return sv.getTblsWithAlias();
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

    //immutable class only for replacing Pair<String, String> for tableWithAlias
    static class Table {
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

    private String getUsername() {
        QueryContext context = QueryContext.current();
        return context.getUsername();
    }

    private Set<String> getUserGroups() {
        QueryContext context = QueryContext.current();
        return context.getGroups();
    }

    //get all user/groups's row ACL
    private List<Map<String, String>> getAllWhereCondWithTbls(String project) {
        RowACLManager rowACLManager = RowACLManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<Map<String, String>> list = new ArrayList<>();
        list.add(rowACLManager.getQueryUsedTblToConds(project, getUsername(), MetadataConstants.TYPE_USER));
        for (String group : getUserGroups()) {
            list.add(rowACLManager.getQueryUsedTblToConds(project, group, MetadataConstants.TYPE_GROUP));
        }
        return list;
    }
}