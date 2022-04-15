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
import java.util.List;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.base.Preconditions;

//find child inner select first
public class SqlSubqueryFinder extends SqlBasicVisitor<SqlNode> {
    private List<SqlCall> sqlSelectsOrOrderbys;
    private List<SqlIdentifier> subqueryAlias;
    private boolean includeNestedQueries = false;

    SqlSubqueryFinder() {
        this.sqlSelectsOrOrderbys = new ArrayList<>();
        this.subqueryAlias = new ArrayList<>();
    }

    public SqlSubqueryFinder(boolean includeNestedQueries) {
        this();
        this.includeNestedQueries = includeNestedQueries;
    }

    public static List<SqlCall> getSubqueries(String sql) throws SqlParseException {
        return getSubqueries(sql, false);
    }

    public static List<SqlCall> getSubqueries(String sql, boolean includeNestedQueries) throws SqlParseException {
        SqlNode parsed = CalciteParser.parse(sql);
        SqlSubqueryFinder sqlSubqueryFinder = new SqlSubqueryFinder(includeNestedQueries);
        parsed.accept(sqlSubqueryFinder);
        return sqlSubqueryFinder.getSqlSelectsOrOrderbys();
    }

    //subquery will precede
    List<SqlCall> getSqlSelectsOrOrderbys() {
        return sqlSelectsOrOrderbys;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                operand.accept(this);
            }
        }
        if (call instanceof SqlSelect && ((SqlSelect) call).getFrom() != null) {
            SqlSelect select = (SqlSelect) call;
            RootTableValidator validator = new RootTableValidator();
            select.getFrom().accept(validator);
            if (validator.hasRoot) {
                sqlSelectsOrOrderbys.add(call);
            }
        }
        if (call instanceof SqlWithItem) {
            SqlWithItem sqlWithQuery = (SqlWithItem) call;
            subqueryAlias.add(sqlWithQuery.name);
        }
        if (includeNestedQueries && SqlKind.UNION == call.getKind()) {
            sqlSelectsOrOrderbys.add(call);
        }

        if (call instanceof SqlOrderBy) {
            SqlCall sqlCall = sqlSelectsOrOrderbys.get(sqlSelectsOrOrderbys.size() - 1);
            SqlNode query = ((SqlOrderBy) call).query;
            if (query instanceof SqlWith) {
                query = ((SqlWith) query).body;
            }
            if (query instanceof SqlBasicCall && SqlKind.UNION == query.getKind()) {
                for (SqlNode operand : ((SqlBasicCall) query).getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            } else {
                RootTableValidator validator = new RootTableValidator();
                ((SqlSelect) query).getFrom().accept(validator);
                if (validator.hasRoot) {
                    Preconditions.checkState(query == sqlCall);
                    sqlSelectsOrOrderbys.set(sqlSelectsOrOrderbys.size() - 1, call);
                }
            }
        }
        return null;
    }

    private class RootTableValidator extends SqlBasicVisitor<SqlNode> {

        private boolean hasRoot = true;

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                if (includeNestedQueries) {
                    SqlNodeList list = ((SqlSelect) call).getSelectList();
                    hasRoot = list.get(0).toString().equals("*");
                } else {
                    hasRoot = false; // false if a nested select is found
                }
            } else if (call instanceof SqlJoin) {
                ((SqlJoin) call).getLeft().accept(this);
            } else if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                call.getOperandList().get(0).accept(this);
            } else {
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            }

            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            for (SqlIdentifier alias : subqueryAlias) {
                if (alias.equalsDeep(id, Litmus.IGNORE)) {
                    hasRoot = false;
                    break;
                }
            }
            return null;
        }
    }
}
