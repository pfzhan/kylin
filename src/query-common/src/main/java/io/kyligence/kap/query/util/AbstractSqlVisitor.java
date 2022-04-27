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

import java.util.List;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;

public abstract class AbstractSqlVisitor extends SqlBasicVisitor<SqlNode> {
    public String originSql;

    protected AbstractSqlVisitor(String originSql) {
        this.originSql = originSql;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        if (isUnion(call)) {
            SqlNode[] operands = ((SqlBasicCall) call).getOperands();
            for (SqlNode operand : operands) {
                operand.accept(this);
            }
        }

        if (call instanceof SqlOrderBy) {
            visitInSqlOrderBy((SqlOrderBy) call);
        }
        if (call instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) call;
            visitInSqlSelect(sqlSelect);
        }

        if (call instanceof SqlWith) {
            sqlWithFound((SqlWith) call);
        }

        if (call instanceof SqlJoin) {
            visitInSqlJoin((SqlJoin) call);
        }

        return null;
    }

    private void visitInSqlOrderBy(SqlOrderBy orderBy) {
        SqlNodeList orderList = orderBy.orderList;
        visitInSqlNodeList(orderList);

        SqlNode limit = orderBy.fetch;
        visitInSqlNode(limit);

        SqlNode offset = orderBy.offset;
        visitInSqlNode(offset);

        SqlNode query = orderBy.query;
        query.accept(this);
    }

    private void visitInSqlSelect(SqlSelect sqlSelect) {
        visitInSelectList(sqlSelect);

        SqlNode from = sqlSelect.getFrom();

        if (from != null) {
            visitInSqlFrom(from);
        }

        visitInSqlWhere(sqlSelect.getWhere());

        visitInSqlNodeList(sqlSelect.getGroup());

        visitInSqlNode(sqlSelect.getHaving());

        visitInSqlNodeList(sqlSelect.getWindowList());

        SqlNode limit = sqlSelect.getFetch();
        visitInSqlNode(limit);

        visitInSqlNode(sqlSelect.getOffset());
    }

    protected void visitInSqlWhere(SqlNode where) {
        visitInSqlNode(where);
    }

    private void visitInSelectList(SqlSelect sqlSelect) {
        SqlNodeList selectNodeList = sqlSelect.getSelectList();
        List<SqlNode> selectList = selectNodeList.getList();
        for (SqlNode selectItem : selectList) {
            if (selectItem instanceof SqlWith) {
                sqlWithFound((SqlWith) selectItem);
            } else if (selectItem instanceof SqlOrderBy) {
                selectItem.accept(this);
            } else if (selectItem instanceof SqlSelect) {
                selectItem.accept(this);
            } else if (isAs(selectItem)) {
                visitInAsNode((SqlBasicCall) selectItem);
            } else if (isSqlBasicCall(selectItem)) {
                visitInSqlBasicCall((SqlBasicCall) selectItem);
            } else {
                visitInSqlNode(selectItem);
            }
        }
    }

    protected void visitInSqlFrom(SqlNode from) {
        if (from instanceof SqlWith) {
            sqlWithFound((SqlWith) from);
        } else if (isAs(from)) {
            visitInAsNode((SqlBasicCall) from);
        } else if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) from;
            visitInSqlJoin(join);
        } else {
            from.accept(this);
        }
    }

    protected void visitInSqlJoin(SqlJoin join) {
        visitInSqlNode(join.getLeft());
        visitInSqlNode(join.getRight());
        visitInSqlNode(join.getCondition());
    }

    protected void visitInAsNode(SqlBasicCall from) {
        SqlNode left = from.getOperands()[0];
        visitInSqlNode(left);
    }

    protected void visitInSqlNode(SqlNode node) {
        if (node == null)
            return;
        if (node instanceof SqlWith) {
            sqlWithFound((SqlWith) node);
        } else if (node instanceof SqlDynamicParam) {
            questionMarkFound((SqlDynamicParam) node);
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

    protected void visitInSqlBasicCall(SqlBasicCall call) {
        SqlNode[] operands = call.getOperands();
        for (SqlNode ope : operands) {
            visitInSqlNode(ope);
        }
    }

    protected void visitInSqlNodeList(SqlNodeList sqlNodeList) {
        if (sqlNodeList == null)
            return;

        List<SqlNode> nodeList = sqlNodeList.getList();
        for (SqlNode node : nodeList) {
            visitInSqlNode(node);
        }
    }

    protected void visitInSqlCase(SqlCase sqlCase) {
        if (sqlCase == null) {
            return;
        }
        List<SqlNode> sqlNodes = sqlCase.getOperandList();
        if (CollectionUtils.isEmpty(sqlNodes)) {
            return;
        }
        for (SqlNode sqlNode : sqlNodes) {
            visitInSqlNode(sqlNode);
        }
    }

    protected void questionMarkFound(SqlDynamicParam questionMark) {
        //do something
    }

    protected void sqlWithFound(SqlWith sqlWith) {
        visitInSqlWithList(sqlWith.withList);
        SqlNode sqlWithQuery = sqlWith.body;
        sqlWithQuery.accept(this);
    }

    protected void visitInSqlWithList(SqlNodeList withList) {
        List<SqlNode> list = withList.getList();
        for (SqlNode node : list) {
            SqlWithItem withItem = (SqlWithItem) node;
            SqlNode query = withItem.query;
            visitInSqlNode(query);
        }
    }

    public static boolean isUnion(SqlCall call) {
        return call instanceof SqlBasicCall && call.getOperator().getKind() == SqlKind.UNION;
    }

    public static boolean isAs(SqlNode call) {
        return call instanceof SqlBasicCall && ((SqlBasicCall) call).getOperator().isName("AS");
    }

    public static boolean isSqlBasicCall(SqlNode call) {
        return call instanceof SqlBasicCall && ((SqlBasicCall) call).getOperands().length != 0;
    }
}