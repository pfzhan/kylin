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
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class HackSelectStarWithColumnACL implements QueryUtil.IQueryTransformer, IKeep {
    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!isSingleSelectStar(sql) || !isColumnInterceptorEnabled()) {
            return sql;
        }

        Set<String> columnBlackList = ColumnACLManager
                .getInstance(KylinConfig.getInstanceFromEnv())
                .getColumnACLByCache(project)
                .getColumnBlackListByUser(QueryContext.current().getUsername());

        if (columnBlackList.isEmpty()) {
            return sql;
        }

        String newSelectClause = getNewSelectClause(sql, project, defaultSchema, columnBlackList);
        int selectStarPos = getSelectStarPos(sql);
        StringBuilder result = new StringBuilder(sql);
        result.replace(selectStarPos, selectStarPos + 1, newSelectClause);
        return result.toString();
    }

    static String getNewSelectClause(String sql, String project, String defaultSchema, Set<String> columnBlackList) {
        StringBuilder newSelectClause = new StringBuilder();
        List<String> allCols = getColsCanAccess(sql, project, defaultSchema, columnBlackList);
        for (String col : allCols) {
            if (!col.equals(allCols.get(allCols.size() - 1))) {
                newSelectClause.append(col).append(", ");
            } else {
                newSelectClause.append(col);
            }
        }
        return newSelectClause.toString();
    }

    private static List<String> getColsCanAccess(String sql, String project, String defaultSchema, Set<String> columnBlackList) {
        List<String> cols = new ArrayList<>();

        List<RowFilter.Table> tblWithAlias = RowFilter.getTblWithAlias(defaultSchema, getSingleSelect(sql));
        for (RowFilter.Table table : tblWithAlias) {
            TableDesc tableDesc = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(table.getName(), project);
            for (ColumnDesc column : tableDesc.getColumns()) {
                if (!columnBlackList.contains(tableDesc.getIdentity() + "." + column.getName())) {
                    cols.add(table.getAlias() + "." + column.getName());
                }
            }
        }
        return cols;
    }

    private static boolean isColumnInterceptorEnabled() {
        return KapConfig.getInstanceFromEnv().isColumnACLEnabled();
    }

    private static boolean isSingleSelectStar(String sql) {
        if (SelectNumVisitor.getSelectNum(sql) != 1) {
            return false;
        }
        SqlSelect singleSelect = getSingleSelect(sql);
        return singleSelect.getSelectList().toString().equals("*");
    }

    private static int getSelectStarPos(String sql) {
        SqlSelect singleSelect = getSingleSelect(sql);
        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(singleSelect.getSelectList(), sql);
        Preconditions.checkState(replacePos.getSecond() - replacePos.getFirst() == 1);
        return replacePos.getFirst();
    }

    private static SqlSelect getSingleSelect(String sql) {
        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }
        if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            return (SqlSelect) orderBy.query;
        } else {
            return (SqlSelect) sqlNode;
        }
    }

    static class SelectNumVisitor extends SqlBasicVisitor<SqlNode> {
        int selectNum = 0;

        static int getSelectNum(String sql) {
            SelectNumVisitor snv = new SelectNumVisitor();
            SqlNode sqlNode;
            try {
                sqlNode = CalciteParser.parse(sql);
            } catch (SqlParseException e) {
                throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
            }
            sqlNode.accept(snv);
            return snv.getNum();
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                selectNum++;
            }
            if (call instanceof SqlOrderBy) {
                SqlOrderBy sqlOrderBy = (SqlOrderBy) call;
                sqlOrderBy.query.accept(this);
                return null;
            }
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }

        private int getNum() {
            return selectNum;
        }
    }
}
