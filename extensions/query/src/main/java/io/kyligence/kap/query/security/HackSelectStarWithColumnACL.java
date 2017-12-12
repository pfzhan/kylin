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
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplain;
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
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class HackSelectStarWithColumnACL implements QueryUtil.IQueryTransformer, IKeep {
    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!isColumnInterceptorEnabled()) {
            return sql;
        }

        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }

        if (!isSingleSelectStar(sqlNode)) {
            return sql;
        }

        QueryContext context = QueryContext.current();
        Set<String> columnBlackList = ColumnACLManager //
                .getInstance(KylinConfig.getInstanceFromEnv()) //
                .getColumnACLByCache(project) //
                .getColumnBlackList(context.getUsername(), context.getGroups()); //

        if (columnBlackList.isEmpty()) {
            return sql;
        }

        String newSelectClause = getNewSelectClause(sqlNode, project, defaultSchema, columnBlackList);
        int selectStarPos = getSelectStarPos(sql, sqlNode);
        StringBuilder result = new StringBuilder(sql);
        result.replace(selectStarPos, selectStarPos + 1, newSelectClause);
        return result.toString();
    }

    static String getNewSelectClause(SqlNode sqlNode, String project, String defaultSchema, Set<String> columnBlackList) {
        StringBuilder newSelectClause = new StringBuilder();
        List<String> allCols = getColsCanAccess(sqlNode, project, defaultSchema, columnBlackList);
        for (String col : allCols) {
            if (!col.equals(allCols.get(allCols.size() - 1))) {
                newSelectClause.append(col).append(", ");
            } else {
                newSelectClause.append(col);
            }
        }
        return newSelectClause.toString();
    }

    static List<String> getColsCanAccess(SqlNode sqlNode, String project, String defaultSchema,
            Set<String> columnBlackList) {
        List<String> cols = new ArrayList<>();

        List<RowFilter.Table> tblWithAlias = RowFilter.getTblWithAlias(defaultSchema, getSingleSelect(sqlNode));
        for (RowFilter.Table table : tblWithAlias) {
            TableDesc tableDesc = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getTableDesc(table.getName(), project);
            List<ColumnDesc> columns = listExposedColumns(project, tableDesc);
            for (ColumnDesc column : columns) {
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

    private static boolean isSingleSelectStar(SqlNode sqlNode) {
        if (SelectNumVisitor.getSelectNum(sqlNode) != 1 || sqlNode instanceof SqlExplain) {
            return false;
        }
        SqlSelect singleSelect = getSingleSelect(sqlNode);
        return singleSelect.getSelectList().toString().equals("*");
    }

    private static int getSelectStarPos(String sql, SqlNode sqlNode) {
        SqlSelect singleSelect = getSingleSelect(sqlNode);
        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(singleSelect.getSelectList(), sql);
        Preconditions.checkState(replacePos.getSecond() - replacePos.getFirst() == 1);
        return replacePos.getFirst();
    }

    private static SqlSelect getSingleSelect(SqlNode sqlNode) {
        if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            return (SqlSelect) orderBy.query;
        } else {
            return (SqlSelect) sqlNode;
        }
    }

    public static List<ColumnDesc> listExposedColumns(String project, TableDesc tableDesc) {
        List<ColumnDesc> columns = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listExposedColumns(project, tableDesc, OLAPSchemaFactory.exposeMore());
        
        Collections.sort(columns, new Comparator<ColumnDesc>() {
            @Override
            public int compare(ColumnDesc o1, ColumnDesc o2) {
                return o1.getZeroBasedIndex() - o2.getZeroBasedIndex();
            }
        });
        return columns;
    }

    static class SelectNumVisitor extends SqlBasicVisitor<SqlNode> {
        int selectNum = 0;

        static int getSelectNum(SqlNode sqlNode) {
            SelectNumVisitor snv = new SelectNumVisitor();
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
