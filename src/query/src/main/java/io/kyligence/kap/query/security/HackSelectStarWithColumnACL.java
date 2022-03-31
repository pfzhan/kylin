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
import java.util.Objects;
import java.util.Set;

import io.kyligence.kap.query.exception.NoAuthorizedColsError;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

public class HackSelectStarWithColumnACL implements QueryUtil.IQueryTransformer, IPushDownConverter {

    private static final String SELECT_STAR = "*";

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return transform(originSql, project, defaultSchema);
    }

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        QueryContext.AclInfo aclLocal = QueryContext.current().getAclInfo();
        if (!KylinConfig.getInstanceFromEnv().isAclTCREnabled() || hasAdminPermission(aclLocal)) {
            return sql;
        }

        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql, project);
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }

        if (!isSingleSelectStar(sqlNode)) {
            return sql;
        }

        String newSelectClause = getNewSelectClause(sqlNode, project, defaultSchema, aclLocal);
        int selectStarPos = getSelectStarPos(sql, sqlNode);
        StringBuilder result = new StringBuilder(sql);
        result.replace(selectStarPos, selectStarPos + 1, newSelectClause);
        return result.toString();
    }

    static String getNewSelectClause(SqlNode sqlNode, String project, String defaultSchema,
            QueryContext.AclInfo aclInfo) {
        StringBuilder newSelectClause = new StringBuilder();
        List<String> allCols = getColsCanAccess(sqlNode, project, defaultSchema, aclInfo);
        if (CollectionUtils.isEmpty(allCols)) {
            throw new NoAuthorizedColsError();
        }
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
            QueryContext.AclInfo aclInfo) {
        List<String> cols = new ArrayList<>();
        String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        final List<AclTCR> aclTCRs = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getAclTCRs(user, groups);
        List<RowFilter.Table> tblWithAlias = RowFilter.getTblWithAlias(defaultSchema, getSingleSelect(sqlNode));
        for (RowFilter.Table table : tblWithAlias) {
            TableDesc tableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getTableDesc(table.getName());
            if (Objects.isNull(tableDesc)) {
                throw new IllegalStateException(
                        "Table " + table.getAlias() + " not found. Please add table " + table.getAlias()
                                + " to data source. If this table does exist, mention it as DATABASE.TABLE.");
            }

            List<ColumnDesc> columns = Lists.newArrayList(tableDesc.getColumns());
            Collections.sort(columns, Comparator.comparing(ColumnDesc::getZeroBasedIndex));
            String quotingChar = Quoting.valueOf(KylinConfig.getInstanceFromEnv().getCalciteQuoting()).string;
            for (ColumnDesc column : columns) {
                if (aclTCRs.stream()
                        .anyMatch(aclTCR -> aclTCR.isAuthorized(tableDesc.getIdentity(), column.getName()))) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(quotingChar).append(table.getAlias()).append(quotingChar) //
                            .append('.') //
                            .append(quotingChar).append(column.getName()).append(quotingChar);
                    cols.add(sb.toString());
                }
            }
        }
        return cols;
    }

    private static boolean isSingleSelectStar(SqlNode sqlNode) {
        if (SelectNumVisitor.getSelectNum(sqlNode) != 1 || sqlNode instanceof SqlExplain) {
            return false;
        }
        SqlSelect singleSelect = getSingleSelect(sqlNode);
        return singleSelect.getSelectList().toString().equals(SELECT_STAR);
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

    private static boolean hasAdminPermission(QueryContext.AclInfo aclInfo) {
        if (Objects.isNull(aclInfo) || Objects.isNull(aclInfo.getGroups())) {
            return false;
        }
        return aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals) || aclInfo.isHasAdminPermission();
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
