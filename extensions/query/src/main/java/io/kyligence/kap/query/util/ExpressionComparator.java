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
import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExpressionComparator {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionComparator.class);

    static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, QueryAliasMatchInfo queryAliasMatchInfo) {
        try {
            if (queryNode == null) {
                return exprNode == null;
            } else if (exprNode == null) {
                return false;
            }

            if (!Objects.equals(queryNode.getClass().getSimpleName(), exprNode.getClass().getSimpleName())) {
                return false;
            }

            if (queryNode instanceof SqlCall) {
                if (!(exprNode instanceof SqlCall)) {
                    return false;
                }

                SqlCall thisNode = (SqlCall) queryNode;
                SqlCall thatNode = (SqlCall) exprNode;

                if (!thisNode.getOperator().getName().equalsIgnoreCase(thatNode.getOperator().getName())) {
                    return false;
                }
                return isNodeListEqual(thisNode.getOperandList(), thatNode.getOperandList(), queryAliasMatchInfo);
            }
            if (queryNode instanceof SqlLiteral) {
                if (!(exprNode instanceof SqlLiteral)) {
                    return false;
                }

                SqlLiteral thisNode = (SqlLiteral) queryNode;
                SqlLiteral thatNode = (SqlLiteral) exprNode;

                return Objects.equals(thisNode.getValue(), thatNode.getValue());
            }
            if (queryNode instanceof SqlNodeList) {
                if (!(exprNode instanceof SqlNodeList)) {
                    return false;
                }

                SqlNodeList thisNode = (SqlNodeList) queryNode;
                SqlNodeList thatNode = (SqlNodeList) exprNode;

                if (thisNode.getList().size() != thatNode.getList().size()) {
                    return false;
                }

                for (int i = 0; i < thisNode.getList().size(); i++) {
                    SqlNode thisChild = thisNode.getList().get(i);
                    final SqlNode thatChild = thatNode.getList().get(i);
                    if (!isNodeEqual(thisChild, thatChild, queryAliasMatchInfo)) {
                        return false;
                    }
                }
                return true;
            }

            if (queryNode instanceof SqlIdentifier) {
                if (!(exprNode instanceof SqlIdentifier)) {
                    return false;
                }
                SqlIdentifier thisNode = (SqlIdentifier) queryNode;
                SqlIdentifier thatNode = (SqlIdentifier) exprNode;

                return isSqlIdentifierEqual(thisNode, thatNode, queryAliasMatchInfo);
            }

            if (queryNode instanceof SqlDataTypeSpec) {
                if (!(exprNode instanceof SqlDataTypeSpec))
                    return false;

                SqlDataTypeSpec thisNode = (SqlDataTypeSpec) queryNode;
                SqlDataTypeSpec thatNode = (SqlDataTypeSpec) exprNode;
                return isSqlDataTypeSpecEqual(thisNode, thatNode);
            }

            return false;
        } catch (Exception e) {
            logger.error("Exception while running isNodeEqual, return false", e);
            return false;
        }
    }

    private static boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier,
            QueryAliasMatchInfo queryAliasMatchInfo) {
        Preconditions.checkNotNull(exprSqlIdentifier.names.size() == 2);
        String queryAlias = null, queryCol = null;
        if (querySqlIdentifier.names.size() == 1) {
            queryCol = querySqlIdentifier.names.get(0);
            TblColRef tblColRef = QueryAliasMatchInfo.resolveTblColRef(queryAliasMatchInfo.getQueryAlias(), queryCol);
            queryAlias = tblColRef.getTableAlias();
        } else if (querySqlIdentifier.names.size() == 2) {
            queryCol = querySqlIdentifier.names.get(1);
            queryAlias = querySqlIdentifier.names.get(0);
        }

        //translate user alias to alias in model
        String modelAlias = queryAliasMatchInfo.getAliasMapping().get(queryAlias);
        Preconditions.checkNotNull(modelAlias);
        Preconditions.checkNotNull(queryCol);

        return StringUtils.equals(modelAlias, exprSqlIdentifier.names.get(0))
                && StringUtils.equals(queryCol, exprSqlIdentifier.names.get(1));
    }

    private static boolean isSqlDataTypeSpecEqual(SqlDataTypeSpec querySqlDataTypeSpec,
            SqlDataTypeSpec exprSqlDataTypeSpec) {
        if (querySqlDataTypeSpec.getTypeName() == null
                || CollectionUtils.isEmpty(querySqlDataTypeSpec.getTypeName().names))
            return false;
        if (querySqlDataTypeSpec.getTypeName().names.size() != exprSqlDataTypeSpec.getTypeName().names.size())
            return false;

        for (int i = 0; i < querySqlDataTypeSpec.getTypeName().names.size(); i++) {
            String queryName = querySqlDataTypeSpec.getTypeName().names.get(i);
            if (!exprSqlDataTypeSpec.getTypeName().names.contains(queryName)) {
                return false;
            }
        }

        return querySqlDataTypeSpec.getScale() == exprSqlDataTypeSpec.getScale()
                && querySqlDataTypeSpec.getPrecision() == exprSqlDataTypeSpec.getPrecision();
    }

    private static boolean isNodeListEqual(List<SqlNode> queryNodeList, List<SqlNode> exprNodeList,
            QueryAliasMatchInfo queryAliasMatchInfo) {
        if (queryNodeList.size() != exprNodeList.size()) {
            return false;
        }
        for (int i = 0; i < queryNodeList.size(); i++) {
            if (!isNodeEqual(queryNodeList.get(i), exprNodeList.get(i), queryAliasMatchInfo)) {
                return false;
            }
        }
        return true;
    }
}
