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
package io.kyligence.kap.metadata.model.alias;

import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExpressionComparator {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionComparator.class);

    /**
     *
     * @param queryNode
     * @param exprNode
     * @param aliasMapping
     * @param aliasDeduce is only required if column reference in queryNode might be COL instead of ALIAS.COL
     * @return
     */
    public static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, final AliasMapping aliasMapping,
            final AliasDeduce aliasDeduce) {
        if (aliasMapping == null) {
            return false;
        }
        return isNodeEqual(queryNode, exprNode, new AliasMachingSqlNodeComparator(aliasMapping, aliasDeduce));
    }

    public static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, SqlNodeComparator nodeComparator) {
        try {
            Preconditions.checkNotNull(nodeComparator);
            return nodeComparator.isSqlNodeEqual(queryNode, exprNode);
        } catch (Exception e) {
            logger.error("Exception while running isNodeEqual, return false", e);
            return false;
        }
    }

    public static class AliasMachingSqlNodeComparator extends SqlNodeComparator {
        private final AliasMapping aliasMapping;
        private final AliasDeduce aliasDeduce;

        public AliasMachingSqlNodeComparator(AliasMapping aliasMapping, AliasDeduce aliasDeduce) {
            this.aliasMapping = aliasMapping;
            this.aliasDeduce = aliasDeduce;
        }

        protected boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier) {
            Preconditions.checkState(exprSqlIdentifier.names.size() == 2);
            String queryAlias = null, queryCol = null;
            if (querySqlIdentifier.isStar()) {
                return exprSqlIdentifier.isStar();
            } else if (exprSqlIdentifier.isStar()) {
                return false;
            }

            try {
                if (querySqlIdentifier.names.size() == 1) {
                    queryCol = querySqlIdentifier.names.get(0);
                    queryAlias = aliasDeduce.deduceAlias(queryCol);
                } else if (querySqlIdentifier.names.size() == 2) {
                    queryCol = querySqlIdentifier.names.get(1);
                    queryAlias = querySqlIdentifier.names.get(0);
                }

                //translate user alias to alias in model
                String modelAlias = aliasMapping.getAliasMapping().get(queryAlias);
                Preconditions.checkNotNull(modelAlias);
                Preconditions.checkNotNull(queryCol);

                return StringUtils.equals(modelAlias, exprSqlIdentifier.names.get(0))
                        && StringUtils.equals(queryCol, exprSqlIdentifier.names.get(1));
            } catch (NullPointerException | IllegalStateException e) {
                logger.trace("met exception when doing expressions[{}, {}] comparison", querySqlIdentifier,
                        exprSqlIdentifier, e);
                return false;
            }
        }
    }

    public abstract static class SqlNodeComparator {
        protected abstract boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier,
                SqlIdentifier exprSqlIdentifier);

        public boolean isSqlNodeEqual(SqlNode queryNode, SqlNode exprNode) {
            if (queryNode == null) {
                return exprNode == null;
            }

            if (exprNode == null) {
                return false;
            }

            if (!Objects.equals(queryNode.getClass().getSimpleName(), exprNode.getClass().getSimpleName())) {
                return false;
            }

            if (queryNode instanceof SqlCall) {
                SqlCall thisNode = (SqlCall) queryNode;
                SqlCall thatNode = (SqlCall) exprNode;
                if (!thisNode.getOperator().getName().equalsIgnoreCase(thatNode.getOperator().getName())) {
                    return false;
                }
                return isNodeListEqual(thisNode.getOperandList(), thatNode.getOperandList());
            }
            if (queryNode instanceof SqlLiteral) {
                SqlLiteral thisNode = (SqlLiteral) queryNode;
                SqlLiteral thatNode = (SqlLiteral) exprNode;
                return Objects.equals(thisNode.getValue(), thatNode.getValue());
            }
            if (queryNode instanceof SqlNodeList) {
                SqlNodeList thisNode = (SqlNodeList) queryNode;
                SqlNodeList thatNode = (SqlNodeList) exprNode;
                if (thisNode.getList().size() != thatNode.getList().size()) {
                    return false;
                }
                for (int i = 0; i < thisNode.getList().size(); i++) {
                    SqlNode thisChild = thisNode.getList().get(i);
                    final SqlNode thatChild = thatNode.getList().get(i);
                    if (!isSqlNodeEqual(thisChild, thatChild)) {
                        return false;
                    }
                }
                return true;
            }

            if (queryNode instanceof SqlIdentifier) {
                SqlIdentifier thisNode = (SqlIdentifier) queryNode;
                SqlIdentifier thatNode = (SqlIdentifier) exprNode;
                return isSqlIdentifierEqual(thisNode, thatNode);
            }

            if (queryNode instanceof SqlDataTypeSpec) {
                SqlDataTypeSpec thisNode = (SqlDataTypeSpec) queryNode;
                SqlDataTypeSpec thatNode = (SqlDataTypeSpec) exprNode;
                return isSqlDataTypeSpecEqual(thisNode, thatNode);
            }

            if (queryNode instanceof SqlIntervalQualifier) {
                SqlIntervalQualifier thisNode = (SqlIntervalQualifier) queryNode;
                SqlIntervalQualifier thatNode = (SqlIntervalQualifier) exprNode;
                return isSqlIntervalQualifierEqual(thisNode, thatNode);
            }

            return false;
        }

        private boolean isSqlIntervalQualifierEqual(SqlIntervalQualifier querySqlIntervalQualifier,
                SqlIntervalQualifier exprIntervalQualifier) {
            return querySqlIntervalQualifier.getUnit().equals(exprIntervalQualifier.getUnit());
        }

        protected boolean isSqlDataTypeSpecEqual(SqlDataTypeSpec querySqlDataTypeSpec,
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

        protected boolean isNodeListEqual(List<SqlNode> queryNodeList, List<SqlNode> exprNodeList) {
            if (queryNodeList.size() != exprNodeList.size()) {
                return false;
            }
            for (int i = 0; i < queryNodeList.size(); i++) {
                if (!isSqlNodeEqual(queryNodeList.get(i), exprNodeList.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

}
