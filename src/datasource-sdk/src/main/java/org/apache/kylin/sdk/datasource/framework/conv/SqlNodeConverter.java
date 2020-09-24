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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.kylin.common.util.Pair;

import com.google.common.base.Preconditions;

public class SqlNodeConverter extends SqlShuttle {

    private final ConvMaster convMaster;

    SqlNodeConverter(ConvMaster convMaster) {
        this.convMaster = convMaster;
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        SqlDataTypeSpec target = convertSqlDataTypeSpec(type);
        return target == null ? super.visit(type) : target;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlNode target = convertSqlCall(call);
        return target == null ? super.visit(call) : target;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        SqlNode target = convertSqlIdentifier(id);
        return target == null ? super.visit(id) : target;
    }

    private SqlDataTypeSpec convertSqlDataTypeSpec(SqlDataTypeSpec typeSpec) {
        return convMaster.findTargetSqlDataTypeSpec(typeSpec);
    }

    private SqlNode convertSqlIdentifier(SqlIdentifier sqlIdentifier) {
        Pair<SqlNode, SqlNode> matched = convMaster.matchSqlFunc(sqlIdentifier);
        if (matched != null) {
            Preconditions.checkState(matched.getFirst() instanceof SqlIdentifier);
            return matched.getSecond();
        } else {
            return null;
        }
    }

    private SqlNode convertSqlCall(SqlCall sqlCall) {
        SqlOperator operator = sqlCall.getOperator();
        if (operator != null) {
            Pair<SqlNode, SqlNode> matched = convMaster.matchSqlFunc(sqlCall);

            if (matched != null) {
                Preconditions.checkState(matched.getFirst() instanceof SqlCall);
                SqlCall sourceTmpl = (SqlCall) matched.getFirst();

                Preconditions.checkState(sourceTmpl.operandCount() == sqlCall.operandCount());
                SqlNode targetTmpl = matched.getSecond();

                boolean isWindowCall = sourceTmpl.getOperator() instanceof SqlOverOperator;
                SqlParamsFinder sqlParamsFinder = SqlParamsFinder.newInstance(sourceTmpl, sqlCall, isWindowCall);
                return targetTmpl.accept(new SqlFuncFiller(sqlParamsFinder.getParamNodes(), isWindowCall));
            }
        }
        return null;
    }

    private class SqlFuncFiller extends SqlShuttle {

        private final Map<Integer, SqlNode> operands;

        private boolean isWindowCall = false;

        private SqlFuncFiller(final Map<Integer, SqlNode> operands) {
            this.operands = operands;
        }

        private SqlFuncFiller(final Map<Integer, SqlNode> operands, boolean isWindowCall) {
            this.operands = operands;
            this.isWindowCall = isWindowCall;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            String maybeParam = id.toString();
            int idx = ParamNodeParser.parseParamIdx(maybeParam);
            if (idx >= 0 && operands.containsKey(idx)) {
                SqlNode sqlNode = operands.get(idx);
                if (sqlNode instanceof SqlIdentifier) {
                    return sqlNode;
                } else {
                    return sqlNode.accept(SqlNodeConverter.this);
                }
            }
            return id;
        }

        @Override
        public SqlNode visit(SqlCall sqlCall) {
            return sqlCall instanceof SqlWindow && isWindowCall ? operands.get(1) : super.visit(sqlCall);
        }
    }
}
