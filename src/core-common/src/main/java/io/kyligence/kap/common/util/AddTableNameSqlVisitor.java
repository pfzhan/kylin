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
package io.kyligence.kap.common.util;

import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.ImmutableList;

public class AddTableNameSqlVisitor extends SqlBasicVisitor<Object> {
    private static final String DEFAULT_REASON = "Something went wrong. %s";
    private String expr;
    private Map<String, String> colToTable;
    private Set<String> ambiguityCol;
    private Set<String> allColumn;

    public AddTableNameSqlVisitor(String expr, Map<String, String> colToTable, Set<String> ambiguityCol,
            Set<String> allColumn) {
        this.expr = expr;
        this.colToTable = colToTable;
        this.ambiguityCol = ambiguityCol;
        this.allColumn = allColumn;
    }

    @Override
    public Object visit(SqlIdentifier id) {
        boolean ok = true;
        if (id.names.size() == 1) {
            String column = id.names.get(0).toUpperCase().trim();
            if (!colToTable.containsKey(column) || ambiguityCol.contains(column)) {
                ok = false;
            } else {
                id.names = ImmutableList.of(colToTable.get(column), column);
            }
        } else if (id.names.size() == 2) {
            String table = id.names.get(0).toUpperCase().trim();
            String column = id.names.get(1).toUpperCase().trim();
            ok = allColumn.contains(table + "." + column);
        } else {
            ok = false;
        }
        if (!ok) {
            throw new IllegalArgumentException(
                    "Unrecognized column: " + id.toString() + " in expression '" + expr + "'.");
        }
        return null;
    }

    @Override
    public Object visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            if (call.getOperator() instanceof SqlAsOperator) {
                throw new IllegalArgumentException(String.format(DEFAULT_REASON, "null"));
            }

            if (call.getOperator() instanceof SqlAggFunction) {
                throw new IllegalArgumentException(String.format(DEFAULT_REASON, "null"));
            }
        }
        return call.getOperator().acceptCall(this, call);
    }
}