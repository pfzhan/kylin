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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.base.Preconditions;

//find child inner select first
public class SqlSubqueryFinder extends SqlBasicVisitor<SqlNode> {
    private List<SqlCall> sqlSelectsOrOrderbys;

    SqlSubqueryFinder() {
        this.sqlSelectsOrOrderbys = new ArrayList<>();
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
        if (call instanceof SqlSelect) {
            sqlSelectsOrOrderbys.add(call);
        }

        if (call.getKind().equals(SqlKind.UNION)) {
            sqlSelectsOrOrderbys.add(call);
        }

        if (call instanceof SqlOrderBy) {
            SqlCall sqlCall = sqlSelectsOrOrderbys.get(sqlSelectsOrOrderbys.size() - 1);
            Preconditions.checkState(((SqlOrderBy) call).query == sqlCall);
            sqlSelectsOrOrderbys.set(sqlSelectsOrOrderbys.size() - 1, call);
        }
        return null;
    }

    public static List<SqlCall> getSubqueries(String sql) throws SqlParseException {
        SqlNode parsed = CalciteParser.parse(sql);
        SqlSubqueryFinder sqlSubqueryFinder = new SqlSubqueryFinder();
        parsed.accept(sqlSubqueryFinder);
        return sqlSubqueryFinder.getSqlSelectsOrOrderbys();
    }
}
