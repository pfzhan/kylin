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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OBIEEConverter implements KapQueryUtil.IQueryTransformer, IPushDownConverter {

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return doConvert(sql, project);
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return doConvert(originSql, project);
    }

    public String doConvert(String originSql, String project) {
        try {
            SqlNode sqlNode = CalciteParser.parse(originSql, project);
            List<SqlNumericLiteral> sqlNumericLiterals = new ArrayList<>(numbersToTrim(sqlNode));
            CalciteParser.descSortByPosition(sqlNumericLiterals);
            final StringBuilder sb = new StringBuilder(originSql);
            for (SqlNumericLiteral numLit : sqlNumericLiterals) {
                if (numLit.getTypeName() == SqlTypeName.DECIMAL && numLit.getPrec() >= 0 && numLit.getScale() > 0) {
                    BigDecimal number = ((BigDecimal) numLit.getValue());
                    String numStr = number.toString();
                    int i = numStr.length() - numLit.getScale();
                    for ( ; i < numStr.length(); i++) {
                        if (numStr.charAt(i) != '0') {
                            break;
                        }
                    }
                    if (i == numStr.length()) {
                        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(numLit, originSql);
                        String trimmedNumStr = number.setScale(0, RoundingMode.FLOOR).toString();
                        sb.replace(replacePos.getFirst(), replacePos.getSecond(), trimmedNumStr);
                    }
                }
            }
            return sb.toString();
        } catch (SqlParseException e) {
            log.warn("Error converting sql for OBIEE", e);
            return originSql;
        }
    }

    private Set<SqlNumericLiteral> numbersToTrim(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            return numbersToTrimFromBasicCall((SqlBasicCall) node);
        } else if (node instanceof SqlCall) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : ((SqlCall) node).getOperandList()) {
                numbers.addAll(numbersToTrim(op));
            }
            return numbers;
        } else {
            return Collections.emptySet();
        }
    }

    private Set<SqlNumericLiteral> numbersToTrimFromBasicCall(SqlBasicCall call) {
        if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : call.getOperands()) {
                if (op instanceof SqlNumericLiteral) {
                    numbers.add((SqlNumericLiteral) op);
                }
            }
            return numbers.size() == 1 ? numbers : Collections.emptySet();
        } else if (call.getOperator() == SqlStdOperatorTable.IN) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : ((SqlNodeList) call.getOperands()[1]).getList()) {
                if (op instanceof SqlNumericLiteral) {
                    numbers.add((SqlNumericLiteral) op);
                }
            }
            return numbers;
        } else {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : call.getOperands()) {
                numbers.addAll(numbersToTrim(op));
            }
            return numbers;
        }
    }

}
