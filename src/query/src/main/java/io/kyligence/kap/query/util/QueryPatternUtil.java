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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.query.util;

import java.util.List;
import java.util.Set;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.DateString;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class QueryPatternUtil {

    private static final Logger logger = LoggerFactory.getLogger(QueryPatternUtil.class);
    private static final SqlDialect HIVE_DIALECT = SqlDialect.DatabaseProduct.HIVE.getDialect();
    private static final String DEFAULT_DATE = "2010-01-01";
    private static final String DEFAULT_DATE_GT = "2010-01-02";
    // Value of default date string is "2010-01-01"
    private static final DateString DEFAULT_DATE_STR = DateString.fromDaysSinceEpoch(14610);
    // "2010-01-02"
    private static final DateString DEFAULT_DATE_STR_GT = DateString.fromDaysSinceEpoch(14611);

    private QueryPatternUtil() {
        throw new IllegalStateException("Wrong usage for utility class.");
    }
    
    public static String normalizeSQLPattern(String sqlToNormalize) {
        if (!KapConfig.getInstanceFromEnv().enableQueryPattern()) {
            return sqlToNormalize;
        }
        return normalizeSQLPatternImpl(sqlToNormalize);
    }

    /**
     * Normalize the SQL pattern
     * e.g. A > 10 -> A > 1
     *      A <= 6 -> A <= 2
     *      18 > A -> 2 > A
     *      A < "Job" -> A < "Z"
     *      A in (10, 20, 30) -> A in (1)
     *      A not in ('Bob', 'Sam') -> A NOT IN ('A')
     *      A like "%10" -> A LIKE ''
     *      A between 10 and 20 -> A BETWEEN ASYMMETRIC 1 AND 1
     *      A <= "1998-10-10" -> A <= "2010-01-02"
     *      A > date "1950-03-29" -> A > DATE "2010-01-01"
     *      interval '10' year -> interval '1' day
     *
     * @param sqlToNormalize input SQL statement which needs to normalize
     * @return               normalized SQL statement in uppercase
     * @throws SqlParseException if there is a parsing error
     */
    static String normalizeSQLPatternImpl(String sqlToNormalize) {
        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sqlToNormalize);
        } catch (SqlParseException e) {
            logger.error("Cannot parse the SQL statement, please check {}", sqlToNormalize, e);
            return sqlToNormalize;
        }
        PatternGenerator patternGenerator = new PatternGenerator();
        patternGenerator.visit((SqlCall) sqlNode);
        String sql = sqlNode.toSqlString(HIVE_DIALECT).toString();
        sql = sql.replaceAll("(?i)default\\.", "\"DEFAULT\".");
        return sql;
    }

    private static class PatternGenerator extends SqlBasicVisitor {

        @Override
        public Object visit(SqlCall call) {
            if (call instanceof SqlBasicCall) {

                SqlBasicCall basicCall = (SqlBasicCall) call;
                SqlKind operator = basicCall.getOperator().getKind();
                List<SqlNode> operandList = basicCall.getOperandList();

                switch (operator) {
                    case IN:
                    case NOT_IN:
                        normalizeInClause(basicCall);
                        break;
                    case JDBC_FN: // select { fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) } from A
                    case AS:  // select 2 as column_2 from tableA
                        break;
                    case EQUALS:
                    case NOT_EQUALS:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        normalizeComparisonClause(basicCall);
                        break;
                    default:
                        // for SUM(1), COUNT(1), etc
                        for (int i = 0; i < operandList.size(); i++) {
                            SqlNode currentNode = operandList.get(i);
                            if (currentNode instanceof SqlLiteral) {
                                SqlLiteral sqlLiteral = (SqlLiteral) currentNode;
                                if (shouldSkipNormalize(sqlLiteral)) {
                                    continue;
                                }
                                SqlLiteral mockLiteral = mockLiteral(sqlLiteral, false);
                                basicCall.setOperand(i, mockLiteral);
                            }
                        }
                }
            }
            return call.getOperator().acceptCall(this, call);
        }

        @Override
        public Object visit(SqlNodeList nodeList) {
            Object result = null;
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode currentNode = nodeList.get(i);
                if (currentNode instanceof SqlLiteral) {
                    SqlLiteral sqlLiteral = (SqlLiteral) currentNode;
                    // SqlNumericLiteral should skip normalize: select 1, 2, 3 from A
                    if (shouldSkipNormalize(sqlLiteral) || sqlLiteral instanceof SqlNumericLiteral) {
                        continue;
                    }
                    SqlLiteral mockLiteral = mockLiteral(sqlLiteral, false);
                    nodeList.set(i, mockLiteral);
                }
                result = currentNode.accept(this);
            }
            return result;
        }

        private SqlLiteral mockLiteral(SqlLiteral literal, boolean useGreaterValue) {
            SqlParserPos position = literal.getParserPosition();

            if (literal instanceof SqlNumericLiteral) {
                return useGreaterValue ? SqlLiteral.createExactNumeric("2", position)
                        : SqlLiteral.createExactNumeric("1", position);
            }

            if (literal instanceof SqlCharStringLiteral) {

                if (isValidDate(literal.toString())) {
                    return useGreaterValue ? SqlLiteral.createCharString(DEFAULT_DATE_GT, position)
                            : SqlLiteral.createCharString(DEFAULT_DATE, position);
                }

                return useGreaterValue ? SqlLiteral.createCharString("Z", position)
                        : SqlLiteral.createCharString("A", position);
            }

            if (literal instanceof SqlDateLiteral) {
                return useGreaterValue ? SqlLiteral.createDate(DEFAULT_DATE_STR_GT, position)
                        : SqlLiteral.createDate(DEFAULT_DATE_STR, position);
            }

            if (literal instanceof SqlIntervalLiteral) {
                SqlIntervalQualifier sqlIntervalQualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, position);
                // interval '10' month -> interval '1' day
                return SqlLiteral.createInterval(1, "1", sqlIntervalQualifier, position);
            }

            logger.debug("Current SqlLiteral is not normalized, {}", literal);
            return literal;
        }

        private void normalizeInClause(SqlBasicCall basicCall) {
            // For case operand(1) instance of sqlSelect
            if (!(basicCall.operand(1) instanceof SqlNodeList)) {
                return;
            }

            SqlNodeList operand2 = basicCall.operand(1);
            if (operand2 == null || operand2.size() == 0) {
                return;
            }

            Set<SqlNode> nodeSet = Sets.newLinkedHashSet();
            for (int i = 0; i < operand2.size(); i++) {
                SqlNode node = operand2.get(i);
                if (node instanceof SqlLiteral) {
                    node = mockLiteral((SqlLiteral) node, false);
                }
                nodeSet.add(node);
            }

            basicCall.setOperand(1, new SqlNodeList(nodeSet, SqlParserPos.ZERO));
        }

        private void normalizeComparisonClause(SqlBasicCall basicCall) {
            SqlKind operatorKind = basicCall.getOperator().getKind();
            boolean isOpLt;
            boolean isOpGt;

            int numOfOperands = basicCall.operands.length;
            if (numOfOperands != 2) {
                return;
            }

            SqlNode operand1 = basicCall.operand(0);
            SqlNode operand2 = basicCall.operand(1);
            // for case 1 = 2
            if (operand1 instanceof SqlLiteral && operand2 instanceof SqlLiteral) {
                return;
            }

            isOpGt = operatorKind.equals(SqlKind.GREATER_THAN) || operatorKind.equals(SqlKind.GREATER_THAN_OR_EQUAL);
            isOpLt = operatorKind.equals(SqlKind.LESS_THAN) || operatorKind.equals(SqlKind.LESS_THAN_OR_EQUAL);

            if (operand1 instanceof SqlLiteral) {
                SqlLiteral mockLiteral = mockLiteral((SqlLiteral) operand1, isOpGt);
                basicCall.setOperand(0, mockLiteral);
            }

            if (operand2 instanceof SqlLiteral) {
                SqlLiteral mockLiteral = mockLiteral((SqlLiteral) operand2, isOpLt);
                basicCall.setOperand(1, mockLiteral);
            }

        }

        private boolean shouldSkipNormalize(SqlLiteral sqlLiteral) {
            Object value = sqlLiteral.getValue();
            // SqlSelectKeyword: distinct, TimeUnit: DAY
            return (value instanceof SqlSelectKeyword) || (value instanceof TimeUnit);
        }

        private boolean isValidDate(String date) {
            date = date.replaceAll("'", "");
            return date.matches("[12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])");
        }

    }
}
