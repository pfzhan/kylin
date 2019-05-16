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
import java.util.regex.Pattern;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJdbcDataTypeName;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class QueryPatternUtil {

    /** See ref: RedshiftSqlDialect. */
    private static class KySqlDialect extends SqlDialect {
        static final SqlDialect DEFAULT = new KySqlDialect(EMPTY_CONTEXT.withIdentifierQuoteString("\""));

        private KySqlDialect(Context context) {
            super(context);
        }

        @Override
        protected boolean allowsAs() {
            return false;
        }

        @Override
        public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
            unparseFetchUsingLimit(writer, offset, fetch);
        }
    }

    private static class SqlSelectForPattern extends SqlSelect {
        private SqlSelectForPattern(SqlSelect select) {
            super(select.getParserPosition(), null, select.getSelectList(), select.getFrom(), select.getWhere(),
                    select.getGroup(), select.getHaving(), select.getWindowList(), select.getOrderList(),
                    select.getOffset(), select.getFetch());
        }

        @Override
        public List<SqlNode> getOperandList() {
            // skip adding selectList, group, orderList into operand list
            return ImmutableNullableList.of(getFrom(), getWhere(), getHaving(), getWindowList(), getOffset(),
                    getFetch());
        }
    }

    private static class SqlOrderByForPattern extends SqlOrderBy {
        private SqlOrderByForPattern(SqlOrderBy orderBy) {
            super(orderBy.getParserPosition(), orderBy.query, orderBy.orderList, orderBy.offset, orderBy.fetch);
        }

        @Override
        public List<SqlNode> getOperandList() {
            // skip adding orderList into operandList
            return ImmutableNullableList.of(query, offset, fetch);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(QueryPatternUtil.class);
    private static final SqlDialect DEFAULT_DIALECT = KySqlDialect.DEFAULT;

    private static final String DEFAULT_DATE = "2010-01-01";
    private static final String DEFAULT_DATE_GT = "2010-01-02";
    private static final DateString DEFAULT_DATE_STR = new DateString(DEFAULT_DATE);
    private static final DateString DEFAULT_DATE_STR_GT = new DateString(DEFAULT_DATE_GT);

    private static final String DEFAULT_TIMESTAMP = "2010-01-01 00:00:00";
    private static final String DEFAULT_TIMESTAMP_GT = "2010-01-02 00:00:00";
    private static final TimestampString DEFAULT_TIMESTAMP_STR = new TimestampString(DEFAULT_TIMESTAMP);
    private static final TimestampString DEFAULT_TIMESTAMP_STR_GT = new TimestampString(DEFAULT_TIMESTAMP_GT);

    private static final String DEFAULT_TIME = "01:00:00";
    private static final String DEFAULT_TIME_GT = "02:00:00";
    private static final TimeString DEFAULT_TIME_STR = new TimeString(DEFAULT_TIME);
    private static final TimeString DEFAULT_TIME_STR_GT = new TimeString(DEFAULT_TIME_GT);

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
     * e.g. A > 10 -> "A" > 1
     *      A <= 6 -> "A" <= 2
     *      18 > A -> 2 > "A"
     *      100 = 1000 -> 'A' = 'Z'
     *      2 = 2 -> 'A' = 'A'
     *      'abc' = 'abc' -> 'A' = 'A'
     *      'ABC' = 'XYZ' -> 'A' = 'Z'
     *      A < 'Job' -> "A" < 'Z'
     *      '12' in ('12', '15') -> 'A' in ('A')
     *      '12' in ('13', '16') -> 'Z' in ('A')
     *      '12' not in ('12', '14') -> 'A' not in ('A')
     *      '13' not in ('12', '14') -> 'Z' not in ('A')
     *      A in (10, 20, 30) -> "A" in (1)
     *      A in (TIMESTAMP '2012-01-01 00:00:00.000', ...) -> "A" in (TIMESTAMP '2010-01-01 00:00:00')
     *      A in (Date '2018-03-08', ... ) -> "A" in (DATE '2010-01-01')
     *      A not in ('Bob', 'Sam') -> "A" NOT IN ('A')
     *      A like "%10" -> "A" LIKE 'A'
     *      A between 10 and 20 -> "A" BETWEEN ASYMMETRIC 1 AND 1
     *      A <= '1998-10-10' -> "A" <= '2010-01-02'
     *      A > date '1950-03-29' -> A > DATE '2010-01-01'
     *      interval '10' year -> INTERVAL '1' DAY
     *      {fn convert('123.34', double)} -> {fn CONVERT('1', SQL_DOUBLE)}
     *      {fn CONVERT('apple', VARCHAR)} -> {fn CONVERT('A', SQL_VARCHAR)}
     *      CAST('20191210' AS bigint) -> CAST('1' AS BIGINT)
     *      CAST('abc' AS VARCHAR) -> CAST('A' AS VARCHAR)
     *
     * @param sqlToNormalize input SQL statement which needs to normalize
     * @return normalized SQL statement in uppercase, if there is a parsing error, return original SQL statement
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
        // normalize offset and limit clause
        if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            SqlParserPos pos = sqlOrderBy.getParserPosition();
            SqlNode offset = sqlOrderBy.offset == null ? null
                    : SqlLiteral.createExactNumeric("1", sqlOrderBy.offset.getParserPosition());
            SqlNode fetch = sqlOrderBy.fetch == null ? null
                    : SqlLiteral.createExactNumeric("1", sqlOrderBy.fetch.getParserPosition());
            sqlNode = new SqlOrderBy(pos, sqlOrderBy.query, sqlOrderBy.orderList, offset, fetch);
        }
        patternGenerator.visit((SqlCall) sqlNode);
        return sqlNode.toSqlString(DEFAULT_DIALECT).toString();
    }

    private static class PatternGenerator extends SqlBasicVisitor {

        @Override
        public Object visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                call = new SqlSelectForPattern((SqlSelect) call);
            }

            if (call instanceof SqlOrderBy) {
                call = new SqlOrderByForPattern((SqlOrderBy) call);
            }

            if (call instanceof SqlBasicCall) {

                SqlBasicCall basicCall = (SqlBasicCall) call;
                SqlOperator operator = basicCall.getOperator();
                if (operator instanceof SqlAggFunction) {
                    return null;
                }
                SqlKind operatorKind = operator.getKind();
                List<SqlNode> operandList = basicCall.getOperandList();

                switch (operatorKind) {
                case IN:
                case NOT_IN:
                    normalizeInClause(basicCall);
                    break;
                case JDBC_FN: // select { fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) } from A
                    normalizeJdbcFunctionClause(basicCall);
                    break;
                case AS: // select 2 as column_2 from tableA
                    break;
                case CAST:
                    normalizeCastClause(basicCall);
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
                return mockSqlCharStringLiteral(literal, useGreaterValue, position);
            }

            if (literal instanceof SqlAbstractDateTimeLiteral) {
                return mockDateTimeLiteral(literal, useGreaterValue, position);
            }

            if (literal instanceof SqlIntervalLiteral) {
                SqlIntervalQualifier sqlIntervalQualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, position);
                // interval '10' month -> interval '1' day
                return SqlLiteral.createInterval(1, "1", sqlIntervalQualifier, position);
            }

            logger.debug("Current SqlLiteral is not normalized, {}", literal);
            return literal;
        }

        private SqlLiteral mockSqlCharStringLiteral(SqlLiteral literal, boolean useGreaterValue,
                SqlParserPos position) {

            final String value = literal.toValue();
            if (DateTimeCheckUtil.isValidDate(value)) {
                return useGreaterValue ? SqlLiteral.createCharString(DEFAULT_DATE_GT, position)
                        : SqlLiteral.createCharString(DEFAULT_DATE, position);
            } else if (DateTimeCheckUtil.isValidTime(value)) {
                return useGreaterValue ? SqlLiteral.createCharString(DEFAULT_TIME_GT, position)
                        : SqlLiteral.createCharString(DEFAULT_TIME, position);
            } else if (DateTimeCheckUtil.isValidTimestamp(value)) {
                return useGreaterValue ? SqlLiteral.createCharString(DEFAULT_TIMESTAMP_GT, position)
                        : SqlLiteral.createCharString(DEFAULT_TIMESTAMP, position);
            }
            return useGreaterValue ? SqlLiteral.createCharString("Z", position)
                    : SqlLiteral.createCharString("A", position);
        }

        private SqlLiteral mockNumericStringLiteral(SqlLiteral literal, boolean useGreaterValue,
                SqlParserPos position) {

            final Pattern pattern = Pattern.compile("[+-]?\\d+(\\.\\d+)?");

            if (pattern.matcher(literal.toValue()).matches()) {
                return useGreaterValue ? SqlLiteral.createCharString("2", position)
                        : SqlLiteral.createCharString("1", position);
            } else {
                return useGreaterValue ? SqlLiteral.createCharString("Z", position)
                        : SqlLiteral.createCharString("A", position);
            }
        }

        private SqlLiteral mockDateTimeLiteral(SqlLiteral literal, boolean useGreaterValue, SqlParserPos position) {

            Preconditions.checkArgument(literal instanceof SqlAbstractDateTimeLiteral);
            if (literal instanceof SqlDateLiteral) {
                return useGreaterValue ? SqlLiteral.createDate(DEFAULT_DATE_STR_GT, position)
                        : SqlLiteral.createDate(DEFAULT_DATE_STR, position);
            } else if (literal instanceof SqlTimeLiteral) {
                return useGreaterValue ? SqlLiteral.createTime(DEFAULT_TIME_STR_GT, 0, position)
                        : SqlLiteral.createTime(DEFAULT_TIME_STR, 0, position);
            } else { // SqlTimestampLiteral
                return useGreaterValue ? SqlLiteral.createTimestamp(DEFAULT_TIMESTAMP_STR_GT, 0, position)
                        : SqlLiteral.createTimestamp(DEFAULT_TIMESTAMP_STR, 0, position);
            }
        }

        private void normalizeCastClause(SqlBasicCall basicCall) {
            if (basicCall.getOperands().length != 2) {
                return;
            }

            if (basicCall.getOperator().getKind() == SqlKind.CAST
                    && basicCall.operand(0) instanceof SqlCharStringLiteral
                    && basicCall.operand(1) instanceof SqlDataTypeSpec) {

                final SqlLiteral value = basicCall.operand(0);
                final SqlDataTypeSpec type = basicCall.operand(1);
                final SqlParserPos pos = value.getParserPosition();
                String dataType = type.getTypeName().toString();

                if (DataTypeUtil.isNumeric(dataType)) {
                    basicCall.setOperand(0, mockNumericStringLiteral(value, false, pos));
                } else if (DataTypeUtil.isDateTime(dataType) || DataTypeUtil.isCharType(dataType)) {
                    basicCall.setOperand(0, mockSqlCharStringLiteral(value, false, pos));
                }
            }
        }

        private void normalizeJdbcFunctionClause(SqlBasicCall basicCall) {
            if (basicCall.getOperands().length != 2) {
                return;
            }

            // {fn CONVERT(value, type)} -> Cast value into type
            if (basicCall.getOperator().getKind() == SqlKind.JDBC_FN
                    && basicCall.operand(0) instanceof SqlCharStringLiteral
                    && basicCall.operand(1) instanceof SqlLiteral) {

                final SqlLiteral value = basicCall.operand(0);
                final SqlLiteral type = basicCall.operand(1);

                Preconditions.checkArgument(type.getValue() instanceof SqlJdbcDataTypeName);
                SqlJdbcDataTypeName convertedType = (SqlJdbcDataTypeName) type.getValue();
                if (JdbcDataTypeUtil.isDateTime(convertedType) || JdbcDataTypeUtil.isCharType(convertedType)) {
                    basicCall.setOperand(0, mockSqlCharStringLiteral(value, false, value.getParserPosition()));
                } else if (JdbcDataTypeUtil.isNumeric(convertedType)) {
                    basicCall.setOperand(0, mockNumericStringLiteral(value, false, value.getParserPosition()));
                }
            }
        }

        private void normalizeInClause(SqlBasicCall basicCall) {
            // For case operand(1) instance of sqlSelect
            if (!(basicCall.operand(1) instanceof SqlNodeList)) {
                return;
            }

            SqlNode operand1 = basicCall.operand(0);
            SqlNodeList operand2 = basicCall.operand(1);
            if (operand2 == null || operand2.size() == 0) {
                return;
            }

            // '54' in ('52','63')
            if (operand1 instanceof SqlCharStringLiteral) {
                String op1Value = operand1.toString();
                boolean inNodeList = false;
                for (SqlNode node : operand2) {
                    if (node.toString().equals(op1Value)) {
                        inNodeList = true;
                        break;
                    }
                }
                // if op1 is in the nodelist, then 'A' in ('A'), else 'Z' in ('A')
                basicCall.setOperand(0, mockSqlCharStringLiteral((SqlCharStringLiteral) operand1, !inNodeList,
                        operand1.getParserPosition()));
            }

            Set<SqlNode> nodeSet = Sets.newLinkedHashSet();
            for (SqlNode node : operand2) {
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
            if (operand1 instanceof SqlLiteral && operand2 instanceof SqlLiteral) {
                if (operand1.equals(operand2)) {
                    // for case 100 = 100 or 'abc' = 'abc' or '10' = '10'
                    basicCall.setOperand(0,
                            mockSqlCharStringLiteral((SqlLiteral) operand1, false, operand1.getParserPosition()));
                    basicCall.setOperand(1,
                            mockSqlCharStringLiteral((SqlLiteral) operand2, false, operand2.getParserPosition()));
                } else {
                    // for case 2 = 5 or 'abc' = 'xyz'
                    basicCall.setOperand(0,
                            mockSqlCharStringLiteral((SqlLiteral) operand1, false, operand1.getParserPosition()));
                    basicCall.setOperand(1,
                            mockSqlCharStringLiteral((SqlLiteral) operand2, true, operand2.getParserPosition()));
                }
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

    }

    private static class DataTypeUtil {
        private static final Set<String> numericTypes = ImmutableSet.of("BIGINT", "SMALLINT", "TINYINT", "DOUBLE",
                "FLOAT", "NUMERIC", "DECIMAL", "BOOLEAN", "INTEGER", "BINARY", "VARBINARY", "REAL", "SQL_BIGINT",
                "SQL_SMALLINT", "SQL_TINYINT", "SQL_DOUBLE", "SQL_FLOAT", "SQL_REAL", "SQL_NUMERIC", "SQL_DECIMAL",
                "SQL_BOOLEAN", "SQL_INTEGER", "SQL_BINARY", "SQL_VARBINARY");

        private static final Set<String> datetimeTypes = ImmutableSet.of("DATE", "TIME", "TIME_WITH_LOCAL_TIME_ZONE",
                "TIMESTAMP", "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "SQL_DATE", "SQL_TIME", "SQL_TIME_WITH_LOCAL_TIME_ZONE",
                "SQL_TIMESTAMP", "SQL_TIMESTAMP_WITH_LOCAL_TIME_ZONE");

        private static final Set<String> charTypes = ImmutableSet.of("CHAR", "VARCHAR", "SQL_CHAR", "SQL_VARCHAR");

        static boolean isCharType(String typeName) {
            return charTypes.contains(typeName);
        }

        static boolean isDateTime(String typeName) {
            return datetimeTypes.contains(typeName);
        }

        static boolean isNumeric(String typeName) {
            return numericTypes.contains(typeName);
        }
    }

    private static class JdbcDataTypeUtil {
        // All used types are copied from enum of SqlJdbcDateTypeName in calcite.
        private static final Set<SqlJdbcDataTypeName> datetimeTypes = ImmutableSet.of(//
                SqlJdbcDataTypeName.SQL_DATE, SqlJdbcDataTypeName.SQL_TIME,
                SqlJdbcDataTypeName.SQL_TIME_WITH_LOCAL_TIME_ZONE, SqlJdbcDataTypeName.SQL_TIMESTAMP,
                SqlJdbcDataTypeName.SQL_TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        private static final Set<SqlJdbcDataTypeName> numericTypes = ImmutableSet.of(//
                SqlJdbcDataTypeName.SQL_DECIMAL, SqlJdbcDataTypeName.SQL_NUMERIC, SqlJdbcDataTypeName.SQL_BOOLEAN,
                SqlJdbcDataTypeName.SQL_INTEGER, SqlJdbcDataTypeName.SQL_BINARY, SqlJdbcDataTypeName.SQL_VARBINARY,
                SqlJdbcDataTypeName.SQL_TINYINT, SqlJdbcDataTypeName.SQL_SMALLINT, SqlJdbcDataTypeName.SQL_BIGINT,
                SqlJdbcDataTypeName.SQL_REAL, SqlJdbcDataTypeName.SQL_DOUBLE, SqlJdbcDataTypeName.SQL_FLOAT);

        private static final Set<SqlJdbcDataTypeName> charTypes = ImmutableSet.of(SqlJdbcDataTypeName.SQL_CHAR,
                SqlJdbcDataTypeName.SQL_VARCHAR);

        static boolean isCharType(SqlJdbcDataTypeName typeName) {
            return charTypes.contains(typeName);
        }

        static boolean isDateTime(SqlJdbcDataTypeName typeName) {
            return datetimeTypes.contains(typeName);
        }

        static boolean isNumeric(SqlJdbcDataTypeName typeName) {
            return numericTypes.contains(typeName);
        }
    }

    /**
     * copied from calcite: TimeString, DateString, TimestampString
     */
    private static class DateTimeCheckUtil {
        private static final Pattern TIMESTAMP_PTN = Pattern.compile(
                "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?");
        private static final Pattern DATE_PTN = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");
        private static final Pattern TIME_PTN = Pattern.compile("[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?");

        static boolean isValidDate(String value) {
            boolean isMatch = DATE_PTN.matcher(value).matches();
            if (!isMatch) {
                return false;
            }

            int year = Integer.parseInt(value.substring(0, 4));
            if (year < 1 || year > 9999) {
                return false;
            }

            int month = Integer.parseInt(value.substring(5, 7));
            if (month < 1 || month > 12) {
                return false;
            }

            int day = Integer.parseInt(value.substring(8, 10));
            return day >= 1 && day <= 31;
        }

        static boolean isValidTime(String value) {
            boolean isMatch = TIME_PTN.matcher(value).matches();
            if (!isMatch) {
                return false;
            }

            int hour = Integer.parseInt(value.substring(0, 2));
            if (hour < 0 || hour >= 24) {
                return false;
            }

            int minute = Integer.parseInt(value.substring(3, 5));
            if (minute < 0 || minute >= 60) {
                return false;
            }
            int second = Integer.parseInt(value.substring(6, 8));
            return second >= 0 && second < 60;
        }

        static boolean isValidTimestamp(String value) {
            return TIMESTAMP_PTN.matcher(value).matches();
        }
    }
}
