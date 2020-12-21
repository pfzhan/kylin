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

import java.sql.SQLException;
import java.util.Locale;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlConverter {
    private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

    private final IConfigurer configurer;
    private final SqlNodeConverter sqlNodeConverter;
    private final SingleSqlNodeReplacer singleSqlNodeReplacer;

    public SqlConverter(IConfigurer configurer, ConvMaster convMaster) throws SQLException {
        this.sqlNodeConverter = new SqlNodeConverter(convMaster);
        this.configurer = configurer;
        this.singleSqlNodeReplacer = new SingleSqlNodeReplacer(convMaster);
    }

    public String convertSql(String orig) {
        String converted = orig;

        if (!configurer.skipHandleDefault()) {
            String escapedDefault = SqlDialect.CALCITE
                    .quoteIdentifier(configurer.useUppercaseDefault() ? "DEFAULT" : "default");
            converted = converted.replaceAll("(?i)default\\.", escapedDefault + "."); // use Calcite dialect to cater to SqlParser
            converted = converted.replaceAll("\"(?i)default\"\\.", escapedDefault + ".");
        }

        if (!configurer.skipDefaultConvert()) {
            ConvSqlWriter sqlWriter = null;
            String beforeConvert = converted;
            try {
                // calcite cannot recognize `, convert ` to " before parse
                converted = converted.replaceAll("`", "\"");
                sqlWriter = getConvSqlWriter();
                SqlNode sqlNode = SqlParser.create(converted).parseQuery();
                sqlNode = sqlNode.accept(sqlNodeConverter);
                converted = sqlWriter.format(sqlNode);
            } catch (Exception e) {
                logger.error("Failed to default convert sql, will use the origin input: {}", beforeConvert, e);
                // revert to beforeConvert when occur Exception
                converted = beforeConvert;
            } finally {
                if (sqlWriter != null) {
                    sqlWriter.reset();
                }
            }
        }

        converted = configurer.fixAfterDefaultConvert(converted);
        return converted;
    }

    public String convertColumn(String column, String originQuote) {
        String converted = column;
        if (StringUtils.isNotEmpty(originQuote)) {
            converted = column.replace(originQuote, "\"");
        }
        ConvSqlWriter sqlWriter = null;
        try {
            sqlWriter = getConvSqlWriter();
            SqlNode sqlNode = SqlParser.create(converted).parseExpression();
            sqlNode = sqlNode.accept(sqlNodeConverter);
            converted = sqlWriter.format(sqlNode);
            converted = configurer.fixAfterDefaultConvert(converted);
        } catch (Throwable e) {
            logger.error("Failed to default convert Column, will use the input: {}", column, e);
        } finally {
            if (sqlWriter != null) {
                sqlWriter.reset();
            }
        }
        return converted;
    }

    /**
    * for oracle,  oracle does not support convert date to varchar implicitly
    * @param column
    * @param colType
    * @param format
    * @return 
    */
    public String formatDateColumn(String column, DataType colType, String format) {
        String formated = column;
        if (configurer.enableTransformDateToString() && colType != null && colType.isDateTimeFamily()) {
            String datePattern = StringUtils.isNotEmpty(format) ? format : configurer.getTransformDatePattern();
            String template = configurer.getTransformDateToStringExpression();
            formated = String.format(Locale.ROOT, template, column, datePattern);
        }
        return convertColumn(formated, "");
    }

    /**
    * for oracle,  oracle does not support convert date to varchar implicitly
     * ORA-01861: literal does not match format string
     *
     * format date column in where clause
     *
    * @param orig
    * @param partColumn
    * @param partColType
    * @param partColFormat
    * @return 
    */
    public String convertDateCondition(String orig, String partColumn, DataType partColType, String partColFormat) {
        // for jdbc source, convert quote from backtick to double quote
        String converted = orig.replaceAll("`", "\"");

        if (configurer.enableTransformDateToString() && partColType != null && partColType.isDateTimeFamily()) {
            SqlNode sqlNode;
            ConvSqlWriter sqlWhereWriter = null;
            try {
                sqlNode = SqlParser.create(converted).parseQuery();
                if (sqlNode instanceof SqlSelect) {
                    SqlNode sqlNodeWhere = ((SqlSelect) sqlNode).getWhere();
                    if (sqlNodeWhere != null) {
                        ((SqlSelect) sqlNode).setWhere(null);
                        sqlWhereWriter = getConvSqlWriter();
                        String strSelect = sqlWhereWriter.format(sqlNode);
                        StringBuilder sb = new StringBuilder(strSelect);
                        sqlWhereWriter.reset();

                        String datePattern = StringUtils.isNotEmpty(partColFormat) ? partColFormat
                                : configurer.getTransformDatePattern();
                        String template = configurer.getTransformDateToStringExpression();
                        if (StringUtils.isNotEmpty(template)) {
                            SqlNode sqlNodeTryToFind = SqlParser.create(partColumn).parseExpression();
                            SqlNode sqlNodeToReplace = SqlParser
                                    .create(String.format(Locale.ROOT, template, partColumn, datePattern))
                                    .parseExpression();
                            singleSqlNodeReplacer.setSqlNodeTryToFind(sqlNodeTryToFind);
                            singleSqlNodeReplacer.setSqlNodeToReplace(sqlNodeToReplace);
                            sqlNodeWhere = sqlNodeWhere.accept(singleSqlNodeReplacer);
                        }
                        String sqlWhere = sqlWhereWriter.format(sqlNodeWhere);
                        sb.append(" WHERE ").append(sqlWhere);

                        return sb.toString();
                    }
                }
            } catch (Throwable e) {
                logger.error("Failed to default convert date condition for sqoop, will use the input: {}", orig, e);
            } finally {
                if (sqlWhereWriter != null) {
                    sqlWhereWriter.reset();
                }
            }
        }
        return converted;
    }

    public IConfigurer getConfigurer() {
        return configurer;
    }

    public interface IConfigurer {
        default boolean skipDefaultConvert() {
            return false;
        }

        default boolean skipHandleDefault() {
            return false;
        }

        default boolean useUppercaseDefault() {
            return false;
        }

        default String fixAfterDefaultConvert(String orig) {
            return orig;
        }

        default SqlDialect getSqlDialect() {
            return SqlDialect.CALCITE;
        }

        default boolean allowNoOffset() {
            return false;
        }

        default boolean allowFetchNoRows() {
            return false;
        }

        default boolean allowNoOrderByWithFetch() {
            return false;
        }

        default String getPagingType() {
            return "AUTO";
        }

        default boolean isCaseSensitive() {
            return false;
        }

        default boolean enableCache() {
            return false;
        }

        default boolean enableQuote() {
            return false;
        }

        default String fixIdentifierCaseSensitive(String orig) {
            return orig;
        }

        default boolean enableTransformDateToString() {
            return false;
        }

        default String getTransformDateToStringExpression() {
            return "";
        }

        default String getTransformDatePattern() {
            return "";
        }
    }

    private ConvSqlWriter getConvSqlWriter() throws SQLException {
        ConvSqlWriter sqlWriter;
        if ("ROWNUM".equalsIgnoreCase(configurer.getPagingType())) {
            sqlWriter = new ConvRownumSqlWriter(configurer);
        } else {
            sqlWriter = new ConvSqlWriter(configurer);
        }
        sqlWriter.setQuoteAllIdentifiers(false);
        return sqlWriter;
    }
}
