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
package org.apache.kylin.source.adhocquery;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import lombok.extern.slf4j.Slf4j;

/**
 * <pre>
 * for example:
 *
 *     select ACCOUNT_ID as id, ACCOUNT_COUNTRY as "country" from "DEFAULT".TEST_ACCOUNT
 *
 * will be converted to:
 *
 *      select "ACCOUNT_ID" as "ID", "ACCOUNT_COUNTRY" as "country" from "DEFAULT"."TEST_ACCOUNT"
 * </pre>
 *
 * <P>if unquoted,quote all SqlIdentifier with {@link org.apache.calcite.avatica.util.Quoting#DOUBLE_QUOTE}
 * <P>if already quoted, unchanged
 * </P>and visit SqlIdentifier with
 * {@link DoubleQuoteSqlIdentifierConvert}
 */
@Slf4j
public class DoubleQuotePushDownConverter implements IPushDownConverter, IKeep {

    //inner class for convert SqlIdentifier with DoubleQuote
    private static class DoubleQuoteSqlIdentifierConvert {

        private final String sql;

        public DoubleQuoteSqlIdentifierConvert(String sql) {
            this.sql = sql;
        }

        private SqlNode parse() throws SqlParseException {
            return CalciteParser.parse(this.sql);
        }

        private Collection<SqlIdentifier> getAllSqlIdentifiers() throws SqlParseException {
            Set<SqlIdentifier> allSqlIdentifier = Sets.newHashSet();
            SqlVisitor<Void> sqlVisitor = new SqlBasicVisitor<Void>() {
                @Override
                public Void visit(SqlIdentifier id) {
                    if (!isFunctionWithoutParentheses(id)) {
                        allSqlIdentifier.add(id);
                    }
                    return null;
                }
            };
            parse().accept(sqlVisitor);

            return allSqlIdentifier;
        }

        public String convert() throws SqlParseException {
            final StringBuilder sqlConvertedStringBuilder = new StringBuilder(sql);
            List<SqlIdentifier> sqlIdentifierList = Lists.newArrayList(getAllSqlIdentifiers());
            CalciteParser.descSortByPosition(sqlIdentifierList);
            sqlIdentifierList.forEach(sqlIdentifier -> {
                Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(sqlIdentifier, sql);
                //* 'name is empty
                List<String> toStarNames = SqlIdentifier.toStar(sqlIdentifier.names);
                String newIdentifierStr = toStarNames.stream().map(this::convertIdentifier)
                        .collect(Collectors.joining("."));
                sqlConvertedStringBuilder.replace(replacePos.getFirst(), replacePos.getSecond(), newIdentifierStr);
            });
            return sqlConvertedStringBuilder.toString();
        }

        private String convertIdentifier(String identifierStr) {
            if (identifierStr.equals("*")) {
                return identifierStr;
            } else {
                return Quoting.DOUBLE_QUOTE.string + identifierStr + Quoting.DOUBLE_QUOTE.string;
            }

        }

        /**
         * filter the function without parentheses
         * {@link org.apache.calcite.sql.SqlUtil#makeCall(SqlOperatorTable, SqlIdentifier)}
         * @param id
         * @return
         */
        private boolean isFunctionWithoutParentheses(SqlIdentifier id) {
            SqlOperatorTable opTab = SqlStdOperatorTable.instance();
            return Objects.nonNull(SqlUtil.makeCall(opTab, id));
        }

    }

    //End DoubleQuoteSqlIdentifierConvert.class
    @Override
    public String convert(String originSql, String project, String defaultSchema) {

        return convertDoubleQuote(originSql);
    }

    public static String convertDoubleQuote(String originSql) {
        String sqlParsed = originSql;

        try {
            DoubleQuoteSqlIdentifierConvert sqlIdentifierConvert = new DoubleQuoteSqlIdentifierConvert(originSql);
            sqlParsed = sqlIdentifierConvert.convert();
        } catch (Exception e) {
            log.warn("convert sql:{} with double quoted with exception", originSql, e);
        }
        return sqlParsed;
    }
}
