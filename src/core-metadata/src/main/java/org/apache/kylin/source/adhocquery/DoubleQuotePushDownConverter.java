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

import javax.annotation.Nonnull;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;

/**
 * <pre>
 * for example:
 *
 *     select ACCOUNT_ID as id, ACCOUNT_COUNTRY as "country" from "DEFAULT".TEST_ACCOUNT
 *
 * will be converted to:
 *
 *      SELECT "ACCOUNT_ID" AS "ID", "ACCOUNT_COUNTRY" AS "country"
 * FROM "DEFAULT"."TEST_ACCOUNT"
 * </pre>
 *
 * <P>convert sql quote with
 * {@link org.apache.calcite.avatica.util.Quoting#DOUBLE_QUOTE}
 * </P>and SqlDialect with
 * {@link org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter.DoubleQuoteSqlDialect#DEFAULT}
 */
public class DoubleQuotePushDownConverter implements IPushDownConverter, IKeep {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoubleQuotePushDownConverter.class);

    //inner static class for DoubleQuotePushDownConverter
    private static class DoubleQuoteSqlDialect extends CalciteSqlDialect {
        public static final SqlDialect DEFAULT = new DoubleQuotePushDownConverter.DoubleQuoteSqlDialect(emptyContext()
                .withDatabaseProduct(DatabaseProduct.CALCITE).withIdentifierQuoteString(Quoting.DOUBLE_QUOTE.string));

        /**
         * Creates a DoubleQuoteSqlDialect.
         */
        public DoubleQuoteSqlDialect(SqlDialect.Context context) {
            super(context);
        }

        @Override
        public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
            unparseFetchUsingLimit(writer, offset, fetch);
        }

    }
    //End DoubleQuoteSqlDialect.class

    @Override
    public String convert(String originSql, String project, String defaultSchema, boolean isPrepare) {

        return convertDoubleQuote(originSql);
    }

    public static String convertDoubleQuote(@Nonnull String originSql) {
        String sqlParsed = originSql;

        try {
            sqlParsed = CalciteParser.formatSqlStringWithDialect(originSql,
                    DoubleQuotePushDownConverter.DoubleQuoteSqlDialect.DEFAULT);
        } catch (SqlParseException e) {
            LOGGER.warn("convert sql:{} with double quoted with exception {} ", originSql, e);
        }
        return sqlParsed;
    }
}
