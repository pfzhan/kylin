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

import org.apache.calcite.sql.SqlDialect;
import org.apache.kylin.sdk.datasource.adaptor.AbstractJdbcAdaptor;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;

import com.google.common.collect.Maps;

public class DefaultConfigurer implements SqlConverter.IConfigurer{

    private static final Map<String, SqlDialect> sqlDialectMap = Maps.newHashMap();

    static {
        sqlDialectMap.put("default", SqlDialect.CALCITE);
        sqlDialectMap.put("calcite", SqlDialect.CALCITE);
        sqlDialectMap.put("greenplum", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("postgresql", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("mysql", SqlDialect.DatabaseProduct.MYSQL.getDialect());
        sqlDialectMap.put("mssql", SqlDialect.DatabaseProduct.MSSQL.getDialect());
        sqlDialectMap.put("oracle", SqlDialect.DatabaseProduct.ORACLE.getDialect());
        sqlDialectMap.put("vertica", SqlDialect.DatabaseProduct.VERTICA.getDialect());
        sqlDialectMap.put("redshift", SqlDialect.DatabaseProduct.REDSHIFT.getDialect());
        sqlDialectMap.put("hive", SqlDialect.DatabaseProduct.HIVE.getDialect());
        sqlDialectMap.put("h2", SqlDialect.DatabaseProduct.H2.getDialect());
        sqlDialectMap.put("unkown", SqlDialect.DUMMY);
    }

    private AbstractJdbcAdaptor adaptor;

    private DataSourceDef dsDef;

    public DefaultConfigurer(AbstractJdbcAdaptor adaptor, DataSourceDef dsDef) {
        this.adaptor = adaptor;
        this.dsDef = dsDef;
    }

    public DefaultConfigurer(DataSourceDef dsDef) {
        this(null, dsDef);
    }

    @Override
    public boolean skipDefaultConvert() {
        return !"true".equalsIgnoreCase(dsDef.getPropertyValue("sql.default-converted-enabled", "true"));
    }

    @Override
    public boolean skipHandleDefault() {
        return !"true".equalsIgnoreCase(dsDef.getPropertyValue("sql.keyword-default-escape", "true"));
    }

    @Override
    public boolean useUppercaseDefault() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.keyword-default-uppercase", "true"));
    }

    @Override
    public String fixAfterDefaultConvert(String orig) {
        if (this.adaptor == null) {
            return orig;
        }
        return adaptor.fixSql(orig);
    }

    @Override
    public SqlDialect getSqlDialect() {
        String dialectName = dsDef.getDialectName() == null ? dsDef.getId() : dsDef.getDialectName();
        SqlDialect sqlDialect = sqlDialectMap.get(dialectName.toLowerCase());
        return sqlDialect == null ? sqlDialectMap.get("unkown") : sqlDialect;
    }

    @Override
    public boolean allowNoOffset() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-no-offset", "true"));
    }

    @Override
    public boolean allowFetchNoRows() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-fetch-no-rows", "true"));
    }

    @Override
    public boolean allowNoOrderByWithFetch() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-no-orderby-with-fetch", "true"));
    }

    @Override
    public String getPagingType() {
        return dsDef.getPropertyValue("sql.paging-type", "AUTO");
    }

    @Override
    public boolean isCaseSensitive() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.case-sensitive", "false"));
    }

    @Override
    public boolean enableCache() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("metadata.enable-cache", "false"));
    }

    @Override
    public boolean enableQuote() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.enable-quote-all-identifiers", "false"));
    }

    @Override
    public String fixIdentifierCaseSensitive(String orig) {
        if (this.adaptor == null || !isCaseSensitive()) {
            return orig;
        }
        return adaptor.fixIdentifierCaseSensitive(orig);
    }

    @Override
    public boolean enableTransformDateToString() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.sqoop.enable-transform-date-to-string", "false"));
    }

    @Override
    public String getTransformDateToStringExpression() {
        return dsDef.getPropertyValue("sql.sqoop.transform-date-to-string-expression", "");
    }

    @Override
    public String getTransformDatePattern() {
        return dsDef.getPropertyValue("sql.sqoop.transform-date-pattern", "");
    }
}
