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

package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public class MssqlAdaptor extends DefaultAdaptor {

    private static final Pattern ASYM = Pattern.compile("BETWEEN(\\s*)ASYMMETRIC");
    private static final Pattern SYM = Pattern.compile("BETWEEN(\\s*)SYMMETRIC");

    public MssqlAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    /**
     * Simple Implementation:
     * <p>
     * LIMIT X OFFSET Y is not supported in MSSQL, and will convert to OFFSET Y FETCH NEXT X ROWS ONLY by framework.
     *
     * in MSSQL, The OFFSET and FETCH clauses are options of the ORDER BY clause.
     * 1. this requires a ORDER BY clause, will add this ORDER BY clause if missing here.
     * 2. When there is a FETCH clause, the OFFSET clause is required, will add OFFSET clause if missing here.
     *
     * example:
     * * * * * * * * * * * * * * * * * * * * * * *
     *                                           *
     *    ORDER BY column_list                   *
     *    OFFSET offset_row_count ROWS           *
     *    FETCH NEXT fetch_row_count ROWS ONLY   *
     *                                           *
     * * * * * * * * * * * * * * * * * * * * * * *
     *
     *
     * @param sql The SQL statement to be fixed.
     * @return
     */
    @Override
    public String fixSql(String sql) {
        sql = sql.replaceAll(" DOUBLE", " FLOAT");

        boolean hasOrderBy = sql.toLowerCase(Locale.ROOT).contains("order by ");
        boolean hasOffset = sql.toLowerCase(Locale.ROOT).contains("offset ");

        if (!hasOrderBy) {
            int idx = sql.indexOf("OFFSET ");
            if (idx >= 0)
                sql = sql.substring(0, idx) + " ORDER BY 1 " + sql.substring(idx);
        } else if (!hasOffset) {
            // add offset when exist order by and fetch, not exist offset
            int idx = sql.toLowerCase(Locale.ROOT).indexOf("fetch ");
            if (idx >= 0) {
                sql = sql.substring(0, idx) + "OFFSET 0 ROWS \n" + sql.substring(idx);
            }
        }
        // repalce ceil() -> ceiling()
        sql = sql.replaceAll("CEIL\\(", "CEILING\\(");
        sql = resolveBetweenAsymmetricSymmetric(sql);
        return sql;
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection()) {
            String database = con.getCatalog();
            Preconditions.checkArgument(StringUtils.isNotEmpty(database),
                    "SQL Server needs a specific database in " + "connection string.");

            try (ResultSet rs = con.getMetaData().getSchemas(database, "%")) {
                String schema;
                String catalog;
                while (rs.next()) {
                    schema = rs.getString("TABLE_SCHEM");
                    catalog = rs.getString("TABLE_CATALOG");
                    // Skip system schemas
                    if (database.equals(catalog) || schema.equals("dbo")) {
                        ret.add(schema);
                    }
                }
            }
        }
        return ret;
    }

    @Override
    public String toSourceTypeName(String kylinTypeName) {
        String lower = kylinTypeName.toLowerCase(Locale.ROOT);
        switch (lower) {
        case "double":
            return "float";
        default:
            return lower;
        }
    }

    /**
     * remove [Asymmetric|Symmetric]  after between
     *
     * @param sql sql to be modified
     * @return sql modified
     */
    private String resolveBetweenAsymmetricSymmetric(String sql) {
        String sqlReturn = sql;

        Matcher matcher = ASYM.matcher(sql);
        if (matcher.find()) {
            sqlReturn = sql.replace(matcher.group(), "BETWEEN");
        }

        matcher = SYM.matcher(sql);
        if (matcher.find()) {
            sqlReturn = sqlReturn.replace(matcher.group(), "BETWEEN");
        }

        return sqlReturn;
    }
}
