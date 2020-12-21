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
package io.kyligence.kap.source.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.IReadableTable;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcTableReader implements IReadableTable.TableReader {
    private static final String DATABASE_AND_TABLE = "%s.%s";
    private String dbName;
    private String tableName;

    private Connection conn;
    private Statement statement;
    private ResultSet rs;
    private int colCount;

    /**
     * Constructor for reading whole jdbc table
     *
     * @param dataSource
     * @param tableDesc
     * @throws IOException
     */
    public JdbcTableReader(JdbcConnector dataSource, TableDesc tableDesc) throws IOException {
        this.dbName = tableDesc.getDatabase();
        this.tableName = tableDesc.getName();

        String sql = dataSource.convertSql(generateSelectDataStatement(tableDesc));
        try {
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(sql);
            colCount = rs.getMetaData().getColumnCount();
        } catch (SQLException e) {
            log.error("error when get jdbc tableReader.", e);
            throw new IOException(String.format(Locale.ROOT, "error while exec %s", sql));
        }
    }

    @Override
    public boolean next() throws IOException {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String[] getRow() {
        String[] ret = new String[colCount];
        for (int i = 1; i <= colCount; i++) {
            try {
                Object o = rs.getObject(i);
                String result;
                if (null == o || o instanceof byte[]) {
                    result = null;
                } else {
                    result = o.toString();
                }
                ret[i - 1] = result;
            } catch (Exception e) {
                log.error("Failed to get row", e);
            }
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        DBUtils.closeQuietly(rs);
        DBUtils.closeQuietly(statement);
        DBUtils.closeQuietly(conn);
    }

    public String toString() {
        return "jdbc table reader for: " + dbName + "." + tableName;
    }

    private String generateSelectDataStatement(TableDesc tableDesc) {
        ColumnDesc columnDesc;
        List<String> columns = Lists.newArrayList();
        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            columnDesc = tableDesc.getColumns()[i];
            columns.add(String.format(Locale.ROOT, DATABASE_AND_TABLE, tableDesc.getName(), columnDesc.getName()));
        }
        final String sep = " ";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT").append(sep).append(StringUtils.join(columns, ",")).append(sep).append("FROM").append(sep)
                .append(String.format(Locale.ROOT, DATABASE_AND_TABLE, tableDesc.getDatabase(), tableDesc.getName()));
        return sql.toString();
    }
}
