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

package io.kyligence.kap.tool.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.UUID;

import io.kyligence.kap.common.persistence.JDBCSqlQueryFormat;
import io.kyligence.kap.common.persistence.JDBCSqlQueryFormatProvider;
import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.JDBCConnectionManager;

public class KapTestJdbcCLI {

    protected static final Logger logger = LoggerFactory.getLogger(KapTestJdbcCLI.class);
    private String tableName;
    private JDBCConnectionManager connectionManager = null;
    private JDBCSqlQueryFormat sqlQueryFormat;

    public KapTestJdbcCLI() {
        this.tableName = UUID.randomUUID().toString().replace('-', '_');
        this.sqlQueryFormat = JDBCSqlQueryFormatProvider.createJDBCSqlQueriesFormat(KapConfig.getInstanceFromEnv().getMetadataDialect());
    }

    public void testConnection() {
        logger.info("Testing JDBC connection...");
        try {
            connectionManager = JDBCConnectionManager.getConnectionManager();
        } catch (RuntimeException e) {
            logger.error(e.toString());
            System.out.println("Fail to connect to database, please make sure jdbc connector jar exists in $KYLIN_HOME/ext, and kylin.metadata.url is correctly configured.");
            System.exit(1);
        }
    }

    public void testCreateTable() {
        logger.info("Test JDBC create table...");
        String sql = MessageFormat.format(sqlQueryFormat.getTestCreateSql(), tableName);
        try {
            execute(sql);
        } catch (RuntimeException e) {
            logger.error(e.toString());
            System.out.println("Fail to create table in database, more info please check full log");
            System.exit(1);
        }
    }

    public void cleanUp() {
        logger.info("Clean up...");
        String sql = MessageFormat.format(sqlQueryFormat.getTestDropSql(), tableName);
        try {
            execute(sql);
        } catch (RuntimeException e) {
            logger.error(e.toString());
            System.out.println("Fail to drop the table: " + tableName + ", more info please check full log");
            System.exit(1);
        }
    }

    private void execute(String sql) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = connectionManager.getConn();
            statement = conn.prepareStatement(sql);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JDBCConnectionManager.closeQuietly(statement);
            JDBCConnectionManager.closeQuietly(conn);
        }
    }

    public static void main(String[] args) {

        if (args.length != 0) {
            System.out.println("Usage: KapTestJdbcCLI");
            System.exit(1);
        }
        KapTestJdbcCLI kapTestJdbcCLI = new KapTestJdbcCLI();
        kapTestJdbcCLI.testConnection();
        kapTestJdbcCLI.testCreateTable();
        kapTestJdbcCLI.cleanUp();
    }

}
