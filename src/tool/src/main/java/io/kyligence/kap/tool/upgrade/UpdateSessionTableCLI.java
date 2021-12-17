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
package io.kyligence.kap.tool.upgrade;

import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnGreen;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import javax.sql.DataSource;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.tool.util.MetadataUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class UpdateSessionTableCLI extends ExecutableApplication {

    private static final Option OPTION_TRUNCATE = OptionBuilder.getInstance().hasArg(false).withArgName("truncate")
            .withDescription("Truncate the session table.").isRequired(false).withLongOpt("truncate").create("t");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    private static final int SESSION_ID_LENGTH = 180;

    private static final String UPDATE_MYSQL_SESSION_TABLE_SQL = String.format(Locale.ROOT,
            "ALTER TABLE SPRING_SESSION MODIFY COLUMN SESSION_ID VARCHAR(%d) NOT NULL;", SESSION_ID_LENGTH);
    private static final String UPDATE_MYSQL_SESSION_ATTRIBUTES_TABLE_SQL = String.format(Locale.ROOT,
            "ALTER TABLE SPRING_SESSION_ATTRIBUTES MODIFY COLUMN SESSION_ID VARCHAR(%d) NOT NULL;", SESSION_ID_LENGTH);

    private static final String UPDATE_PG_SESSION_TABLE_SQL = String.format(Locale.ROOT,
            "ALTER TABLE SPRING_SESSION ALTER COLUMN SESSION_ID TYPE VARCHAR(%d) , ALTER COLUMN SESSION_ID SET NOT NULL;",
            SESSION_ID_LENGTH);
    private static final String UPDATE_PG_SESSION_ATTRIBUTES_TABLE_SQL = String.format(Locale.ROOT,
            "ALTER TABLE SPRING_SESSION_ATTRIBUTES ALTER COLUMN SESSION_ID TYPE VARCHAR(%d) , ALTER COLUMN SESSION_ID SET NOT NULL;",
            SESSION_ID_LENGTH);

    private static final String ERROR_MSG_FORMAT = "Failed to alter session table schema : %s , "
            + "please alter session table schema manually according to user manual. "
            + "Otherwise you may not be able to log in Detailed Message is at logs/shell.stderr";

    private DataSource dataSource;

    public static void main(String[] args) {
        UpdateSessionTableCLI updateSessionTableCLI = new UpdateSessionTableCLI();
        try {
            updateSessionTableCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec UpdateSessionTableCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade session table finished.");
        systemExitWhenMainThread(0);
    }

    @VisibleForTesting
    public int affectedRowsWhenTruncate(String sessionTableName) throws SQLException {
        if (!JdbcUtil.isTableExists(dataSource.getConnection(), sessionTableName)) {
            log.info("Table {} is not exist, affected rows is zero.", sessionTableName);
            return 0;
        }
        try (PreparedStatement preparedStatementQuery = dataSource.getConnection()
                .prepareStatement("SELECT COUNT(1) FROM " + sessionTableName);
                ResultSet rs = preparedStatementQuery.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            log.error("Failed to count table: {}", sessionTableName, e);
            throw e;
        }
    }

    @VisibleForTesting
    public void truncateSessionTable(String sessionTableName) throws SQLException {
        if (!JdbcUtil.isTableExists(dataSource.getConnection(), sessionTableName)) {
            log.info("Table {} is not exist, skip truncate.", sessionTableName);
            return;
        }
        try (PreparedStatement preparedStatement = dataSource.getConnection()
                .prepareStatement("DELETE FROM " + sessionTableName + " WHERE SESSION_ID IS NOT NULL")) {

            int rows = preparedStatement.executeUpdate();
            log.info("Delete {} rows from {} .", rows, sessionTableName);
        } catch (Exception e) {
            log.error("Failed to truncate table: {}", sessionTableName, e);
            throw e;
        }
    }

    @VisibleForTesting
    public boolean isSessionTableNeedUpgrade(String sessionTableName) throws SQLException {
        if (!JdbcUtil.isTableExists(dataSource.getConnection(), sessionTableName)) {
            log.info("Table {} is not exist, no need to upgrade.", sessionTableName);
            return false;
        }
        try (PreparedStatement preparedStatement = dataSource.getConnection()
                .prepareStatement("SELECT SESSION_ID FROM " + sessionTableName + " LIMIT 1")) {
            int columnLength = preparedStatement.getMetaData().getPrecision(1);
            if (columnLength < SESSION_ID_LENGTH) {
                log.info("Table: {}, Alter SESSION_ID column length: {} to length: {}", sessionTableName, columnLength,
                        SESSION_ID_LENGTH);
                return true;
            }

            log.info("Table: {} is matched, skip upgrade.", sessionTableName);
        } catch (Exception e) {
            log.error("Failed to check SESSION_ID from table: {}", sessionTableName, e);
            systemExitWhenMainThread(1);
        }

        return false;
    }

    private void tryUpdateSessionTable(String replaceName, String sql, String sessionTableName) throws SQLException {
        if (!isSessionTableNeedUpgrade(sessionTableName)) {
            return;
        }

        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            String sessionScript = sql.replaceAll("SPRING_SESSION", replaceName);
            populator.addScript(new InMemoryResource(sessionScript));
            populator.setContinueOnError(false);
            DatabasePopulatorUtils.execute(populator, dataSource);
            log.info("session table {} upgrade succeeded.", sessionTableName);
        } catch (Exception e) {
            log.error("try update session table failed", e);
            throw e;
        }
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_TRUNCATE);
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        if (!StringUtils.equalsIgnoreCase(systemKylinConfig.getSpringStoreType(), "JDBC")) {
            printlnGreen("skip upgrade session and session_ATTRIBUTES table.");
            return;
        }
        String tableName = systemKylinConfig.getMetadataUrlPrefix() + "_session";
        String tableAttributesName = tableName + "_ATTRIBUTES";

        dataSource = MetadataUtil.getDataSource(systemKylinConfig);

        printlnGreen(String.format(Locale.ROOT, "found %d rows need to be modified.",
                affectedRowsWhenTruncate(tableAttributesName) + affectedRowsWhenTruncate(tableName)));

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            if (optionsHelper.hasOption(OPTION_TRUNCATE)) {
                truncateSessionTable(tableAttributesName);
                truncateSessionTable(tableName);
            }

            printlnGreen("start to check the permission to update tables.");
            if (dataSource instanceof org.apache.commons.dbcp2.BasicDataSource
                    && ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getDriverClassName()
                            .equals("com.mysql.jdbc.Driver")) {
                tryUpdateSessionTable(tableName, UPDATE_MYSQL_SESSION_ATTRIBUTES_TABLE_SQL, tableAttributesName);
                tryUpdateSessionTable(tableName, UPDATE_MYSQL_SESSION_TABLE_SQL, tableName);
            } else {
                tryUpdateSessionTable(tableName, UPDATE_PG_SESSION_ATTRIBUTES_TABLE_SQL, tableAttributesName);
                tryUpdateSessionTable(tableName, UPDATE_PG_SESSION_TABLE_SQL, tableName);
            }

            printlnGreen("session and session_ATTRIBUTES table upgrade succeeded.");
        }
    }
}
