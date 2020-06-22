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

import static io.kyligence.kap.tool.garbage.StorageCleaner.ANSI_GREEN;
import static io.kyligence.kap.tool.garbage.StorageCleaner.ANSI_RED;
import static io.kyligence.kap.tool.garbage.StorageCleaner.ANSI_RESET;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateSessionTableCLI {

    private final static String UPDATE_MYSQL_SESSION_TABLE_SQL = "ALTER TABLE SPRING_SESSION MODIFY COLUMN SESSION_ID VARCHAR(180) NOT NULL;";
    private final static String UPDATE_MYSQL_SESSION_ATTRIBUTES_TABLE_SQL = "ALTER TABLE SPRING_SESSION_ATTRIBUTES MODIFY COLUMN SESSION_ID VARCHAR(180) NOT NULL;";

    private final static String UPDATE_PG_SESSION_TABLE_SQL = "ALTER TABLE SPRING_SESSION ALTER COLUMN SESSION_ID TYPE VARCHAR(180) , ALTER COLUMN SESSION_ID SET NOT NULL;";
    private final static String UPDATE_PG_SESSION_ATTRIBUTES_TABLE_SQL = "ALTER TABLE SPRING_SESSION_ATTRIBUTES ALTER COLUMN SESSION_ID TYPE VARCHAR(180) , ALTER COLUMN SESSION_ID SET NOT NULL;";

    private static DataSource dataSource;

    private static void initDataSource() {
        val config = KylinConfig.getInstanceFromEnv();
        val url = config.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        try {
            dataSource = JdbcDataSource.getDataSource(props);
        } catch (Exception e) {
            log.error("init data source failed", e);
            dataSource = null;
        }
    }

    public static void main(String[] args) {
        initDataSource();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String tableName = config.getMetadataUrlPrefix() + "_session";
        String tableAttributesName = tableName + "_ATTRIBUTES";
        if (dataSource instanceof org.apache.commons.dbcp2.BasicDataSource
                && ((org.apache.commons.dbcp2.BasicDataSource) dataSource).getDriverClassName()
                        .equals("com.mysql.jdbc.Driver")) {
            tryUpdateSessionTable(tableName, UPDATE_MYSQL_SESSION_ATTRIBUTES_TABLE_SQL, tableAttributesName);
            tryUpdateSessionTable(tableName, UPDATE_MYSQL_SESSION_TABLE_SQL, tableName);
        } else {
            tryUpdateSessionTable(tableName, UPDATE_PG_SESSION_ATTRIBUTES_TABLE_SQL, tableAttributesName);
            tryUpdateSessionTable(tableName, UPDATE_PG_SESSION_TABLE_SQL, tableName);
        }
        System.out.println("Update session table finished.");
        System.exit(0);
    }

    private static void tryUpdateSessionTable(String replaceName, String sql, String sessionTableName) {
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            String sessionScript = sql.replaceAll("SPRING_SESSION", replaceName);
            populator.addScript(new InMemoryResource(sessionScript));
            populator.setContinueOnError(false);
            DatabasePopulatorUtils.execute(populator, dataSource);
            System.out.println(ANSI_GREEN + "Succeed to alter session table schema : " + sessionTableName + ANSI_RESET);
        } catch (Exception e) {
            log.error("try update session table failed", e);
            System.out.println(ANSI_RED + "Failed to alter session table schema : " + sessionTableName
                    + " ,  please alter session table schema manually according to user manaul. Otherwise you may not be able to log in"
                    + "Detailed Message is at logs/shell.stderr" + ANSI_RESET);
        }
    }

}
