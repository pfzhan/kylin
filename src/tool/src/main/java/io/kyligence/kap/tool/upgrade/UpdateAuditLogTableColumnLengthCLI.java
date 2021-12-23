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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.util.Unsafe;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateAuditLogTableColumnLengthCLI {
    private static final String SHOW_TABLE = "SHOW TABLES LIKE '%s'";
    private static final String UPDATE_COL_TO_TABLE_SQL = "alter table %s modify column %s %s";
    private static final String TABLE_SUFFIX = "_audit_log";
    private static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    private static final int COLUMN_LENGTH = 200;

    public static void main(String[] args) throws Exception {
        log.info("Start to modify column length log...");
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            val url = kylinConfig.getMetadataUrl();
            val props = datasourceParameters(url);
            val dataSource = JdbcDataSource.getDataSource(props);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.setQueryTimeout(-1);
            String tableName = url.getIdentifier() + TABLE_SUFFIX;
            if (tableIsExist(jdbcTemplate, tableName)) {
                int length = COLUMN_LENGTH;
                String sql = String.format(Locale.ROOT, "SELECT %s FROM %s LIMIT 1", AUDIT_LOG_TABLE_OPERATOR,
                        tableName);
                try (PreparedStatement preparedStatement = dataSource.getConnection().prepareStatement(sql)) {
                    ResultSetMetaData metaData = preparedStatement.getMetaData();
                    length = metaData.getPrecision(1);
                }
                if (length < COLUMN_LENGTH) {
                    modifyColumnLength(jdbcTemplate, tableName, AUDIT_LOG_TABLE_OPERATOR,
                            String.format(Locale.ROOT, "varchar(%s)", COLUMN_LENGTH));
                }
            } else {
                log.info("table {} not exist.", tableName);
            }
        } catch (Exception e) {
            log.error("modify column length error", e);
        }
        Unsafe.systemExit(0);
    }

    public static boolean tableIsExist(JdbcTemplate jdbcTemplate, String tableName) {
        try {
            String object = jdbcTemplate.queryForObject(String.format(Locale.ROOT, SHOW_TABLE, tableName),
                    (resultSet, i) -> resultSet.getString(1));
            return Objects.equals(tableName, object);
        } catch (EmptyResultDataAccessException emptyResultDataAccessException) {
            log.error("not found table", emptyResultDataAccessException);
            return false;
        }
    }

    public static void modifyColumnLength(JdbcTemplate jdbcTemplate, String tableName, String column, String type) {
        String sql = String.format(Locale.ROOT, UPDATE_COL_TO_TABLE_SQL, tableName, column, type);
        try {
            jdbcTemplate.execute(sql);
            log.info("update column length finished!");
        } catch (Exception e) {
            log.error("Failed to execute upgradeSql: {}", sql, e);
        }
    }
}
