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

package io.kyligence.kap.metadata.query;

import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.common.obf.IKeep;

public class QueryHistoryTable extends SqlTable implements IKeep {

    public final SqlColumn<String> sql = column("sql_text", JDBCType.VARCHAR);
    public final SqlColumn<String> sqlPattern = column("sql_pattern", JDBCType.VARCHAR);
    public final SqlColumn<Long> queryTime = column("query_time", JDBCType.BIGINT);
    public final SqlColumn<String> month = column("month", JDBCType.VARCHAR);
    public final SqlColumn<Long> queryFirstDayOfMonth = column("query_first_day_of_month", JDBCType.BIGINT);
    public final SqlColumn<Long> queryFirstDayOfWeek = column("query_first_day_of_week", JDBCType.BIGINT);
    public final SqlColumn<Long> queryDay = column("query_day", JDBCType.BIGINT);
    public final SqlColumn<Long> duration = column("duration", JDBCType.BIGINT);
    public final SqlColumn<String> queryRealizations = column("realizations", JDBCType.VARCHAR);
    public final SqlColumn<String> hostName = column("server", JDBCType.VARCHAR);
    public final SqlColumn<String> querySubmitter = column("submitter", JDBCType.VARCHAR);
    public final SqlColumn<String> queryStatus = column("query_status", JDBCType.VARCHAR);
    public final SqlColumn<String> queryId = column("query_id", JDBCType.VARCHAR);
    public final SqlColumn<Long> id = column("id", JDBCType.BIGINT);
    public final SqlColumn<Long> totalScanCount = column("total_scan_count", JDBCType.BIGINT);
    public final SqlColumn<Long> totalScanBytes = column("total_scan_bytes", JDBCType.BIGINT);
    public final SqlColumn<Long> resultRowCount = column("result_row_count", JDBCType.BIGINT);
    public final SqlColumn<Boolean> cacheHit = column("cache_hit", JDBCType.BOOLEAN);
    public final SqlColumn<Boolean> indexHit = column("index_hit", JDBCType.BOOLEAN);
    public final SqlColumn<String> engineType = column("engine_type", JDBCType.VARCHAR);
    public final SqlColumn<String> projectName = column("project_name", JDBCType.BIGINT);
    public final SqlColumn<String> errorType = column("error_type", JDBCType.VARCHAR);
    public final SqlColumn<QueryHistoryInfo> queryHistoryInfo = column("reserved_field_3", JDBCType.BLOB,
            QueryHistoryInfoHandler.class.getName());

    public QueryHistoryTable(String tableName) {
        super(tableName);
    }

    public static class QueryHistoryInfoHandler implements TypeHandler<QueryHistoryInfo>, IKeep {

        private static final Logger logger = LoggerFactory.getLogger(QueryHistoryInfoHandler.class);

        @Override
        public void setParameter(PreparedStatement ps, int i, QueryHistoryInfo parameter, JdbcType jdbcType)
                throws SQLException {
            byte[] bytes = "".getBytes();
            try {
                bytes = JsonUtil.writeValueAsBytes(parameter);
            } catch (JsonProcessingException e) {
                logger.error("Fail transform object to json", e);
            }
            ps.setBytes(i, bytes);
        }

        @Override
        public QueryHistoryInfo getResult(ResultSet rs, String columnName) throws SQLException {
            if (rs.getBytes(columnName) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(rs.getBytes(columnName), QueryHistoryInfo.class);
        }

        @Override
        public QueryHistoryInfo getResult(ResultSet rs, int columnIndex) throws SQLException {
            if (rs.getBytes(columnIndex) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(rs.getBytes(columnIndex), QueryHistoryInfo.class);
        }

        @Override
        public QueryHistoryInfo getResult(CallableStatement cs, int columnIndex) throws SQLException {
            if (cs.getBytes(columnIndex) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(cs.getBytes(columnIndex), QueryHistoryInfo.class);
        }
    }

    public static class InstantHandler implements TypeHandler<Instant>, IKeep {

        @Override
        public void setParameter(PreparedStatement ps, int i, Instant parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setLong(i, parameter.toEpochMilli());
        }

        @Override
        public Instant getResult(ResultSet rs, String columnName) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnName)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(ResultSet rs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(CallableStatement cs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(cs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }
    }

}
