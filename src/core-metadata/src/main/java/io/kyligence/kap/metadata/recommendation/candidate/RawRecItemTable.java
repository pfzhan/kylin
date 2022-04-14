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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.util.JsonUtil;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.recommendation.entity.RecItemV2;

public class RawRecItemTable extends SqlTable {

    public final SqlColumn<Integer> id = column("id", JDBCType.INTEGER);
    public final SqlColumn<String> project = column("project", JDBCType.VARCHAR);
    public final SqlColumn<String> modelID = column("model_id", JDBCType.VARCHAR);
    public final SqlColumn<String> uniqueFlag = column("unique_flag", JDBCType.VARCHAR);
    public final SqlColumn<Integer> semanticVersion = column("semantic_version", JDBCType.INTEGER);
    public final SqlColumn<RawRecItem.RawRecType> type = column("type", JDBCType.TINYINT,
            RecTypeHandler.class.getName());
    public final SqlColumn<RecItemV2> recEntity = column("rec_entity", JDBCType.VARCHAR,
            RecItemV2Handler.class.getName());
    public final SqlColumn<int[]> dependIDs = column("depend_ids", JDBCType.VARCHAR, DependIdHandler.class.getName());
    public final SqlColumn<LayoutMetric> layoutMetric = column("layout_metric", JDBCType.VARCHAR,
            LayoutMetricHandler.class.getName());
    public final SqlColumn<Double> cost = column("cost", JDBCType.DOUBLE);
    public final SqlColumn<Double> totalLatencyOfLastDay = column("total_latency_of_last_day", JDBCType.DOUBLE);
    public final SqlColumn<Integer> hitCount = column("hit_count", JDBCType.INTEGER);
    public final SqlColumn<Double> totalTime = column("total_time", JDBCType.DOUBLE);
    public final SqlColumn<Double> maxTime = column("max_time", JDBCType.DOUBLE);
    public final SqlColumn<Double> minTime = column("min_time", JDBCType.DOUBLE);
    public final SqlColumn<String> queryHistoryInfo = column("query_history_info", JDBCType.VARCHAR);
    public final SqlColumn<RawRecItem.RawRecState> state = column("state", JDBCType.TINYINT,
            RecStateHandler.class.getName());
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);
    public final SqlColumn<Long> updateTime = column("update_time", JDBCType.BIGINT);
    public final SqlColumn<String> recSource = column("reserved_field_1", JDBCType.VARCHAR);

    public RawRecItemTable(String tableName) {
        super(tableName);
    }

    public static class RecTypeHandler implements TypeHandler<RawRecItem.RawRecType> {

        @Override
        public void setParameter(PreparedStatement ps, int i, RawRecItem.RawRecType parameter, JdbcType jdbcType)
                throws SQLException {
            Preconditions.checkArgument(parameter != null, "recommendation type cannot be null");
            ps.setByte(i, (byte) parameter.id());
        }

        @Override
        public RawRecItem.RawRecType getResult(ResultSet rs, String columnName) throws SQLException {
            return RawRecItem.toRecType(rs.getByte(columnName));
        }

        @Override
        public RawRecItem.RawRecType getResult(ResultSet rs, int columnIndex) throws SQLException {
            return RawRecItem.toRecType(rs.getByte(columnIndex));
        }

        @Override
        public RawRecItem.RawRecType getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return RawRecItem.toRecType(cs.getByte(columnIndex));
        }
    }

    public static class RecStateHandler implements TypeHandler<RawRecItem.RawRecState> {

        @Override
        public void setParameter(PreparedStatement ps, int i, RawRecItem.RawRecState parameter, JdbcType jdbcType)
                throws SQLException {
            Preconditions.checkArgument(parameter != null, "recommendation state cannot be null");
            ps.setByte(i, (byte) parameter.id());
        }

        @Override
        public RawRecItem.RawRecState getResult(ResultSet rs, String columnName) throws SQLException {
            return RawRecItem.toRecState(rs.getByte(columnName));
        }

        @Override
        public RawRecItem.RawRecState getResult(ResultSet rs, int columnIndex) throws SQLException {
            return RawRecItem.toRecState(rs.getByte(columnIndex));
        }

        @Override
        public RawRecItem.RawRecState getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return RawRecItem.toRecState(cs.getByte(columnIndex));
        }
    }

    public static class RecItemV2Handler implements TypeHandler<RecItemV2> {

        private static final String REC_TYPE = "type";

        @Override
        public void setParameter(PreparedStatement ps, int i, RecItemV2 parameter, JdbcType jdbcType)
                throws SQLException {
            Preconditions.checkArgument(parameter != null, "recommendation entity cannot be null");
            try {
                ps.setString(i, JsonUtil.writeValueAsString(parameter));
            } catch (JsonProcessingException e) {
                throw new SerializationException("cannot serialize recEntity", e);
            }
        }

        @Override
        public RecItemV2 getResult(ResultSet rs, String columnName) throws SQLException {
            return RawRecItem.toRecItem(rs.getString(columnName), rs.getByte(RecItemV2Handler.REC_TYPE));
        }

        @Override
        public RecItemV2 getResult(ResultSet rs, int columnIndex) throws SQLException {
            return RawRecItem.toRecItem(rs.getString(columnIndex), rs.getByte(REC_TYPE));
        }

        @Override
        public RecItemV2 getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return RawRecItem.toRecItem(cs.getString(columnIndex), cs.getByte(REC_TYPE));
        }
    }

    public static class DependIdHandler implements TypeHandler<int[]> {

        @Override
        public void setParameter(PreparedStatement ps, int i, int[] parameter, JdbcType jdbcType) throws SQLException {
            Preconditions.checkArgument(parameter != null, "dependIDs cannot be null");
            try {
                ps.setString(i, JsonUtil.writeValueAsString(parameter));
            } catch (JsonProcessingException e) {
                throw new SerializationException("cannot serialize dependIDs", e);
            }
        }

        @Override
        public int[] getResult(ResultSet rs, String columnName) throws SQLException {
            return RawRecItem.toDependIds(rs.getString(columnName));
        }

        @Override
        public int[] getResult(ResultSet rs, int columnIndex) throws SQLException {
            return RawRecItem.toDependIds(rs.getString(columnIndex));
        }

        @Override
        public int[] getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return RawRecItem.toDependIds(cs.getString(columnIndex));
        }
    }

    public static class LayoutMetricHandler implements TypeHandler<LayoutMetric> {

        @Override
        public void setParameter(PreparedStatement ps, int i, LayoutMetric parameter, JdbcType jdbcType)
                throws SQLException {
            try {
                ps.setString(i, parameter == null ? null : JsonUtil.writeValueAsString(parameter));
            } catch (JsonProcessingException e) {
                throw new SerializationException("cannot serialize layoutMetric", e);
            }
        }

        @Override
        public LayoutMetric getResult(ResultSet rs, String columnName) throws SQLException {
            return toLayoutMetric(rs.getString(columnName));
        }

        @Override
        public LayoutMetric getResult(ResultSet rs, int columnIndex) throws SQLException {
            return toLayoutMetric(rs.getString(columnIndex));
        }

        @Override
        public LayoutMetric getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return toLayoutMetric(cs.getString(columnIndex));
        }

        public LayoutMetric toLayoutMetric(String jsonString) {
            if (StringUtils.isEmpty(jsonString)) {
                return null;
            }
            try {
                return JsonUtil.readValue(jsonString, LayoutMetric.class);
            } catch (IOException e) {
                throw new IllegalStateException("cannot deserialize layout metric correctly", e);
            }
        }
    }
}
