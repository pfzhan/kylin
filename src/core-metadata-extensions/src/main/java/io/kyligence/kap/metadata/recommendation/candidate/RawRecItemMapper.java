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

import java.util.List;

import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

@Mapper
public interface RawRecItemMapper {

    @DeleteProvider(type = SqlProviderAdapter.class, method = "delete")
    int delete(DeleteStatementProvider deleteStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "record.id")
    int insert(InsertStatementProvider<RawRecItem> insertStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "RawRecItemResult", value = {
            @Result(column = "id", property = "id", jdbcType = JdbcType.INTEGER, id = true),
            @Result(column = "project", property = "project", jdbcType = JdbcType.VARCHAR),
            @Result(column = "model_id", property = "modelID", jdbcType = JdbcType.VARCHAR),
            @Result(column = "unique_flag", property = "uniqueFlag", jdbcType = JdbcType.VARCHAR),
            @Result(column = "semantic_version", property = "semanticVersion", jdbcType = JdbcType.INTEGER),
            @Result(column = "type", property = "type", jdbcType = JdbcType.TINYINT, typeHandler = RawRecItemTable.RecTypeHandler.class),
            @Result(column = "rec_entity", property = "recEntity", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.RecItemV2Handler.class),
            @Result(column = "state", property = "state", jdbcType = JdbcType.TINYINT, typeHandler = RawRecItemTable.RecStateHandler.class),
            @Result(column = "create_time", property = "createTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "update_time", property = "updateTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "depend_ids", property = "dependIDs", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.DependIdHandler.class),
            @Result(column = "layout_metric", property = "layoutMetric", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.LayoutMetricHandler.class),
            @Result(column = "cost", property = "cost", jdbcType = JdbcType.DOUBLE),
            @Result(column = "total_latency_of_last_day", property = "totalLatencyOfLastDay", jdbcType = JdbcType.DOUBLE),
            @Result(column = "hit_count", property = "hitCount", jdbcType = JdbcType.INTEGER),
            @Result(column = "total_time", property = "totalTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "max_time", property = "maxTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "min_time", property = "minTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "query_history_info", property = "queryHistoryInfo", jdbcType = JdbcType.VARCHAR),
            @Result(column = "reserved_field_1", property = "recSource", jdbcType = JdbcType.VARCHAR) })
    List<RawRecItem> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("RawRecItemResult")
    RawRecItem selectOne(SelectStatementProvider selectStatement);

    @UpdateProvider(type = SqlProviderAdapter.class, method = "update")
    int update(UpdateStatementProvider updateStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    int selectAsInt(SelectStatementProvider selectStatement);
}
