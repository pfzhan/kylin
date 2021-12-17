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
public interface QueryHistoryMapper {

    @DeleteProvider(type = SqlProviderAdapter.class, method = "delete")
    int delete(DeleteStatementProvider deleteStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "record.id")
    int insert(InsertStatementProvider<QueryMetrics> insertStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "QueryHistoryResult", value = {
            @Result(column = "sql_text", property = "sql", jdbcType = JdbcType.VARCHAR),
            @Result(column = "sql_pattern", property = "sqlPattern", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_time", property = "queryTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "duration", property = "duration", jdbcType = JdbcType.BIGINT),
            @Result(column = "realizations", property = "queryRealizations", jdbcType = JdbcType.VARCHAR),
            @Result(column = "server", property = "hostName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "submitter", property = "querySubmitter", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_status", property = "queryStatus", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_id", property = "queryId", jdbcType = JdbcType.VARCHAR),
            @Result(column = "id", property = "id", jdbcType = JdbcType.BIGINT),
            @Result(column = "total_scan_count", property = "totalScanCount", jdbcType = JdbcType.BIGINT),
            @Result(column = "total_scan_bytes", property = "totalScanBytes", jdbcType = JdbcType.BIGINT),
            @Result(column = "result_row_count", property = "resultRowCount", jdbcType = JdbcType.BIGINT),
            @Result(column = "cache_hit", property = "cacheHit", jdbcType = JdbcType.BOOLEAN),
            @Result(column = "index_hit", property = "indexHit", jdbcType = JdbcType.BOOLEAN),
            @Result(column = "engine_type", property = "engineType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "project_name", property = "projectName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "error_type", property = "errorType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "reserved_field_3", property = "queryHistoryInfo", jdbcType = JdbcType.BLOB, typeHandler = QueryHistoryTable.QueryHistoryInfoHandler.class) })
    List<QueryHistory> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("QueryHistoryResult")
    QueryHistory selectOne(SelectStatementProvider selectStatement);


    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    Long selectAsLong(SelectStatementProvider selectStatement);

    @UpdateProvider(type = SqlProviderAdapter.class, method = "update")
    int update(UpdateStatementProvider updateStatement);

}
