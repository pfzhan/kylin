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

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

import io.kyligence.kap.common.obf.IKeep;

@Mapper
public interface QueryStatisticsMapper extends IKeep {

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "QueryStatisticsResult", value = {
            @Result(column = "engine_type", property = "engineType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "count", property = "count", jdbcType = JdbcType.BIGINT),
            @Result(column = "ratio", property = "ratio", jdbcType = JdbcType.DOUBLE),
            @Result(column = "mean", property = "meanDuration", jdbcType = JdbcType.DOUBLE),
            @Result(column = "model", property = "model", jdbcType = JdbcType.VARCHAR),
            @Result(column = "time", property = "time", jdbcType = JdbcType.BIGINT, typeHandler = QueryHistoryTable.InstantHandler.class),
            @Result(column = "month", property = "month", jdbcType = JdbcType.VARCHAR) })
    List<QueryStatistics> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("QueryStatisticsResult")
    QueryStatistics selectOne(SelectStatementProvider selectStatement);
}
