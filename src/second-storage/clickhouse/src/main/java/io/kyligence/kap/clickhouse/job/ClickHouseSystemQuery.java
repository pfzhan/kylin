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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Function;

public class ClickHouseSystemQuery {
    public static final Function<ResultSet, PartitionSize> TABLE_STORAGE_MAPPER= rs -> {
        try {
            return PartitionSize.builder()
                    .database(rs.getString(1))
                    .table(rs.getString(2))
                    .partition(rs.getString(3))
                    .bytes(Long.parseLong(rs.getString(4)))
                    .build();
        } catch (SQLException sqlException) {
            return ExceptionUtils.rethrow(sqlException);
        }
    };

    public static final Function<ResultSet, QueryMetric> QUERY_METRIC_MAPPER= rs -> {
        try {
            return QueryMetric.builder()
                    .readRows(rs.getLong(1))
                    .readBytes(rs.getLong(2))
                    .clientName(rs.getString(3))
                    .build();
        } catch (SQLException sqlException) {
            return ExceptionUtils.rethrow(sqlException);
        }
    };

    private static final Select tableStorageSize = new Select(TableIdentifier.table("system", "parts"))
            .column(ColumnWithAlias.builder().name("database").build())
            .column(ColumnWithAlias.builder().name("table").build())
            .column(ColumnWithAlias.builder().name("partition").build())
            .column(ColumnWithAlias.builder().expr("sum(bytes)").build())
            .groupby(new GroupBy()
                    .column(ColumnWithAlias.builder().name("database").build())
                    .column(ColumnWithAlias.builder().name("table").build())
                    .column(ColumnWithAlias.builder().name("partition").build()))
            .where("active=1");

    private static final Select queryMetric = new Select(TableIdentifier.table("system", "query_log"))
            .column(ColumnWithAlias.builder().expr("sum(read_rows)").alias("readRows").build())
            .column(ColumnWithAlias.builder().expr("sum(read_bytes)").alias("readBytes").build())
            .column(ColumnWithAlias.builder().name("client_name").alias("clientName").build())
            .groupby(new GroupBy().column(ColumnWithAlias.builder().name("client_name").build()))
            .where("type = 'QueryFinish' AND event_time >= addHours(now(), -1) AND event_date >= addDays(now(), -1) AND position(client_name, '%s') = 1");

    private ClickHouseSystemQuery() {}

    public static String queryTableStorageSize() {
        return tableStorageSize.toSql(new ClickHouseRender());
    }

    public static String queryQueryMetric(String queryId) {
        return String.format(Locale.ROOT, queryMetric.toSql(new ClickHouseRender()), queryId);
    }

    @Data
    @Builder
    public static class PartitionSize {
        private String database;
        private String table;
        private String partition;
        private long bytes;
    }

    @Data
    @Builder
    public static class QueryMetric {
        private String clientName;
        private long readRows;
        private long readBytes;
    }
}
