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
    private static final Select sql = new Select(TableIdentifier.table("system", "parts"))
            .column(ColumnWithAlias.builder().name("database").build())
            .column(ColumnWithAlias.builder().name("table").build())
            .column(ColumnWithAlias.builder().name("partition").build())
            .column(ColumnWithAlias.builder().expr("sum(bytes)").build())
            .groupby(new GroupBy()
                    .column(ColumnWithAlias.builder().name("database").build())
                    .column(ColumnWithAlias.builder().name("table").build())
                    .column(ColumnWithAlias.builder().name("partition").build()))
            .where("active=1");

    private ClickHouseSystemQuery() {}
    public static String queryTableStorageSize() {
        return sql.toSql(new ClickHouseRender());
    }


    @Data
    @Builder
    public static class PartitionSize {
        private String database;
        private String table;
        private String partition;
        private long bytes;
    }
}