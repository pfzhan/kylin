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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.DropDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.msgpack.core.Preconditions;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

@Getter
@Slf4j
public class ShardClean {
    @JsonProperty("node")
    private String node;
    @JsonProperty("database")
    private String database;
    @JsonProperty("table")
    private String table;
    @JsonProperty("partitions")
    private List<Date> partitions;

    @JsonIgnore
    private ClickHouse clickHouse;

    public ShardClean() {
    }

    public ShardClean(String node, String database) {
        this(node, database, null, null);
    }

    public ShardClean(String node, String database, String table) {
        this(node, database, table, null);
    }

    public ShardClean(String node, String database, String table, List<Date> partitions) {
        this.node = Preconditions.checkNotNull(node);
        this.database = Preconditions.checkNotNull(database);
        this.table = table;
        this.partitions = partitions;
    }

    public ClickHouse getClickHouse() {
        if (Objects.isNull(clickHouse)) {
            try {
                clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node));
            } catch (SQLException e) {
                log.error("node {} connect failed, jdbc url: {}. Please check node status.", node, SecondStorageNodeHelper.resolve(node));
                return ExceptionUtils.rethrow(e);
            }
        }
        return clickHouse;
    }

    public void cleanDatabase() throws SQLException {
        val dropDatabase = DropDatabase.dropDatabase(database);
        log.debug("drop database {}", database);
        Preconditions.checkNotNull(getClickHouse()).apply(dropDatabase.toSql(getRender()));
    }

    private ClickHouseRender getRender() {
        return new ClickHouseRender();
    }

    public void cleanTable() throws SQLException {
        Preconditions.checkNotNull(table);
        val dropTable = DropTable.dropTable(database, table);
        log.debug("drop table {}.{}", database, table);
        Preconditions.checkNotNull(getClickHouse()).apply(dropTable.toSql(getRender()));
    }

    public void cleanPartitions() throws SQLException {
        Preconditions.checkNotNull(table);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(partitions));
        AlterTable alterTable;
        log.debug("drop partitions in table {}.{}: {}", database, table, partitions);
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, table),
                    new AlterTable.ManipulatePartition(Objects.toString(partition),
                            AlterTable.PartitionOperation.DROP));
            Preconditions.checkNotNull(getClickHouse()).apply(alterTable.toSql(getRender()));
        }
    }
}
