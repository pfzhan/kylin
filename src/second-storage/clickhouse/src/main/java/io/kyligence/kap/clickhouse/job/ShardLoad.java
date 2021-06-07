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

import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.RenameTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static io.kyligence.kap.clickhouse.job.Load.columns;
import static io.kyligence.kap.clickhouse.job.Load.getPrefixColumn;

@Getter
public class ShardLoad {
    private final ClickHouse clickHouse;
    private final String database;
    private final ClickHouseRender render = new ClickHouseRender();
    private final Engine tableEngine;
    private final LayoutEntity layout;
    private final String partitionColumn;
    private final String partitionFormat;
    private final List<String> parquetFiles;
    private final String destTableName;
    private final String insertTempTableName;
    private final String destTempTableName;
    private final String likeTempTableName;
    private final boolean incremental;
    private final List<Date> targetPartitions;
    private final List<Date> committedPartition = new ArrayList<>();

    public ShardLoad(ShardLoadContext context) throws SQLException {
        this.clickHouse = new ClickHouse(context.jdbcURL);
        this.database = context.database;
        this.tableEngine = context.tableEngine;
        this.layout = context.layout;
        this.parquetFiles = context.parquetFiles;
        this.partitionColumn = context.partitionColumn;
        this.partitionFormat = context.partitionFormat;
        this.incremental = partitionColumn != null;
        this.destTableName = context.destTableName;
        this.insertTempTableName = destTableName + "_temp";
        this.destTempTableName = destTableName + "_temp_tmp";
        this.likeTempTableName = destTableName + "_temp_ke_like";
        this.targetPartitions = context.targetPartitions;
    }

    private void commitIncrementalLoad() throws SQLException {
        final ClickHouseCreateTable likeTable =
                ClickHouseCreateTable.createCKTableIgnoreExist(database, destTableName)
                        .likeTable(database, insertTempTableName);
        clickHouse.apply(likeTable.toSql(render));
        Select queryPartition = new Select(TableIdentifier.table(database, insertTempTableName))
                .column(ColumnWithAlias.builder().name(getPrefixColumn(partitionColumn))
                        .distinct(true).build());
        List<Date> partitions = clickHouse.queryPartition(queryPartition.toSql(render), partitionFormat);
        // clean exists partition data
        batchDropPartition(targetPartitions);
        batchMovePartition(partitions, committedPartition);
    }

    public void setup() throws SQLException {
        //1. prepare database and temp table
        final CreateDatabase createDb = CreateDatabase.createDatabase(database);
        clickHouse.apply(createDb.toSql(render));
        createTable(insertTempTableName, layout, partitionColumn, true);
        createTable(likeTempTableName, layout, partitionColumn, false);
    }

    public void loadDataIntoTempTable() throws SQLException {
        // 2 insert into temp
        for (int index = 0; index < parquetFiles.size(); index++) {
            loadOneFile(insertTempTableName, parquetFiles.get(index),
                    String.format(Locale.ROOT, "%s_src_%05d", destTableName, index));
        }
    }

    private boolean tableNotExistError(SQLException e) {
        return e.getErrorCode() == ClickHouseErrorCode.UNKNOWN_TABLE;
    }

    public void commit() throws SQLException {
        if (isIncremental()) {
            commitIncrementalLoad();
        } else {
            commitFullLoad();
        }
    }

    private void commitFullLoad() throws SQLException {
        //3 rename with atomically
        final RenameTable renameToTempTemp =
                RenameTable.renameSource(database, destTableName).to(database, destTempTableName);
        try {
            clickHouse.apply(renameToTempTemp.toSql(render));
        } catch (SQLException e) {
            if (!tableNotExistError(e)) {
                throw e;
            }
        }
        final RenameTable renameToDest =
                RenameTable.renameSource(database, insertTempTableName).to(database, destTableName);
        clickHouse.apply(renameToDest.toSql(render));
    }

    @Builder
    public static class ShardLoadContext {
        String jdbcURL;
        String database;
        LayoutEntity layout;
        List<String> parquetFiles;
        String destTableName;
        Engine tableEngine;
        String partitionColumn;
        String partitionFormat;
        List<Date> targetPartitions;
    }

    public void cleanIncrementLoad() throws SQLException {
        batchDropPartition(committedPartition);
    }

    private void batchDropPartition(List<Date> partitions) throws SQLException {
        AlterTable alterTable;
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, destTableName),
                    new AlterTable.ManipulatePartition(Objects.toString(partition),
                            AlterTable.PartitionOperation.DROP));
            clickHouse.apply(alterTable.toSql(render));
        }
    }

    private void batchMovePartition(List<Date> partitions, List<Date> successPartition) throws SQLException {
        AlterTable alterTable;
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, insertTempTableName),
                    new AlterTable.ManipulatePartition(Objects.toString(partition),
                            TableIdentifier.table(database, destTableName),
                            AlterTable.PartitionOperation.MOVE));
            // clean partition data
            clickHouse.apply(alterTable.toSql(render));
            if (successPartition != null) {
                successPartition.add(partition);
            }
        }
    }

    public void cleanUp() throws SQLException {
        dropTable(insertTempTableName);
        dropTable(destTempTableName);
        dropTable(likeTempTableName);
    }


    private void createTable(String table, LayoutEntity layout, String partitionBy, boolean addPrefix) throws SQLException {
        dropTable(table);

        final ClickHouseCreateTable mergeTable =
                ClickHouseCreateTable.createCKTable(database, table)
                        .columns(columns(layout, partitionBy, addPrefix))
                        .partitionBy(addPrefix && partitionBy!= null ? getPrefixColumn(partitionBy) : partitionBy)
                        .engine(Engine.DEFAULT);
        clickHouse.apply(mergeTable.toSql(render));
    }

    private void dropTable(String table) throws SQLException {
        final String dropSQL = DropTable.dropTable(database, table).toSql(render);
        clickHouse.apply(dropSQL);
    }

    private void loadOneFile(String destTable, String parquetFile, String srcTable) throws SQLException {
        dropTable(srcTable);
        try {
            final ClickHouseCreateTable likeTable =
                    ClickHouseCreateTable.createCKTable(database, srcTable)
                            .likeTable(database, likeTempTableName)
                            .engine(tableEngine.apply(parquetFile));
            clickHouse.apply(likeTable.toSql(render));

            final InsertInto insertInto =
                    InsertInto.insertInto(database, destTable).from(database, srcTable);
            clickHouse.apply(insertInto.toSql(render));
        } finally {
            dropTable(srcTable);
        }
    }
}
