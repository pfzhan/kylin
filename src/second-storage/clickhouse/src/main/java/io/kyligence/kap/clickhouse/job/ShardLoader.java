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

import io.kyligence.kap.clickhouse.database.ClickHouseOperator;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.RenameTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.kyligence.kap.clickhouse.job.DataLoader.columns;
import static io.kyligence.kap.clickhouse.job.DataLoader.getPrefixColumn;

@Getter
@Slf4j
public class ShardLoader {
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

    public ShardLoader(ShardLoadContext context) throws SQLException {
        this.clickHouse = new ClickHouse(context.jdbcURL);
        this.database = context.database;
        this.tableEngine = context.tableEngine;
        this.layout = context.layout;
        this.parquetFiles = context.parquetFiles;
        this.partitionColumn = context.partitionColumn;
        this.partitionFormat = context.partitionFormat;
        this.incremental = partitionColumn != null;
        this.destTableName = context.destTableName;
        this.insertTempTableName = context.executableId + "@" + destTableName + "_temp";
        this.destTempTableName = context.executableId + "@" + destTableName + "_" + NameUtil.TEMP_TABLE_FLAG + "_tmp";
        this.likeTempTableName = context.executableId + "@" + destTableName + "_" + NameUtil.TEMP_TABLE_FLAG + "_ke_like";
        this.targetPartitions = context.targetPartitions;
    }

    private void commitIncrementalLoad() throws SQLException {
        final ClickHouseCreateTable likeTable = ClickHouseCreateTable.createCKTableIgnoreExist(database, destTableName)
                .likeTable(database, insertTempTableName);
        clickHouse.apply(likeTable.toSql(render));
        Select queryPartition = new Select(TableIdentifier.table(database, insertTempTableName))
                .column(ColumnWithAlias.builder().name(getPrefixColumn(partitionColumn)).distinct(true).build());
        List<Date> partitions = clickHouse.queryPartition(queryPartition.toSql(render), partitionFormat);
        // clean exists partition data
        batchDropPartition(targetPartitions);
        batchMovePartition(partitions, committedPartition);
    }

    public void setup(boolean newJob) throws SQLException {
        //1. prepare database and temp table
        final CreateDatabase createDb = CreateDatabase.createDatabase(database);
        clickHouse.apply(createDb.toSql(render));
        if (newJob) {
            createTable(insertTempTableName, layout, partitionColumn, true);
        } else {
            createTableIfNotExist(insertTempTableName, layout, partitionColumn, true);
        }
        createTable(likeTempTableName, layout, partitionColumn, false);
    }

    public List<String> loadDataIntoTempTable(List<String> history, AtomicBoolean stopped) throws SQLException {
        // 2 insert into temp
        List<String> completeFiles = new ArrayList<>();
        for (int index = 0; index < parquetFiles.size(); index++) {
            if (stopped.get()) break;
            if (history.contains(parquetFiles.get(index))) continue;
            loadOneFile(insertTempTableName, parquetFiles.get(index),
                    String.format(Locale.ROOT, "%s_src_%05d", insertTempTableName, index));
            completeFiles.add(parquetFiles.get(index));
        }
        return completeFiles;
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
        final RenameTable renameToTempTemp = RenameTable.renameSource(database, destTableName).to(database,
                destTempTableName);
        ClickHouseOperator operator = new ClickHouseOperator(clickHouse);
        if (operator.listTables(database).contains(destTableName)) {
            clickHouse.apply(renameToTempTemp.toSql(render));
        }
        final RenameTable renameToDest = RenameTable.renameSource(database, insertTempTableName).to(database,
                destTableName);
        clickHouse.apply(renameToDest.toSql(render));
    }

    @Builder
    public static class ShardLoadContext {
        String executableId;
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
        if (isIncremental()) {
            batchDropPartition(committedPartition);
        }
    }

    private void batchDropPartition(List<Date> partitions) throws SQLException {
        AlterTable alterTable;
        val dateFormat = new SimpleDateFormat(partitionFormat, Locale.getDefault(Locale.Category.FORMAT));
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, destTableName),
                    new AlterTable.ManipulatePartition(dateFormat.format(partition),
                            AlterTable.PartitionOperation.DROP));
            clickHouse.apply(alterTable.toSql(render));
        }
    }

    private void batchMovePartition(List<Date> partitions, List<Date> successPartition) throws SQLException {
        AlterTable alterTable;
        val dateFormat = new SimpleDateFormat(partitionFormat, Locale.getDefault(Locale.Category.FORMAT));
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, insertTempTableName),
                    new AlterTable.ManipulatePartition(dateFormat.format(partition),
                            TableIdentifier.table(database, destTableName), AlterTable.PartitionOperation.MOVE));
            // clean partition data
            clickHouse.apply(alterTable.toSql(render));
            if (successPartition != null) {
                successPartition.add(partition);
            }
        }
    }

    public void cleanUp(boolean keepInsertTempTable) throws SQLException {
        if (!keepInsertTempTable) {
            dropTable(insertTempTableName);
        }
        dropTable(destTempTableName);
        dropTable(likeTempTableName);
    }

    public void cleanUpQuietly(boolean keepInsertTempTable) {
        try {
            this.cleanUp(keepInsertTempTable);
        } catch (SQLException e) {
            log.error("clean temp table on {} failed.", clickHouse.getPreprocessedUrl(), e);
        }
    }

    private void createTable(String table, LayoutEntity layout, String partitionBy, boolean addPrefix)
            throws SQLException {
        dropTable(table);

        final ClickHouseCreateTable mergeTable = ClickHouseCreateTable.createCKTable(database, table)
                .columns(columns(layout, partitionBy, addPrefix))
                .partitionBy(addPrefix && partitionBy != null ? getPrefixColumn(partitionBy) : partitionBy)
                .engine(Engine.DEFAULT);
        clickHouse.apply(mergeTable.toSql(render));
    }

    private void createTableIfNotExist(String table, LayoutEntity layout, String partitionBy, boolean addPrefix) throws SQLException {
        final ClickHouseCreateTable mergeTable = ClickHouseCreateTable.createCKTableIgnoreExist(database, table)
                .columns(columns(layout, partitionBy, addPrefix))
                .partitionBy(addPrefix && partitionBy != null ? getPrefixColumn(partitionBy) : partitionBy)
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
            final ClickHouseCreateTable likeTable = ClickHouseCreateTable.createCKTable(database, srcTable)
                    .likeTable(database, likeTempTableName).engine(tableEngine.apply(parquetFile));
            clickHouse.apply(likeTable.toSql(render));

            final InsertInto insertInto = InsertInto.insertInto(database, destTable).from(database, srcTable);
            clickHouse.apply(insertInto.toSql(render));
        } finally {
            dropTable(srcTable);
        }
    }
}