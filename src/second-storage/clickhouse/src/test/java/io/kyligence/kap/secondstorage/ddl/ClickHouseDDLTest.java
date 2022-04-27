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
package io.kyligence.kap.secondstorage.ddl;

import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClickHouseDDLTest {

    @Test
    public void testCKCreateTable(){
        final ClickHouseCreateTable create =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()");
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple()", create.toSql(render));

        final ClickHouseCreateTable create2 =
                ClickHouseCreateTable.createCKTable("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()");
        assertEquals("CREATE TABLE `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple()", create2.toSql(render));
    }

    @Test
    public void testCKCreateTableWithDeduplicationWindow(){
        final ClickHouseCreateTable create =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()")
                        .deduplicationWindow(3);
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 3", create.toSql(render));

        final ClickHouseCreateTable create2 =
                ClickHouseCreateTable.createCKTable("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()")
                        .deduplicationWindow(3);
        assertEquals("CREATE TABLE `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 3", create2.toSql(render));
    }

    @Test
    public void testCKCreateTableLike() {
        final ClickHouseCreateTable createLike =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "ut")
                        .likeTable("pufa", "xx")
                        .engine("HDFS(xxx)");
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`ut` AS `pufa`.`xx` ENGINE = HDFS(xxx)", createLike.toSql(render));
    }

    @Test
    public void testCKMovePartition() {
        final AlterTable alterTable = new AlterTable(
                TableIdentifier.table("test", "table1"),
                new AlterTable.ManipulatePartition("2020-01-01",
                        TableIdentifier.table("test", "table2"), AlterTable.PartitionOperation.MOVE));
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("ALTER TABLE `test`.`table1` MOVE PARTITION '2020-01-01' TO TABLE `test`.`table2`",
                alterTable.toSql(render));
    }

    @Test
    public void testCKDropPartition() {
        final AlterTable alterTable = new AlterTable(
                TableIdentifier.table("test", "table1"),
                new AlterTable.ManipulatePartition("2020-01-01", AlterTable.PartitionOperation.DROP));
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("ALTER TABLE `test`.`table1` DROP PARTITION '2020-01-01'",
                alterTable.toSql(render));
    }

}
