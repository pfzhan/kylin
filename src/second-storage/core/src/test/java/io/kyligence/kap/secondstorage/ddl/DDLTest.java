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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class DDLTest {
    @Test
    public void testCreateDb() {
        final CreateDatabase createDb = CreateDatabase.createDatabase("xx");
        assertEquals("CREATE DATABASE if not exists xx", createDb.toSql());
    }

    @Test
    public void testDropTable(){
        final DropTable drop = DropTable.dropTable("pufa", "xx");
        assertEquals("DROP TABLE if exists `pufa`.`xx`", drop.toSql());
    }

    @Test
    public void testRenameTable(){
        final RenameTable drop = RenameTable.renameSource("pufa", "xx_temp").to("pufa", "xx");
        assertEquals("RENAME TABLE `pufa`.`xx_temp` TO `pufa`.`xx`", drop.toSql());
    }

    @Test
    public void testCreateTable(){
        final CreateTable<?> create =
                CreateTable.create("pufa", "xx")
                .columns(new ColumnWithType("a", "int"));
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int)", create.toSql());

        // test create table with nullable
        final CreateTable<?> create2 =
                CreateTable.create("pufa", "xx")
                        .columns(new ColumnWithType("a", "int", true));
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a Nullable(int))", create2.toSql());
    }

    @Test
    public void testInsertInto() throws SQLException {
        final InsertInto insertInto = InsertInto.insertInto("pufa", "xx").from("pufa", "ut");
        assertEquals("INSERT INTO `pufa`.`xx` SELECT * FROM `pufa`.`ut`", insertInto.toSql());
    }

    @Test
    public void testInsertIntoValue() throws SQLException {
        final InsertInto insertInto =
                InsertInto.insertInto("pufa", "xx").set("column1", 42).set(
                        "column2", "xyz");
        assertEquals(
                "INSERT INTO `pufa`.`xx` (column1, column2) VALUES (42, 'xyz')",
                insertInto.toSql());
    }

    @Test
    public void testSelectDistinctWithAlias() {
        final Select select = new Select(TableIdentifier.table("test", "table"))
                .column(ColumnWithAlias.builder().distinct(true).name("col1").alias("value1").build())
                .column(ColumnWithAlias.builder().distinct(false).name("col2").build());
        assertEquals("SELECT DISTINCT `col1` AS value1, `col2` FROM `test`.`table`", select.toSql());
    }
}
