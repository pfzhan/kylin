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

package io.kyligence.kap.metadata.acl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RowACLTest {
    @Test
    public void testTrimConds() {
        Map<String, List<String>> condsWithCol = new HashMap<>();
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "string");
        columnWithType.put("COL3", "date");
        columnWithType.put("COL4", "time");
        columnWithType.put("COL5", "datetime");
        columnWithType.put("COL6", "timestamp");
        columnWithType.put("COL7", "int");
        List<String> cond1 = Lists.newArrayList("a", "a'b");
        List<String> cond2 = Lists.newArrayList("a", "a'b");
        List<String> cond3 = Lists.newArrayList("1504051200000"); //date
        List<String> cond4 = Lists.newArrayList("73060000"); //time
        List<String> cond5 = Lists.newArrayList("1505275932000"); //datetime
        List<String> cond6 = Lists.newArrayList("1505275932000"); //timestamp
        List<String> cond7 = Lists.newArrayList("7"); //normal type
        condsWithCol.put("COL1", cond1);
        condsWithCol.put("COL2", cond2);
        condsWithCol.put("COL3", cond3);
        condsWithCol.put("COL4", cond4);
        condsWithCol.put("COL5", cond5);
        condsWithCol.put("COL6", cond6);
        condsWithCol.put("COL7", cond7);

        Map<String, List<String>> result = RowACL.trimConds(condsWithCol, columnWithType);

        List<String> cond11 = Lists.newArrayList("'a'", "'a''b'");
        List<String> cond22 = Lists.newArrayList("'a'", "'a''b'");
        List<String> cond33 = Lists.newArrayList("DATE '2017-08-30'"); //date
        List<String> cond44 = Lists.newArrayList("TIME '20:17:40'"); //time
        List<String> cond55 = Lists.newArrayList("TIMESTAMP '2017-09-13 04:12:12'"); //datetime
        List<String> cond66 = Lists.newArrayList("TIMESTAMP '2017-09-13 04:12:12'"); //timestamp
        List<String> cond77 = Lists.newArrayList("7"); //normal type
        Assert.assertEquals(cond11, result.get("COL1"));
        Assert.assertEquals(cond22, result.get("COL2"));
        Assert.assertEquals(cond33, result.get("COL3"));
        Assert.assertEquals(cond44, result.get("COL4"));
        Assert.assertEquals(cond55, result.get("COL5"));
        Assert.assertEquals(cond66, result.get("COL6"));
        Assert.assertEquals(cond77, result.get("COL7"));
    }

    @Test
    public void testDelRowACLByTable() {
        RowACL rowACL = new RowACL();

        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL11", "varchar(256)");

        Map<String, List<String>> condsWithColumn = new HashMap<>();
        List<String> cond1 = Lists.newArrayList("a", "b", "c");
        List<String> cond11 = Lists.newArrayList("d", "e");
        condsWithColumn.put("COL1", cond1);
        condsWithColumn.put("COL11", cond11);
        rowACL.add("u1", "DB.TABLE1", condsWithColumn, columnWithType);
        rowACL.add("u2", "DB.TABLE1", condsWithColumn, columnWithType);
        rowACL.add("u2", "DB.TABLE2", condsWithColumn, columnWithType);
        rowACL.add("u2", "DB.TABLE3", condsWithColumn, columnWithType);
        rowACL.add("u3", "DB.TABLE3", condsWithColumn, columnWithType);

        RowACL expected = new RowACL();
        expected.add("u2", "DB.TABLE2", condsWithColumn, columnWithType);
        expected.add("u2", "DB.TABLE3", condsWithColumn, columnWithType);
        expected.add("u3", "DB.TABLE3", condsWithColumn, columnWithType);
        Assert.assertEquals(expected, rowACL);
    }

    @Test
    public void testConcatConds() {
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "varchar(256)");

        Map<String, List<String>> condsWithColumn = new HashMap<>();
        List<String> cond1 = Lists.newArrayList("a", "b", "c");
        List<String> cond2 = Lists.newArrayList("d", "e");
        condsWithColumn.put("COL1", cond1);
        condsWithColumn.put("COL2", cond2);

        String exceptConds = "(COL2='d' OR COL2='e') AND (COL1='a' OR COL1='b' OR COL1='c')";
        Assert.assertEquals(exceptConds, RowACL.concatConds(condsWithColumn, columnWithType));

        Map<String, List<String>> condsWithColumn1 = new HashMap<>();
        List<String> cond11 = Lists.newArrayList("a", "b");
        condsWithColumn1.put("COL1", cond11);
        Assert.assertEquals("(COL1='a' OR COL1='b')", RowACL.concatConds(condsWithColumn1, columnWithType));
    }

    @Test
    public void testRowACL() {
        RowACL empty = new RowACL();
        try {
            empty.delete("u", "t");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:u not have any row acl conds!", e.getMessage());
        }

        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL11", "varchar(256)");
        columnWithType.put("COL2", "varchar(256)");
        columnWithType.put("COL3", "varchar(256)");

        //add
        RowACL rowACL = new RowACL();
        Map<String, List<String>> condsWithColumn1 = new HashMap<>();
        List<String> cond1 = Lists.newArrayList("a", "b", "c");
        List<String> cond11 = Lists.newArrayList("d", "e");
        condsWithColumn1.put("COL1", cond1);
        condsWithColumn1.put("COL11", cond11);
        rowACL.add("user1", "DB.TABLE1", condsWithColumn1, columnWithType);

        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").size());
        Assert.assertEquals(3, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").getCondsByColumn("COL1").size());
        Assert.assertEquals(2, rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE1").split("AND").length);
        //will pass in local and github, but failed in ci, cuz the order between left and right is random
        //Assert.assertEquals("COL11=d OR COL11=e AND COL1=a OR COL1=b OR COL1=c", rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE1"));

        //add duplicated
        try {
            Map<String, List<String>> condsWithColumn2 = new HashMap<>();
            List<String> cond2 = Lists.newArrayList("a");
            condsWithColumn1.put("COL2", cond2);
            rowACL.add("user1", "DB.TABLE1", condsWithColumn2, columnWithType);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Operation fail, user:user1, table:DB.TABLE1 already in row cond list!");
        }

        //add different table's column cond list
        Map<String, List<String>> condsWithColumn3 = new HashMap<>();
        List<String> cond3 = Lists.newArrayList("a", "b");
        condsWithColumn3.put("COL2", cond3);
        rowACL.add("user1", "DB.TABLE2", condsWithColumn3, columnWithType);
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").size());
        Assert.assertEquals(2, rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE1").split("AND").length);
        Assert.assertEquals("(COL2='a' OR COL2='b')", rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE2"));

        //add different user
        Map<String, List<String>> condsWithColumn4 = new HashMap<>();
        List<String> cond4 = Lists.newArrayList("c");
        condsWithColumn4.put("COL2", cond4);
        rowACL.add("user2", "DB2.TABLE2", condsWithColumn4, columnWithType);
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user2").size());
        Assert.assertEquals(cond4, rowACL.getTableRowCondsWithUser().get("user2").getRowCondListByTable("DB2.TABLE2").getCondsByColumn("COL2"));
        Assert.assertEquals("COL2='c'", rowACL.getQueryUsedConds().get("user2").getConcatedCondsByTable("DB2.TABLE2"));

        //update
        Map<String, List<String>> condsWithColumn5 = new HashMap<>();
        List<String> cond5 = Lists.newArrayList("f", "ff");
        condsWithColumn5.put("COL2", cond5);
        rowACL.update("user1", "DB.TABLE2", condsWithColumn5, columnWithType);
        Assert.assertEquals(cond5, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE2").getCondsByColumn("COL2"));
        Assert.assertEquals("(COL2='f' OR COL2='ff')", rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE2"));

        //delete
        rowACL.delete("user1", "DB.TABLE2");
        Assert.assertNotNull(rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1"));
        Assert.assertEquals(0, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE2").size());
        Assert.assertNotNull(rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE1"));
        Assert.assertNull(rowACL.getQueryUsedConds().get("user1").getConcatedCondsByTable("DB.TABLE2"));

        //delete
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user2").size());
        rowACL.delete("user2");
        Assert.assertNull(rowACL.getTableRowCondsWithUser().get("user2"));
        Assert.assertNull(rowACL.getQueryUsedConds().get("user2"));
    }

}
