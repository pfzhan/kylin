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

import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.CLOSED;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.LEFT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.OPEN;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.RIGHT_INCLUSIVE;


public class RowACLTest {
    @Test
    public void testTrimConds() {
        Assert.assertEquals("'a'", RowACL.Cond.trim("a", "varchar(256)"));
        Assert.assertEquals("'a''b'", RowACL.Cond.trim("a'b", "string"));
        Assert.assertEquals("DATE '2017-08-30'", RowACL.Cond.trim("1504051200000", "date"));
        Assert.assertEquals("TIME '20:17:40'", RowACL.Cond.trim("73060000", "time"));
        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'", RowACL.Cond.trim("1505275932000", "datetime"));
        Assert.assertEquals("TIMESTAMP '2017-09-13 04:12:12'", RowACL.Cond.trim("1505275932000", "timestamp"));
        Assert.assertEquals("7", RowACL.Cond.trim("7", "int"));
    }

    @Test
    public void testRowACLToString() {
        RowACL.Cond cond1 = new RowACL.Cond(OPEN, null, "100");
        RowACL.Cond cond2 = new RowACL.Cond(RIGHT_INCLUSIVE, null, "100");
        RowACL.Cond cond3 = new RowACL.Cond(OPEN, "100", null);
        RowACL.Cond cond4 = new RowACL.Cond(LEFT_INCLUSIVE, "100", null);
        RowACL.Cond cond5 = new RowACL.Cond(OPEN, null, null);
        RowACL.Cond cond6 = new RowACL.Cond(CLOSED, null, null);
        Assert.assertEquals("(c1<100)", cond1.toString("c1", "int"));
        Assert.assertEquals("(c1<=100)", cond2.toString("c1", "int"));
        Assert.assertEquals("(c1>100)", cond3.toString("c1", "int"));
        Assert.assertEquals("(c1>=100)", cond4.toString("c1", "int"));
        Assert.assertEquals("(c1<>null)", cond5.toString("c1", "int"));
        Assert.assertEquals("(c1=null)", cond6.toString("c1", "int"));
    }

    @Test
    public void testDelRowACLByTable() {
        RowACL rowACL = new RowACL();

        Map<String, List<RowACL.Cond>> condsWithColumn = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("ab"));
        List<RowACL.Cond> cond11 = Lists.newArrayList(new RowACL.Cond("a"));
        condsWithColumn.put("COL1", cond1);
        condsWithColumn.put("COL11", cond11);
        rowACL.add("u1", "DB.TABLE1", condsWithColumn);
        rowACL.add("u2", "DB.TABLE1", condsWithColumn);
        rowACL.add("u2", "DB.TABLE2", condsWithColumn);
        rowACL.add("u2", "DB.TABLE3", condsWithColumn);
        rowACL.add("u3", "DB.TABLE3", condsWithColumn);

        rowACL.deleteByTbl("DB.TABLE1");
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().size());
        Assert.assertFalse(rowACL.getTableRowCondsWithUser().containsKey("u1"));
        Assert.assertNotNull(rowACL.getTableRowCondsWithUser().containsKey("u2"));
        Assert.assertNotNull(rowACL.getTableRowCondsWithUser().containsKey("u3"));
        Assert.assertEquals(0, rowACL.getTableRowCondsWithUser().get("u2").getRowCondListByTable("DB.TABLE1").size());
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("u2").getRowCondListByTable("DB.TABLE2").size());
    }

    @Test
    public void testRowACLCRUD() {
        RowACL empty = new RowACL();
        try {
            empty.delete("u", "t");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:u not have any row acl conds!", e.getMessage());
        }

        //add
        RowACL rowACL = new RowACL();
        Map<String, List<RowACL.Cond>> condsWithColumn1 = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"), new RowACL.Cond("c"));
        List<RowACL.Cond> cond11 = Lists.newArrayList(new RowACL.Cond("d"), new RowACL.Cond("e"));
        condsWithColumn1.put("COL1", cond1);
        condsWithColumn1.put("COL11", cond11);
        rowACL.add("user1", "DB.TABLE1", condsWithColumn1);

        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").size());
        Assert.assertEquals(3, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").getCondsByColumn("COL1").size());

        //add duplicated
        try {
            Map<String, List<RowACL.Cond>> condsWithColumn2 = new HashMap<>();
            List<RowACL.Cond> cond2 = Lists.newArrayList(new RowACL.Cond("a"));
            condsWithColumn1.put("COL2", cond2);
            rowACL.add("user1", "DB.TABLE1", condsWithColumn2);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Operation fail, user:user1, table:DB.TABLE1 already in row cond list!");
        }

        //add different table's column cond list
        Map<String, List<RowACL.Cond>> condsWithColumn3 = new HashMap<>();
        List<RowACL.Cond> cond3 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"));
        condsWithColumn3.put("COL2", cond3);
        rowACL.add("user1", "DB.TABLE2", condsWithColumn3);
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1").size());
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE2").size());

        //add different user
        Map<String, List<RowACL.Cond>> condsWithColumn4 = new HashMap<>();
        List<RowACL.Cond> cond4 = Lists.newArrayList(new RowACL.Cond("c"));
        condsWithColumn4.put("COL2", cond4);
        rowACL.add("user2", "DB2.TABLE2", condsWithColumn4);
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().size());
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user2").size());
        Assert.assertEquals(cond4, rowACL.getTableRowCondsWithUser().get("user2").getRowCondListByTable("DB2.TABLE2").getCondsByColumn("COL2"));

        //update
        Map<String, List<RowACL.Cond>> condsWithColumn5 = new HashMap<>();
        List<RowACL.Cond> cond5 = Lists.newArrayList(new RowACL.Cond("f"), new RowACL.Cond("ff"));
        condsWithColumn5.put("COL2", cond5);
        rowACL.update("user1", "DB.TABLE2", condsWithColumn5);
        Assert.assertEquals(cond5, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE2").getCondsByColumn("COL2"));

        //delete
        rowACL.delete("user1", "DB.TABLE2");
        Assert.assertNotNull(rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE1"));
        Assert.assertEquals(0, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.TABLE2").size());

        //delete
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user2").size());
        rowACL.deleteByUser("user2");
        Assert.assertNull(rowACL.getTableRowCondsWithUser().get("user2"));
    }
}
