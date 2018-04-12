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

import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.CLOSED;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.LEFT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.OPEN;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.RIGHT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.RowACL.concatConds;


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
    public void testConcatConds() {
        Map<String, List<RowACL.Cond>> condsWithCol = new HashMap<>();
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "timestamp");
        columnWithType.put("COL3", "int");
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"), new RowACL.Cond("a'b"));
        List<RowACL.Cond> cond6 = Lists.newArrayList(new RowACL.Cond(LEFT_INCLUSIVE, "1505275932000", "1506321155000")); //timestamp
        List<RowACL.Cond> cond7 = Lists.newArrayList(new RowACL.Cond(RIGHT_INCLUSIVE, "7", "100")); //normal type
        condsWithCol.put("COL1", cond1);
        condsWithCol.put("COL2", cond6);
        condsWithCol.put("COL3", cond7);
        RowACL.ColumnToConds columnToConds = new RowACL.ColumnToConds(condsWithCol);
        Assert.assertEquals(
                "((COL1='a') OR (COL1='b') OR (COL1='a''b')) AND (COL2>=TIMESTAMP '2017-09-13 04:12:12' AND COL2<TIMESTAMP '2017-09-25 06:32:35') AND (COL3>7 AND COL3<=100)",
                concatConds(columnToConds, columnWithType));
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

        Map<String, List<RowACL.Cond>> colToConds = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("ab"));
        List<RowACL.Cond> cond11 = Lists.newArrayList(new RowACL.Cond("a"));
        colToConds.put("COL1", cond1);
        colToConds.put("COL11", cond11);
        RowACL.ColumnToConds columnToConds = new RowACL.ColumnToConds(colToConds);
        rowACL.add("u1", "DB.TABLE1", columnToConds, MetadataConstants.TYPE_USER);
        rowACL.add("u2", "DB.TABLE1", columnToConds, MetadataConstants.TYPE_USER);
        rowACL.add("u2", "DB.TABLE2", columnToConds, MetadataConstants.TYPE_USER);
        rowACL.add("u2", "DB.TABLE3", columnToConds, MetadataConstants.TYPE_USER);
        rowACL.add("u3", "DB.TABLE3", columnToConds, MetadataConstants.TYPE_USER);

        rowACL.add("g1", "DB.TABLE1", columnToConds, MetadataConstants.TYPE_GROUP);
        rowACL.add("g2", "DB.TABLE1", columnToConds, MetadataConstants.TYPE_GROUP);
        rowACL.add("g3", "DB.TABLE2", columnToConds, MetadataConstants.TYPE_GROUP);

        rowACL.deleteByTbl("DB.TABLE1");
        Assert.assertEquals(2, rowACL.size(MetadataConstants.TYPE_USER));
        Assert.assertEquals(1, rowACL.size(MetadataConstants.TYPE_GROUP));
        Assert.assertFalse(rowACL.contains("u1", MetadataConstants.TYPE_USER));
        Assert.assertNotNull(rowACL.contains("u2", MetadataConstants.TYPE_USER));
        Assert.assertNotNull(rowACL.contains("u3", MetadataConstants.TYPE_USER));
        Assert.assertEquals(0, rowACL.getColumnToCondsByTable("DB.TABLE1", MetadataConstants.TYPE_USER).size());
        Assert.assertEquals(2, rowACL.getColumnToCondsByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("u2").size());
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
        Map<String, List<RowACL.Cond>> colToConds1 = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"), new RowACL.Cond("c"));
        List<RowACL.Cond> cond11 = Lists.newArrayList(new RowACL.Cond("d"), new RowACL.Cond("e"));
        colToConds1.put("COL1", cond1);
        colToConds1.put("COL11", cond11);
        RowACL.ColumnToConds columnToConds1 = new RowACL.ColumnToConds(colToConds1);
        rowACL.add("user1", "DB.TABLE1", columnToConds1, MetadataConstants.TYPE_USER);

        Assert.assertEquals(2, rowACL.getColumnToCondsByTable("DB.TABLE1", MetadataConstants.TYPE_USER).get("user1").size());
        Assert.assertEquals(3, rowACL.getColumnToCondsByTable("DB.TABLE1", MetadataConstants.TYPE_USER).get("user1").getCondsByColumn("COL1").size());

        //add duplicated
        try {
            rowACL.add("user1", "DB.TABLE1", columnToConds1, MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:user1, table:DB.TABLE1 already has row ACL!", e.getMessage());
        }

        //add different table's column cond list
        Map<String, List<RowACL.Cond>> colToConds3 = new HashMap<>();
        List<RowACL.Cond> cond3 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"));
        colToConds3.put("COL2", cond3);
        RowACL.ColumnToConds columnToConds3 = new RowACL.ColumnToConds(colToConds3);
        rowACL.add("user1", "DB.TABLE2", columnToConds3, MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, rowACL.getColumnToCondsByTable("DB.TABLE1", MetadataConstants.TYPE_USER).get("user1").size());
        Assert.assertEquals(1, rowACL.getColumnToCondsByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1").size());

        //add different user
        Map<String, List<RowACL.Cond>> colToConds4 = new HashMap<>();
        List<RowACL.Cond> cond4 = Lists.newArrayList(new RowACL.Cond("c"));
        colToConds4.put("COL2", cond4);
        RowACL.ColumnToConds columnToConds4 = new RowACL.ColumnToConds(colToConds4);
        rowACL.add("user2", "DB2.TABLE2", columnToConds4, MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, rowACL.size());
        Assert.assertEquals(cond4, rowACL.getColumnToCondsByTable("DB2.TABLE2", MetadataConstants.TYPE_USER).get("user2").getCondsByColumn("COL2"));

        //update
        Map<String, List<RowACL.Cond>> colToConds5 = new HashMap<>();
        List<RowACL.Cond> cond5 = Lists.newArrayList(new RowACL.Cond("f"), new RowACL.Cond("ff"));
        colToConds5.put("COL2", cond5);
        RowACL.ColumnToConds columnToConds5 = new RowACL.ColumnToConds(colToConds5);
        rowACL.update("user1", "DB.TABLE2", columnToConds5, MetadataConstants.TYPE_USER);
        Assert.assertEquals(cond5, rowACL.getColumnToCondsByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1").getCondsByColumn("COL2"));

        //delete
        rowACL.delete("user1", "DB.TABLE2", MetadataConstants.TYPE_USER);
        Assert.assertNotNull(rowACL.getColumnToCondsByTable("DB.TABLE1", MetadataConstants.TYPE_USER).get("user1"));
        Assert.assertNull(rowACL.getColumnToCondsByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1"));

        //delete
        Assert.assertTrue(rowACL.contains("user2", MetadataConstants.TYPE_USER));
        rowACL.delete("user2", MetadataConstants.TYPE_USER);
        Assert.assertFalse(rowACL.contains("user2", MetadataConstants.TYPE_USER));
    }
}
