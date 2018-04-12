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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

public class ColumnACLTest {
    private static Set<String> EMPTY_GROUP_SET = new HashSet<>();

    @Test
    public void testCaseInsensitive() {
        ColumnACL columnACL = new ColumnACL();
        columnACL.add("u1", "t1", Sets.newHashSet("c1", "c2"), MetadataConstants.TYPE_USER);
        Assert.assertEquals(columnACL.getColumnBlackListByTable("T1", MetadataConstants.TYPE_USER).size(), columnACL.getColumnBlackListByTable("t1", MetadataConstants.TYPE_USER).size());
        Assert.assertTrue(columnACL.getColumnBlackList("u1", EMPTY_GROUP_SET).contains("T1.C1"));
        Assert.assertTrue(columnACL.getColumnBlackList("u1", EMPTY_GROUP_SET).contains("t1.c1"));
        Assert.assertTrue(columnACL.getColumnBlackList("u1", EMPTY_GROUP_SET).contains("T1.C2"));
        Assert.assertTrue(columnACL.getColumnBlackList("u1", EMPTY_GROUP_SET).contains("t1.c2"));
    }

    @Test
    public void testDelColumnACLByTable() {
        ColumnACL columnACL = new ColumnACL();
        Set<String> c1 = Sets.newHashSet("C2", "C3");
        columnACL.add("u1", "DB.TABLE1", c1, MetadataConstants.TYPE_USER);
        columnACL.add("u2", "DB.TABLE1", c1, MetadataConstants.TYPE_USER);
        columnACL.add("u2", "DB.TABLE2", c1, MetadataConstants.TYPE_USER);
        columnACL.add("u2", "DB.TABLE3", c1, MetadataConstants.TYPE_USER);
        columnACL.add("u3", "DB.TABLE3", c1, MetadataConstants.TYPE_USER);

        columnACL.add("g1", "DB.TABLE1", c1, MetadataConstants.TYPE_GROUP);
        columnACL.add("g2", "DB.TABLE1", c1, MetadataConstants.TYPE_GROUP);
        columnACL.add("g3", "DB.TABLE2", c1, MetadataConstants.TYPE_GROUP);

        columnACL.deleteByTbl("DB.TABLE1");

        Assert.assertEquals(2, columnACL.size(MetadataConstants.TYPE_USER));
        Assert.assertEquals(1, columnACL.size(MetadataConstants.TYPE_GROUP));
        Assert.assertTrue(columnACL.contains("u2", MetadataConstants.TYPE_USER));
        Assert.assertEquals(Sets.newHashSet("DB.TABLE2.C2", "DB.TABLE2.C3", "DB.TABLE3.C2", "DB.TABLE3.C3"), columnACL.getColumnBlackList("u2", EMPTY_GROUP_SET));
        Assert.assertTrue(columnACL.contains("u3", MetadataConstants.TYPE_USER));
    }

    @Test
    public void testDeleteToEmpty() {
        ColumnACL columnACL = new ColumnACL();
        columnACL.add("u1", "DB.TABLE1", Sets.newHashSet("c1", "c2"), MetadataConstants.TYPE_USER);
        columnACL.add("u2", "DB.TABLE2", Sets.newHashSet("c1", "c2"), MetadataConstants.TYPE_USER);
        columnACL.delete("u1", "DB.TABLE1", MetadataConstants.TYPE_USER);
        columnACL.deleteByTbl("DB.TABLE2");
        Assert.assertEquals(0, columnACL.size());
    }

    @Test
    public void testGetTableBlackList() {
        ColumnACL columnACL = new ColumnACL();
        Set<String> cols = Sets.newHashSet("c1");
        columnACL.add("u1", "t1", cols, MetadataConstants.TYPE_USER);
        columnACL.add("u1", "t2", cols, MetadataConstants.TYPE_USER);
        columnACL.add("u2", "t1", cols, MetadataConstants.TYPE_USER);
        columnACL.add("g1", "t3", cols, MetadataConstants.TYPE_GROUP);
        columnACL.add("g1", "t4", cols, MetadataConstants.TYPE_GROUP);
        columnACL.add("g1", "t5", cols, MetadataConstants.TYPE_GROUP);
        columnACL.add("g2", "t6", cols, MetadataConstants.TYPE_GROUP);
        Set<String> columnBlackList = columnACL.getColumnBlackList("u1", Sets.newHashSet("g1", "g2"));
        Assert.assertEquals(Sets.newHashSet("t1.c1", "t2.c1", "t3.c1", "t4.c1", "t5.c1", "t6.c1"), columnBlackList);
    }

    @Test
    public void testColumnACL() {
        ColumnACL empty = new ColumnACL();
        try {
            empty.delete("a", "DB.TABLE1", MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:a has no column ACL", e.getMessage());
        }

        //add
        ColumnACL columnACL = new ColumnACL();
        Set<String> c1 = Sets.newHashSet("C2", "C3");
        columnACL.add("user1", "DB.TABLE1", c1, MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, columnACL.size(MetadataConstants.TYPE_USER));

        //add duplicated
        try {
            columnACL.add("user1", "DB.TABLE1", c1, MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:user1 already in table's columns blacklist!", e.getMessage());
        }

        //add null column list
        columnACL.add("user1", "DB.TABLE2", new TreeSet<String>(), MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, columnACL.size(MetadataConstants.TYPE_USER));

        //add different table column list
        columnACL.add("user2", "DB.TABLE2", c1, MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, columnACL.size(MetadataConstants.TYPE_USER));

        //add different user column list
        Set<String> c2 = Sets.newHashSet("C2", "C3");
        columnACL.add("user1", "DB.TABLE2", c2, MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, columnACL.size(MetadataConstants.TYPE_USER));
        Assert.assertEquals(4, columnACL.getColumnBlackList("user1", EMPTY_GROUP_SET).size());
        Assert.assertEquals(2, columnACL.getColumnBlackList("user2", EMPTY_GROUP_SET).size());

        //update
        Set<String> c3 = Sets.newHashSet("C3");
        columnACL.update("user1", "DB.TABLE2", c3, MetadataConstants.TYPE_USER);
        Assert.assertEquals(c3, columnACL.getColumnBlackListByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1"));

        //update
        columnACL.update("user1", "DB.TABLE2", new TreeSet<String>(), MetadataConstants.TYPE_USER);
        Assert.assertEquals(c3, columnACL.getColumnBlackListByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1"));

        //delete
        Assert.assertEquals(3, columnACL.getColumnBlackList("user1", EMPTY_GROUP_SET).size());
        columnACL.delete("user1", "DB.TABLE2", MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, columnACL.size(MetadataConstants.TYPE_USER));
        Assert.assertEquals(2, columnACL.getColumnBlackList("user1", EMPTY_GROUP_SET).size());
        Assert.assertNull(columnACL.getColumnBlackListByTable("DB.TABLE2", MetadataConstants.TYPE_USER).get("user1"));
        Assert.assertNotNull(columnACL.getColumnBlackListByTable("DB.TABLE1", MetadataConstants.TYPE_USER).get("user1"));

        //delete
        columnACL.delete("user1", MetadataConstants.TYPE_USER);
        Assert.assertFalse(columnACL.contains("user1", MetadataConstants.TYPE_USER));
    }
}
