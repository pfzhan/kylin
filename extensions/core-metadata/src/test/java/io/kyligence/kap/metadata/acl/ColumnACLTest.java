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

import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class ColumnACLTest {
    @Test
    public void testCaseInsensitive() {
        ColumnACL columnACL = new ColumnACL();
        columnACL.add("u1", "t1", Sets.newHashSet("c1", "c2"));
        Assert.assertEquals(columnACL.getColumnBlackListByTable("T1").size(), columnACL.getColumnBlackListByTable("t1").size());
        Assert.assertTrue(columnACL.getColumnBlackListByUser("u1").contains("T1.C1"));
        Assert.assertTrue(columnACL.getColumnBlackListByUser("u1").contains("t1.c1"));
        Assert.assertTrue(columnACL.getColumnBlackListByUser("u1").contains("T1.C2"));
        Assert.assertTrue(columnACL.getColumnBlackListByUser("u1").contains("t1.c2"));
    }

    @Test
    public void testDelColumnACLByTable() {
        ColumnACL columnACL = new ColumnACL();
        Set<String> c1 = Sets.newHashSet("C2", "C3");
        columnACL.add("u1", "DB.TABLE1", c1);
        columnACL.add("u2", "DB.TABLE1", c1);
        columnACL.add("u2", "DB.TABLE2", c1);
        columnACL.add("u2", "DB.TABLE3", c1);
        columnACL.add("u3", "DB.TABLE3", c1);

        columnACL.deleteByTbl("DB.TABLE1");

        ColumnACL expected = new ColumnACL();
        expected.add("u2", "DB.TABLE2", c1);
        expected.add("u2", "DB.TABLE3", c1);
        expected.add("u3", "DB.TABLE3", c1);
        Assert.assertEquals(expected, columnACL);
    }

    @Test
    public void testColumnACL() {
        ColumnACL empty = new ColumnACL();
        try {
            empty.delete("a", "DB.TABLE1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:a is not found in column black list", e.getMessage());
        }

        //add
        ColumnACL columnACL = new ColumnACL();
        Set<String> c1 = Sets.newHashSet("C2", "C3");
        columnACL.add("user1", "DB.TABLE1", c1);
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().size());

        //add duplicated
        try {
            columnACL.add("user1", "DB.TABLE1", c1);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:user1 already in table's columns blacklist!", e.getMessage());
        }

        //add null column list
        columnACL.add("user1", "DB.TABLE2", new TreeSet<String>());
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().size());

        //add different table column list
        columnACL.add("user2", "DB.TABLE2", c1);
        Assert.assertEquals(2, columnACL.getUserColumnBlackList().size());

        //add different user column list
        Set<String> c2 = Sets.newHashSet("C2", "C3");
        columnACL.add("user1", "DB.TABLE2", c2);
        Assert.assertEquals(2, columnACL.getUserColumnBlackList().get("user1").size());

        //update
        Set<String> c3 = Sets.newHashSet("C3");
        columnACL.update("user1", "DB.TABLE2", c3);
        Assert.assertTrue(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTbl("DB.TABLE2").equals(c3));

        //update
        columnACL.update("user1", "DB.TABLE2", new TreeSet<String>());
        Assert.assertTrue(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTbl("DB.TABLE2").equals(c3));

        //delete
        columnACL.delete("user1", "DB.TABLE2");
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().get("user1").size());
        Assert.assertNull(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTbl("DB.TABLE2"));
        Assert.assertNotNull(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTbl("DB.TABLE1"));

        //delete
        columnACL.delete("user1");
        Assert.assertNull(columnACL.getUserColumnBlackList().get("user1"));
    }
}
