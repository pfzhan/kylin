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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ColumnACLTest {

    @Test
    public void testColumnACL() {
        ColumnACL empty = new ColumnACL();
        try {
            empty.delete("a", "b");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:a is not found in column black list", e.getMessage());
        }

        //add
        ColumnACL columnACL = new ColumnACL();
        List<String> c1 = new ArrayList<>();
        c1.add("c2");
        c1.add("c2");
        c1.add("c3");
        columnACL.add("user1", "DB.table1", c1);
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().size());

        //add duplicated
        try {
            columnACL.add("user1", "DB.table1", new ArrayList<String>());
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:user1 already in this table's columns blacklist!", e.getMessage());
        }

        //add null column list
        columnACL.add("user1", "DB.table2", new ArrayList<String>());
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().size());

        //add different table column list
        columnACL.add("user2", "DB.table2", c1);
        Assert.assertEquals(2, columnACL.getUserColumnBlackList().size());

        //add different user column list
        List<String> c2 = new ArrayList<>();
        c2.add("c2");
        c2.add("c2");
        c2.add("c3");
        columnACL.add("user1", "DB.table2", c2);
        Assert.assertEquals(2, columnACL.getUserColumnBlackList().get("user1").size());

        //update
        List<String> c3 = new ArrayList<>();
        c3.add("c3");
        columnACL.update("user1", "DB.table2", c3);
        Assert.assertTrue(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTable("DB.table2").equals(c3));

        //update
        columnACL.update("user1", "DB.table2", new ArrayList<String>());
        Assert.assertTrue(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTable("DB.table2").equals(c3));

        //delete
        columnACL.delete("user1", "DB.table2");
        Assert.assertEquals(1, columnACL.getUserColumnBlackList().get("user1").size());
        Assert.assertNull(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTable("DB.table2"));
        Assert.assertNotNull(columnACL.getUserColumnBlackList().get("user1").getColumnBlackListByTable("DB.table1"));
    }
}
