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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class RowACLTest {

    @Test
    public void testRowACL() {
        RowACL empty = new RowACL();
        try {
            empty.delete("u", "t");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, user:u not have any row acl conds!", e.getMessage());
        }

        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("col1", "varchar(256)");
        columnWithType.put("col11", "varchar(256)");
        columnWithType.put("col2", "varchar(256)");
        columnWithType.put("col3", "varchar(256)");

        //add
        RowACL rowACL = new RowACL();
        Map<String, List<String>> condsWithColumn1 = new HashMap<>();
        List<String> cond1 = new ArrayList<>();
        List<String> cond11 = new ArrayList<>();
        cond1.add("a");
        cond1.add("b");
        cond1.add("c");
        cond11.add("d");
        cond11.add("e");
        condsWithColumn1.put("col1", cond1);
        condsWithColumn1.put("col11", cond11);
        rowACL.add("user1", "DB.table1", condsWithColumn1, columnWithType);
        Assert.assertEquals(2, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table1").size());
        Assert.assertEquals(3, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table1").getCondsByColumn("col1").size());
        Assert.assertEquals(2, rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table1").split("AND").length);
        //will pass in local and github, but failed in ci, cuz the order between left and right is random
        //Assert.assertEquals("col11=d OR col11=e AND col1=a OR col1=b OR col1=c", rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table1"));

        //add duplicated
        try {
            Map<String, List<String>> condsWithColumn2 = new HashMap<>();
            List<String> cond2 = new ArrayList<>();
            cond2.add("a");
            condsWithColumn1.put("col2", cond2);
            rowACL.add("user1", "DB.table1", condsWithColumn2, columnWithType);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Operation fail, user:user1, table:DB.table1 already in row cond list!");
        }

        //add different table's column cond list
        Map<String, List<String>> condsWithColumn3 = new HashMap<>();
        List<String> cond3 = new ArrayList<>();
        cond3.add("a");
        cond3.add("b");
        condsWithColumn3.put("col2", cond3);
        rowACL.add("user1", "DB.table2", condsWithColumn3, columnWithType);
        Assert.assertEquals(3, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table1").size());
        Assert.assertEquals(2, rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table1").split("AND").length);
        Assert.assertEquals("col2='a' OR col2='b'", rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table2"));

        //add different user row
        Map<String, List<String>> condsWithColumn4 = new HashMap<>();
        List<String> cond4 = new ArrayList<>();
        cond4.add("c");
        condsWithColumn4.put("col2", cond4);
        rowACL.add("user2", "DB2.table2", condsWithColumn4, columnWithType);
        Assert.assertEquals(1, rowACL.getTableRowCondsWithUser().get("user2").size());
        Assert.assertEquals(cond4, rowACL.getTableRowCondsWithUser().get("user2").getRowCondListByTable("DB2.table2").getCondsByColumn("col2"));
        Assert.assertEquals("col2='c'", rowACL.getQueryUsedConds().get("user2").getSplicedCondsByTable("DB2.table2"));

        //update
        Map<String, List<String>> condsWithColumn5 = new HashMap<>();
        List<String> cond5 = new ArrayList<>();
        cond5.add("f");
        cond5.add("ff");
        condsWithColumn5.put("col2", cond5);
        rowACL.update("user1", "DB.table2", condsWithColumn5, columnWithType);
        Assert.assertEquals(cond5, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table2").getCondsByColumn("col2"));
        Assert.assertEquals("col2='f' OR col2='ff'", rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table2"));

        //delete
        rowACL.delete("user1", "DB.table2");
        Assert.assertNotNull(rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table1"));
        Assert.assertEquals(0, rowACL.getTableRowCondsWithUser().get("user1").getRowCondListByTable("DB.table2").size());
        Assert.assertNotNull(rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table1"));
        Assert.assertNull(rowACL.getQueryUsedConds().get("user1").getSplicedCondsByTable("DB.table2"));
    }

}
