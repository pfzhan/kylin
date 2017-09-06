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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import io.kyligence.kap.metadata.acl.RowACL;

public class RowACLServiceTest extends ServiceTestBase {
    private final static String PROJECT = "learn_kylin";

    @Autowired
    @Qualifier("RowAclService")
    private RowACLService rowACLService;

    @Test
    public void testRowACL() throws IOException {
        RowACL empty = rowACLService.getRowACL(PROJECT);
        Assert.assertEquals(0, empty.getTableRowCondsWithUser().size());

        //test add and get
        Map<String, List<String>> condsWithColumn1 = new HashMap<>();
        Map<String, List<String>> condsWithColumn3 = new HashMap<>();
        Map<String, List<String>> condsWithColumn4 = new HashMap<>();

        List<String> conds1 = new ArrayList<>();
        List<String> conds2 = new ArrayList<>();
        List<String> conds3 = new ArrayList<>();
        List<String> conds4 = new ArrayList<>();
        conds1.add("a");
        conds1.add("b");
        conds1.add("c");
        conds2.add("d");
        conds2.add("e");
        conds3.add("f");
        conds4.add("g");

        condsWithColumn1.put("COUNTRY", conds1);
        condsWithColumn1.put("NAME", conds2);
        condsWithColumn3.put("ACCOUNT_CONTACT", conds3);
        condsWithColumn4.put("LATITUDE", conds4);

        rowACLService.addToRowCondList(PROJECT, "user1", "DEFAULT.TEST_COUNTRY", condsWithColumn1);
        rowACLService.addToRowCondList(PROJECT, "user1", "DEFAULT.TEST_ACCOUNT", condsWithColumn3);
        rowACLService.addToRowCondList(PROJECT, "user2", "DEFAULT.TEST_COUNTRY", condsWithColumn4);

        Map<String, Map<String, List<String>>> columnBlackListByTable = rowACLService.getRowCondsByTable(PROJECT,
                "DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(2, columnBlackListByTable.get("user1").size());
        Assert.assertEquals(1, columnBlackListByTable.get("user2").size());
        Assert.assertEquals(conds1, columnBlackListByTable.get("user1").get("COUNTRY"));
        Assert.assertEquals(conds2, columnBlackListByTable.get("user1").get("NAME"));
        Assert.assertEquals(conds4, columnBlackListByTable.get("user2").get("LATITUDE"));

        //test add null or empty cond
        Map<String, List<String>> emptyCond = new HashMap<>();
        emptyCond.put("COL4", new ArrayList<String>());
        try {
            rowACLService.addToRowCondList(PROJECT, "user3", "DB.TABLE3", emptyCond);
        } catch (Exception e) {
            System.out.println("add empty fail");
            Assert.assertEquals("Operation fail, input condition list is empty", e.getMessage());
        }

        Map<String, List<String>> nullCond = new HashMap<>();
        nullCond.put("COL5", null);
        try {
            rowACLService.addToRowCondList(PROJECT, "user4", "DB.TABLE4", nullCond);
        } catch (Exception e) {
            System.out.println("add null fail");
            Assert.assertEquals("Operation fail, input condition list is empty", e.getMessage());
        }

        //test update
        Map<String, List<String>> condsWithColumn5 = new HashMap<>();
        List<String> conds5 = new ArrayList<>();
        conds5.add("h");
        condsWithColumn5.put("NAME", conds5);
        rowACLService.updateToRowCondList(PROJECT, "user1", "DEFAULT.TEST_COUNTRY", condsWithColumn5);
        Map<String, Map<String, List<String>>> columnBlackListByTable2 = rowACLService.getRowCondsByTable(PROJECT,
                "DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(conds5, columnBlackListByTable2.get("user1").get("NAME"));

        //test delete
        rowACLService.deleteFromRowCondList(PROJECT, "user1", "DEFAULT.TEST_COUNTRY");
        Assert.assertNull(rowACLService.getRowCondsByTable(PROJECT, "DEFAULT.TEST_COUNTRY").get("user1"));
    }

}
