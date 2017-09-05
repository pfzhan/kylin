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
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import io.kyligence.kap.metadata.acl.ColumnACL;

public class ColumnACLServiceTest extends ServiceTestBase {
    private final static String PROJECT = "learn_kylin";

    @Autowired
    @Qualifier("ColumnAclService")
    private ColumnACLService columnACLService;

    @Test
    public void testColumnACL() throws IOException {
        ColumnACL emptyBlackList = columnACLService.getColumnBlackListByProject(PROJECT);
        Assert.assertEquals(0, emptyBlackList.getUserColumnBlackList().size());

        //test add and get
        List<String> columns = new ArrayList<>();
        List<String> columns2 = new ArrayList<>();
        List<String> columns3 = new ArrayList<>();
        columns.add("c1");
        columns.add("c2");
        columns.add("c3");
        columns.add("c4");
        columns.add("c5");
        columns2.add("c1");
        columns3.add("c1");

        columnACLService.addToColumnBlackList(PROJECT, "ADMIN", "DB.TABLE", columns);
        columnACLService.addToColumnBlackList(PROJECT, "MODELER", "DB.TABLE1", columns2);
        columnACLService.addToColumnBlackList(PROJECT, "ANALYST", "DB.TABLE", columns3);
        Map<String, List<String>> userWithBlackColumn = columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE");
        Assert.assertEquals(5, userWithBlackColumn.get("ADMIN").size());
        Assert.assertEquals(1, userWithBlackColumn.get("ANALYST").size());
        Assert.assertNull(userWithBlackColumn.get("MODELER"));

        //test update
        List<String> columns4 = new ArrayList<>();
        columns4.add("c6");
        columnACLService.updateColumnBlackList(PROJECT, "ANALYST", "DB.TABLE", columns4);
        Map<String, List<String>> userWithBlackColumn1 = columnACLService.getColumnBlackListByTable(PROJECT,
                "DB.TABLE");
        Assert.assertTrue(userWithBlackColumn1.get("ANALYST").equals(columns4));

        //test delete
        columnACLService.deleteFromTableBlackList(PROJECT, "ANALYST", "DB.TABLE");
        Map<String, List<String>> userWithBlackColumn2 = columnACLService.getColumnBlackListByTable(PROJECT,
                "DB.TABLE");
        Assert.assertNull(userWithBlackColumn2.get("ANALYST"));

    }

}
