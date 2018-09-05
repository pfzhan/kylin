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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.acl.ColumnToConds;
import io.kyligence.kap.rest.service.RowACLService;

public class RowACLServiceTest extends ServiceTestBase {
    private final static String PROJECT = "learn_kylin";

    @Autowired
    @Qualifier("RowAclService")
    private RowACLService rowACLService;

    @Test
    public void testRowACL() throws IOException {
        //test add and get
        Map<String, List<ColumnToConds.Cond>> condsWithColumn1 = new HashMap<>();
        Map<String, List<ColumnToConds.Cond>> condsWithColumn3 = new HashMap<>();
        Map<String, List<ColumnToConds.Cond>> condsWithColumn4 = new HashMap<>();

        List<ColumnToConds.Cond> conds1 = Lists.newArrayList(new ColumnToConds.Cond("a"), new ColumnToConds.Cond("b"), new ColumnToConds.Cond("c"));
        List<ColumnToConds.Cond> conds2 = Lists.newArrayList(new ColumnToConds.Cond("d"), new ColumnToConds.Cond("e"));
        List<ColumnToConds.Cond> conds3 = Lists.newArrayList(new ColumnToConds.Cond("f"));
        List<ColumnToConds.Cond> conds4 = Lists.newArrayList(new ColumnToConds.Cond("g"));

        condsWithColumn1.put("COUNTRY", conds1);
        condsWithColumn1.put("NAME", conds2);
        condsWithColumn3.put("ACCOUNT_CONTACT", conds3);
        condsWithColumn4.put("LATITUDE", conds4);

        ColumnToConds columnToConds1 = new ColumnToConds(condsWithColumn1);
        ColumnToConds columnToConds3 = new ColumnToConds(condsWithColumn3);
        ColumnToConds columnToConds4 = new ColumnToConds(condsWithColumn4);

        rowACLService.addToRowACL(PROJECT, "user1", "DEFAULT.TEST_COUNTRY", columnToConds1, MetadataConstants.TYPE_USER);
        rowACLService.addToRowACL(PROJECT, "user1", "DEFAULT.TEST_ACCOUNT", columnToConds3, MetadataConstants.TYPE_USER);
        rowACLService.addToRowACL(PROJECT, "user2", "DEFAULT.TEST_COUNTRY", columnToConds4, MetadataConstants.TYPE_USER);

        Map<String, ColumnToConds> columnToCondsWithUser = rowACLService.getRowACLByTable(PROJECT, "DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER);

        Assert.assertEquals(2, columnToCondsWithUser.get("user1").size());
        Assert.assertEquals(1, columnToCondsWithUser.get("user2").size());
        Assert.assertEquals(conds1, columnToCondsWithUser.get("user1").getCondsByColumn("COUNTRY"));
        Assert.assertEquals(conds2, columnToCondsWithUser.get("user1").getCondsByColumn("NAME"));
        Assert.assertEquals(conds4, columnToCondsWithUser.get("user2").getCondsByColumn("LATITUDE"));

        //test add null or empty cond
        Map<String, List<ColumnToConds.Cond>> emptyCond = new HashMap<>();
        emptyCond.put("COL4", new ArrayList<ColumnToConds.Cond>());
        ColumnToConds emptyRowCond = new ColumnToConds(emptyCond);
        try {
            rowACLService.addToRowACL(PROJECT, "user3", "DB.TABLE3", emptyRowCond, MetadataConstants.TYPE_USER);
        } catch (Exception e) {
            System.out.println("add empty fail");
            Assert.assertEquals("Operation fail, input condition list is empty", e.getMessage());
        }

        Map<String, List<ColumnToConds.Cond>> nullCond = new HashMap<>();
        nullCond.put("COL5", null);
        ColumnToConds nullRowCond = new ColumnToConds(nullCond);
        try {
            rowACLService.addToRowACL(PROJECT, "user4", "DB.TABLE4", nullRowCond, MetadataConstants.TYPE_USER);
        } catch (Exception e) {
            System.out.println("add null fail");
            Assert.assertEquals("Operation fail, input condition list is empty", e.getMessage());
        }

        //test update
        Map<String, List<ColumnToConds.Cond>> condsWithColumn5 = new HashMap<>();
        List<ColumnToConds.Cond> conds5 = Lists.newArrayList(new ColumnToConds.Cond("h"));
        condsWithColumn5.put("NAME", conds5);
        ColumnToConds columnToConds5 = new ColumnToConds(condsWithColumn5);
        rowACLService.updateRowACL(PROJECT, "user1", "DEFAULT.TEST_COUNTRY", columnToConds5, MetadataConstants.TYPE_USER);
        Map<String, ColumnToConds> columnToCondsWithUser2 = rowACLService.getRowACLByTable(PROJECT, "DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER);
        Assert.assertEquals(conds5, columnToCondsWithUser2.get("user1").getCondsByColumn("NAME"));

        //test delete
        rowACLService.deleteFromRowACL(PROJECT, "user1", "DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER);
        Assert.assertNull(rowACLService.getRowACLByTable(PROJECT, "DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER).get("user1"));

    }
}