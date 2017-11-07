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
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Sets;

public class ColumnACLServiceTest extends ServiceTestBase {
    private final static String PROJECT = "learn_kylin";

    @Autowired
    @Qualifier("ColumnAclService")
    private ColumnACLService columnACLService;

    @Test
    public void testColumnACL() throws IOException {
        //test add and get
        Set<String> columns = Sets.newHashSet("C1", "C2", "C3", "C4", "C5");
        Set<String> columns2 =Sets.newHashSet("C1") ;
        Set<String> columns3 = Sets.newHashSet("C1");

        columnACLService.addToColumnACL(PROJECT, "ADMIN", "DB.TABLE", columns, MetadataConstants.TYPE_USER);
        columnACLService.addToColumnACL(PROJECT, "MODELER", "DB.TABLE1", columns2, MetadataConstants.TYPE_USER);
        columnACLService.addToColumnACL(PROJECT, "ANALYST", "DB.TABLE", columns3, MetadataConstants.TYPE_USER);
        Map<String, Set<String>> userWithBlackColumn = columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE", MetadataConstants.TYPE_USER);
        Assert.assertEquals(5, userWithBlackColumn.get("ADMIN").size());
        Assert.assertEquals(1, userWithBlackColumn.get("ANALYST").size());
        Assert.assertNull(userWithBlackColumn.get("MODELER"));

        //test update
        Set<String> columns4 = Sets.newHashSet("C6");

        columnACLService.updateColumnACL(PROJECT, "ANALYST", "DB.TABLE", columns4, MetadataConstants.TYPE_USER);
        Map<String, Set<String>> userWithBlackColumn1 = columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE", MetadataConstants.TYPE_USER);
        Assert.assertTrue(userWithBlackColumn1.get("ANALYST").equals(columns4));

        //test delete
        columnACLService.deleteFromColumnACL(PROJECT, "ANALYST", "DB.TABLE", MetadataConstants.TYPE_USER);
        Map<String, Set<String>> userWithBlackColumn2 = columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE", MetadataConstants.TYPE_USER);
        Assert.assertNull(userWithBlackColumn2.get("ANALYST"));

        //test delete
        Assert.assertEquals(1, columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE1", MetadataConstants.TYPE_USER).size());
        columnACLService.deleteFromColumnACL(PROJECT, "MODELER", MetadataConstants.TYPE_USER);
        Assert.assertEquals(0, columnACLService.getColumnBlackListByTable(PROJECT, "DB.TABLE1", MetadataConstants.TYPE_USER).size());
    }
}
