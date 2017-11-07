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

package io.kyligence.kap.rest.security;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.acl.ColumnACL;
import io.kyligence.kap.metadata.acl.ColumnACLManager;
import io.kyligence.kap.rest.util.MultiNodeManagerTestBase;

public class ColumnACLManagerTest extends MultiNodeManagerTestBase {
    @Test
    public void testCaseInsensitiveFromDeserializer() throws IOException {
        final ColumnACLManager manager = new ColumnACLManager(configA);
        manager.addColumnACL(PROJECT, USER, "TABLE1", Sets.newHashSet("c1", "c2"), MetadataConstants.TYPE_USER);
        ColumnACL columnACL = Preconditions.checkNotNull(getStore().getResource("/column_acl/" + PROJECT, ColumnACL.class, new JsonSerializer<>(ColumnACL.class)));
        Map<String, Set<String>> columnBlackListByTable = columnACL.getColumnBlackListByTable("table1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, columnBlackListByTable.size());
        Assert.assertTrue(columnBlackListByTable.get("u1").contains("C1"));
    }

    @Test
    public void test() throws Exception {
        final ColumnACLManager columnACLManagerA = new ColumnACLManager(configA);
        final ColumnACLManager columnACLManagerB = new ColumnACLManager(configB);

        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackList(USER, EMPTY_GROUP_SET).isEmpty());
        columnACLManagerA.addColumnACL(PROJECT, USER, TABLE, Sets.newHashSet("c1", "c2"), MetadataConstants.TYPE_USER);
        // if not sleep, manager B's get method is faster than notify
        Thread.sleep(1000);
        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackList(USER, EMPTY_GROUP_SET).contains("T1.C1"));
        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackList(USER, EMPTY_GROUP_SET).contains("T1.C2"));
        columnACLManagerB.deleteColumnACL(PROJECT, USER, TABLE, MetadataConstants.TYPE_USER);
        Thread.sleep(1000);
        Assert.assertEquals(0, columnACLManagerA.getColumnACLByCache(PROJECT).getColumnBlackList(USER, EMPTY_GROUP_SET).size());

    }
}
