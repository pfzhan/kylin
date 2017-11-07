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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.metadata.acl.RowACLManager;
import io.kyligence.kap.rest.util.MultiNodeManagerTestBase;

public class RowACLManagerTest extends MultiNodeManagerTestBase {
    @Test
    public void testCaseInsensitiveFromDeserializer() throws IOException {
        final RowACLManager manager = new RowACLManager(configA);
        RowACL.ColumnToConds columnToConds = getColumnToConds();
        manager.addRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY", columnToConds, MetadataConstants.TYPE_USER);
        RowACL rowACL = Preconditions.checkNotNull(getStore().getResource("/row_acl/" + PROJECT, RowACL.class, new JsonSerializer<>(RowACL.class)));
        Map<String, RowACL.ColumnToConds> columnToCondsWithUser = rowACL.getColumnToCondsByTable("default.test_country", MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, columnToCondsWithUser.size());
        Assert.assertEquals(2, columnToCondsWithUser.get(USER).getCondsByColumn("name").size());
    }

    @Test
    public void test() throws Exception {
        final RowACLManager rowACLManagerA = new RowACLManager(configA);
        final RowACLManager rowACLManagerB = new RowACLManager(configB);
        RowACL.ColumnToConds columnToConds = getColumnToConds();

        Assert.assertTrue(rowACLManagerB.getRowACLByCache(PROJECT).getColumnToCondsByTable("DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER).isEmpty());
        rowACLManagerA.addRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY", columnToConds, MetadataConstants.TYPE_USER);
        // if not sleep, manager B's get method is faster than notify
        Thread.sleep(1000);
        Assert.assertTrue(rowACLManagerB.getRowACLByCache(PROJECT).getColumnToCondsByTable("DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER).containsKey(USER));

        rowACLManagerB.deleteRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER);
        Thread.sleep(1000);
        Assert.assertTrue(rowACLManagerA.getRowACLByCache(PROJECT).getColumnToCondsByTable("DEFAULT.TEST_COUNTRY", MetadataConstants.TYPE_USER).isEmpty());
    }

    private RowACL.ColumnToConds getColumnToConds() {
        Map<String, List<RowACL.Cond>> colToConds = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"), new RowACL.Cond("c"));
        List<RowACL.Cond> cond2 = Lists.newArrayList(new RowACL.Cond("d"), new RowACL.Cond("e"));
        colToConds.put("COUNTRY", cond1);
        colToConds.put("NAME", cond2);
        return new RowACL.ColumnToConds(colToConds);
    }
}
