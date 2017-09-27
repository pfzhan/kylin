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

import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.LEFT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.RowACL.Cond.IntervalType.RIGHT_INCLUSIVE;
import static io.kyligence.kap.metadata.acl.RowACLManager.concatConds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.metadata.acl.RowACLManager;
import io.kyligence.kap.rest.util.MultiNodeManagerTestBase;

public class RowACLManagerTest extends MultiNodeManagerTestBase {

    @Test
    public void testConcatConds() {
        Map<String, List<RowACL.Cond>> condsWithCol = new HashMap<>();
        Map<String, String> columnWithType = new HashMap<>();
        columnWithType.put("COL1", "varchar(256)");
        columnWithType.put("COL2", "timestamp");
        columnWithType.put("COL3", "int");
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("a'b"));
        List<RowACL.Cond> cond6 = Lists.newArrayList(new RowACL.Cond(LEFT_INCLUSIVE, "1505275932000", "1506321155000")); //timestamp
        List<RowACL.Cond> cond7 = Lists.newArrayList(new RowACL.Cond(RIGHT_INCLUSIVE, "7", "100")); //normal type
        condsWithCol.put("COL1", cond1);
        condsWithCol.put("COL2", cond6);
        condsWithCol.put("COL3", cond7);
        Assert.assertEquals(
                "(COL3>7 AND COL3<=100) AND (COL2>=TIMESTAMP '2017-09-13 04:12:12' AND COL2<TIMESTAMP '2017-09-25 06:32:35') AND ((COL1='a') OR (COL1='a''b'))",
                concatConds(condsWithCol, columnWithType));
    }

    @Test
    public void test() throws Exception {
        final RowACLManager rowACLManagerA = new RowACLManager(configA);
        final RowACLManager rowACLManagerB = new RowACLManager(configB);
        Map<String, List<RowACL.Cond>> condsWithColumn = new HashMap<>();
        List<RowACL.Cond> cond1 = Lists.newArrayList(new RowACL.Cond("a"), new RowACL.Cond("b"), new RowACL.Cond("c"));
        List<RowACL.Cond> cond2 = Lists.newArrayList(new RowACL.Cond("d"), new RowACL.Cond("e"));
        condsWithColumn.put("COUNTRY", cond1);
        condsWithColumn.put("NAME", cond2);

        Assert.assertTrue(rowACLManagerB.getRowCondListByTable(PROJECT, TABLE).isEmpty());
        rowACLManagerA.addRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY", condsWithColumn);
        // if not sleep, manager B's get method is faster than notify
        Thread.sleep(1000);
        Assert.assertTrue(rowACLManagerB.getRowCondListByTable(PROJECT, "DEFAULT.TEST_COUNTRY").containsKey(USER));

        rowACLManagerB.deleteRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY");
        Thread.sleep(1000);
        Assert.assertEquals(0, rowACLManagerA.getRowCondListByTable(PROJECT, "DEFAULT.TEST_COUNTRY").size());
    }
}
