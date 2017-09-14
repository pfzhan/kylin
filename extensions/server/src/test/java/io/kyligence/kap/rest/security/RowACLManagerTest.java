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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.acl.RowACLManager;
import io.kyligence.kap.rest.util.MultiNodeManagerTestBase;

public class RowACLManagerTest extends MultiNodeManagerTestBase {

    @Test
    public void test() throws Exception {
        final RowACLManager rowACLManagerA = new RowACLManager(configA);
        final RowACLManager rowACLManagerB = new RowACLManager(configB);
        Map<String, List<String>> condsWithColumn = new HashMap<>();
        List<String> cond1 = Lists.newArrayList("a", "b", "c");
        List<String> cond2 = Lists.newArrayList("d", "e");
        condsWithColumn.put("COUNTRY", cond1);
        condsWithColumn.put("NAME", cond2);


        Assert.assertTrue(rowACLManagerB.getRowACLByCache(PROJECT).getRowCondListByTable(TABLE).isEmpty());
        rowACLManagerA.addRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY", condsWithColumn);
        // if not sleep, manager B's get method is faster than notify
        Thread.sleep(1000);
        Assert.assertTrue(rowACLManagerB.getRowACLByCache(PROJECT).getRowCondListByTable("DEFAULT.TEST_COUNTRY").containsKey(USER));

        rowACLManagerB.deleteRowACL(PROJECT, USER, "DEFAULT.TEST_COUNTRY");
        Thread.sleep(1000);
        Assert.assertEquals(0, rowACLManagerA.getRowACLByCache(PROJECT).getRowCondListByTable("DEFAULT.TEST_COUNTRY").size());
    }
}
