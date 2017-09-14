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

import io.kyligence.kap.rest.util.MultiNodeManagerTestBase;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class ColumnACLManagerTest extends MultiNodeManagerTestBase {

    @Test
    public void test() throws Exception {
        final ColumnACLManager columnACLManagerA = new ColumnACLManager(configA);
        final ColumnACLManager columnACLManagerB = new ColumnACLManager(configB);

        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackListByUser(USER).isEmpty());
        columnACLManagerA.addColumnACL(PROJECT, USER, TABLE, Sets.newHashSet("c1", "c2"));
        // if not sleep, manager B's get method is faster than notify
        Thread.sleep(1000);
        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackListByUser(USER).contains("T1.C1"));
        Assert.assertTrue(columnACLManagerB.getColumnACLByCache(PROJECT).getColumnBlackListByUser(USER).contains("T1.C2"));
        columnACLManagerB.deleteColumnACL(PROJECT, USER, TABLE);
        Thread.sleep(1000);
        Assert.assertEquals(0, columnACLManagerA.getColumnACLByCache(PROJECT).getColumnBlackListByUser(USER).size());

    }
}
