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

package io.kyligence.kap.metadata.favorite;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccelerateRuleUtilTest extends NLocalFileMetadataTestCase {

    private static String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testCustomerRule() {
        AccelerateRuleUtil accelerateRuleUtil = new AccelerateRuleUtil();

        QueryHistory qh1 = new QueryHistory();
        qh1.setQuerySubmitter("OTHER");
        qh1.setDuration(500L);
        QueryHistory qh2 = new QueryHistory();
        qh2.setQuerySubmitter("OTHER");
        qh2.setDuration(1500L);
        QueryHistory qh3 = new QueryHistory();
        qh3.setQuerySubmitter("OTHER");
        qh3.setDuration(2500L);
        QueryHistory qh4 = new QueryHistory();
        qh4.setQuerySubmitter("ADMIN");
        qh4.setDuration(500L);
        QueryHistory qh5 = new QueryHistory();
        qh5.setQuerySubmitter("ADMIN");
        qh5.setDuration(1500L);
        QueryHistory qh6 = new QueryHistory();
        qh6.setQuerySubmitter("OTHER2");
        qh6.setDuration(3000L);

        FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition("1", "2")), true, FavoriteRule.DURATION_RULE_NAME);

        FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition(null, "ADMIN")), true, FavoriteRule.SUBMITTER_RULE_NAME);

        FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition(null, "ROLE_ADMIN")), true,
                FavoriteRule.SUBMITTER_GROUP_RULE_NAME);

        Set<String> group = new HashSet<>();
        group.add("ROLE_ADMIN");
        Set<String> group2 = new HashSet<>();
        group2.add("ALL_USERS");
        Map<String, Set<String>> submitterToGroups = new HashMap<>();
        submitterToGroups.put("OTHER2", group);
        submitterToGroups.put("OTHER", group2);

        // normal user need meet duration rule
        Assert.assertEquals(false, accelerateRuleUtil.matchCustomerRule(qh1, PROJECT, submitterToGroups));
        Assert.assertEquals(true, accelerateRuleUtil.matchCustomerRule(qh2, PROJECT, submitterToGroups));
        Assert.assertEquals(false, accelerateRuleUtil.matchCustomerRule(qh3, PROJECT, submitterToGroups));

        // ADMIN user always true
        Assert.assertEquals(true, accelerateRuleUtil.matchCustomerRule(qh4, PROJECT, submitterToGroups));
        Assert.assertEquals(true, accelerateRuleUtil.matchCustomerRule(qh5, PROJECT, submitterToGroups));

        // normal user need meet submitter group rule
        Assert.assertEquals(true, accelerateRuleUtil.matchCustomerRule(qh6, PROJECT, submitterToGroups));
        // user group to which the user belongs is not specified in rule
        Assert.assertEquals(false, accelerateRuleUtil.matchCustomerRule(qh1, PROJECT, submitterToGroups));
    }

}