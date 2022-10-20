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

package org.apache.kylin.metadata.favorite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.query.QueryHistory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

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

        FavoriteRuleManager.getInstance(PROJECT).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition("1", "2")), true, FavoriteRule.DURATION_RULE_NAME);

        FavoriteRuleManager.getInstance(PROJECT).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition(null, "ADMIN")), true, FavoriteRule.SUBMITTER_RULE_NAME);

        FavoriteRuleManager.getInstance(PROJECT).updateRule(
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
