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

package org.apache.kylin.metadata.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Lists;

public class FavoriteRuleManagerTest extends NLocalFileMetadataTestCase {
    private static String PROJECT = "default";
    private static FavoriteRuleManager manager;
    private JdbcTemplate jdbcTemplate;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        manager = FavoriteRuleManager.getInstance(PROJECT);
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        createRules();
    }

    @After
    public void cleanUp() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        cleanupTestMetadata();
    }

    private void createRules() {
        FavoriteRule.SQLCondition cond1 = new FavoriteRule.SQLCondition("1", "SELECT *\nFROM \"TEST_KYLIN_FACT\"");
        FavoriteRule.Condition cond2 = new FavoriteRule.Condition(null, "10");
        FavoriteRule.Condition cond3 = new FavoriteRule.Condition(null, "ROLE_ADMIN");
        FavoriteRule.Condition cond4 = new FavoriteRule.Condition(null, "userA");
        FavoriteRule.Condition cond5 = new FavoriteRule.Condition(null, "userB");
        FavoriteRule.Condition cond6 = new FavoriteRule.Condition(null, "userC");
        FavoriteRule.Condition cond7 = new FavoriteRule.Condition("5", "8");
        manager.createRule(new FavoriteRule(Collections.singletonList(cond1), "blacklist", false));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond2), "count", true));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond3), "submitter_group", true));
        manager.createRule(new FavoriteRule(Arrays.asList(cond4, cond5, cond6), "submitter", true));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond7), "duration", true));
    }

    @Test
    public void testBasics() {
        List<FavoriteRule> rules = manager.getAll();
        Assert.assertEquals(5, rules.size());

        FavoriteRule.Condition cond1 = new FavoriteRule.Condition();
        cond1.setRightThreshold("100");

        FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
        cond2.setRightThreshold("4");

        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList(cond1, cond2);
        FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);

        manager.createRule(newRule);
        Assert.assertEquals(6, manager.getAll().size());

        // create duplicate rule
        manager.createRule(new FavoriteRule(Lists.newArrayList(), "new_rule", true));
        Assert.assertEquals(6, manager.getAll().size());

        cond1.setLeftThreshold("10");
        conds = Lists.newArrayList(cond1, cond2);
        newRule.setConds(conds);
        manager.updateRule(conds, true, newRule.getName());

        Assert.assertEquals(6, manager.getAll().size());
        FavoriteRule updatedNewRule = manager.getByName("new_rule");
        conds = updatedNewRule.getConds();
        Assert.assertEquals(2, conds.size());
        Assert.assertEquals("10", ((FavoriteRule.Condition) conds.get(0)).getLeftThreshold());
    }

    @Test
    public void testGetEnabledRules() {
        Assert.assertEquals(4, manager.getAllEnabled().size());
    }
}
