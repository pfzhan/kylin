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

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;

public class FavoriteRuleManagerTest extends NLocalFileMetadataTestCase {
    private static String PROJECT = "default";
    private static FavoriteRuleManager manager;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        createTestMetadata();
        manager = FavoriteRuleManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
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
