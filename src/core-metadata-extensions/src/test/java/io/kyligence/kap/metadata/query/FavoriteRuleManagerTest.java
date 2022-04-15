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

package io.kyligence.kap.metadata.query;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;

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
