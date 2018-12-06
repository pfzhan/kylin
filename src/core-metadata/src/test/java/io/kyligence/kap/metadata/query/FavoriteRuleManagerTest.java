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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class FavoriteRuleManagerTest extends NLocalFileMetadataTestCase {
    private static String PROJECT = "default";
    private static FavoriteRuleManager manager;

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
    public void testBasics() throws IOException {
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
        Assert.assertEquals(3, manager.getAllEnabled().size());
    }

    @Test
    public void testAppendSqls() throws IOException, FavoriteRuleManager.RuleConditionExistException {
        // add two sqls to whitelist
        FavoriteRule.SQLCondition sqlCondition1 = new FavoriteRule.SQLCondition("test_sql1", "test_sql1".hashCode(), false);
        FavoriteRule.SQLAdvice sqlAdvice = new FavoriteRule.SQLAdvice("table not found", "please load table xx");
        sqlCondition1.setSqlAdvices(new HashSet<FavoriteRule.SQLAdvice>(){{add(sqlAdvice);}});
        FavoriteRule.SQLCondition sqlCondition2 = new FavoriteRule.SQLCondition("test_sql2", "test_sql2".hashCode(), false);
        manager.appendSqlConditions(Lists.newArrayList(sqlCondition1, sqlCondition2), FavoriteRule.WHITELIST_NAME);

        FavoriteRule whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(4, whitelist.getConds().size());
        FavoriteRule.SQLCondition firstAppendSql = null;
        for (FavoriteRule.AbstractCondition condition : whitelist.getConds()) {
            FavoriteRule.SQLCondition sqlCondition = (FavoriteRule.SQLCondition) condition;
            if (sqlCondition.getSql().equals("test_sql1"))
                firstAppendSql = sqlCondition;
        }

        Assert.assertEquals(1, firstAppendSql.getSqlAdvices().size());
        Assert.assertEquals("table not found", firstAppendSql.getSqlAdvices().iterator().next().getIncapableReason());
        Assert.assertEquals("please load table xx", firstAppendSql.getSqlAdvices().iterator().next().getSuggestion());

        // add the two sqls to blacklist
        sqlCondition1 = new FavoriteRule.SQLCondition("test_sql3", "test_sql3".hashCode(), false);
        sqlCondition2 = new FavoriteRule.SQLCondition("test_sql4", "test_sql4".hashCode(), false);
        manager.appendSqlConditions(Lists.newArrayList(sqlCondition1, sqlCondition2), FavoriteRule.BLACKLIST_NAME);
        FavoriteRule blacklist = manager.getByName(FavoriteRule.BLACKLIST_NAME);
        Assert.assertEquals(3, blacklist.getConds().size());

        // append an existing sql to whitelist
        FavoriteRule.SQLCondition existSql = new FavoriteRule.SQLCondition("select * from test_account", 722730360, false);
        manager.appendSqlConditions(Lists.newArrayList(existSql), FavoriteRule.WHITELIST_NAME);
        whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(4, whitelist.getConds().size());

        // append a blacklist sql to whitelist
        FavoriteRule.SQLCondition blacklistSql = new FavoriteRule.SQLCondition("select * from test_country", -410461279, false);
        try {
            manager.appendSqlConditions(Lists.newArrayList(blacklistSql), FavoriteRule.WHITELIST_NAME);
        } catch (Throwable ex) {
            Assert.assertEquals(FavoriteRuleManager.RuleConditionExistException.class, ex.getClass());
        }

        // append a whitelist sql to blacklist
        try {
            manager.appendSqlConditions(Lists.newArrayList(existSql), FavoriteRule.BLACKLIST_NAME);
        } catch (Throwable ex) {
            Assert.assertEquals(FavoriteRuleManager.RuleConditionExistException.class, ex.getClass());
        }
    }

    @Test
    public void testRemoveSql() throws IOException, FavoriteRuleManager.RuleConditionExistException {
        // first append a new sql
        FavoriteRule.SQLCondition newSql = new FavoriteRule.SQLCondition("new_sql", "new_sql".hashCode(), false);
        manager.appendSqlConditions(Lists.newArrayList(newSql), FavoriteRule.WHITELIST_NAME);
        FavoriteRule whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(3, whitelist.getConds().size());

        // remove new added sql
        manager.removeSqlCondition(newSql.getId(), FavoriteRule.WHITELIST_NAME);
        whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(2, whitelist.getConds().size());
    }

    @Test
    public void testUpdateWhitelistSql() throws IOException, FavoriteRuleManager.RuleConditionExistException {
        // first append a new sql
        FavoriteRule.SQLCondition newSql = new FavoriteRule.SQLCondition("new_sql", "new_sql".hashCode(), false);
        manager.appendSqlConditions(Lists.newArrayList(newSql), FavoriteRule.WHITELIST_NAME);
        FavoriteRule whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(3, whitelist.getConds().size());

        // update new added sql
        FavoriteRule.SQLCondition updatedSql = new FavoriteRule.SQLCondition("updated_sql", "updated_sql".hashCode(), false);
        updatedSql.setId(newSql.getId());
        manager.updateWhitelistSql(updatedSql);
        whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(3, whitelist.getConds().size());
        Assert.assertEquals("updated_sql", ((FavoriteRule.SQLCondition) whitelist.getConds().get(2)).getSql());

        // when sql condition not found
        updatedSql.setId("not_exist_id");
        FavoriteRule.SQLCondition result = manager.updateWhitelistSql(updatedSql);
        Assert.assertNull(result);

        // when sql already exists
        updatedSql.setSql("new_sql");
        result = manager.updateWhitelistSql(updatedSql);
        Assert.assertNull(result);

        // update a sql already in blacklist
        FavoriteRule.SQLCondition blacklistSql = new FavoriteRule.SQLCondition("select * from test_country", -410461279, false);
        blacklistSql.setId(newSql.getId());
        try {
            manager.updateWhitelistSql(blacklistSql);
        } catch (Throwable ex) {
            Assert.assertEquals(FavoriteRuleManager.RuleConditionExistException.class, ex.getClass());
        }
    }

    @Test
    public void testAppendSimilarSqlConditions() throws IOException, FavoriteRuleManager.RuleConditionExistException {
        int originWhitelistSqlSize = manager.getByName(FavoriteRule.WHITELIST_NAME).getConds().size();
        String similarSql1 = "   select   \u001C\u001D\n  sum(price)\nfrom\n  KYLIN_SALES\nLIMIT\n\t\f  500\n ;";
        String similarSql2 = "\n select sum(price)\u001E\u001F from KYLIN_SALES limit 500;";
        FavoriteRule.SQLCondition sqlCondition1 = new FavoriteRule.SQLCondition(similarSql1, similarSql1.hashCode(), true);
        FavoriteRule.SQLCondition sqlCondition2 = new FavoriteRule.SQLCondition(similarSql2, similarSql2.hashCode(), true);

        manager.appendSqlConditions(Lists.newArrayList(sqlCondition1, sqlCondition2), FavoriteRule.WHITELIST_NAME);

        // not loaded to whitelist
        Assert.assertEquals(originWhitelistSqlSize + 1, manager.getByName(FavoriteRule.WHITELIST_NAME).getConds().size());
    }

    @Test
    public void testUpdateSimilarSqlToWhitelist() throws IOException, FavoriteRuleManager.RuleConditionExistException {
        FavoriteRule whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        String originSql = ((FavoriteRule.SQLCondition) whitelist.getConds().get(0)).getSql();

        // the sql already in whitelist is "select * from test_account"
        String similarSqlInWhitelist = "  select  \n *\n from\n TEST_ACCOUNT\n\t\f ;\n\u001C\u001D\u001E\u001F";

        FavoriteRule.SQLCondition sqlCondition = manager.updateWhitelistSql(new FavoriteRule.SQLCondition(similarSqlInWhitelist, similarSqlInWhitelist.hashCode(), true));
        Assert.assertNull(sqlCondition);

        // update failed
        whitelist = manager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(originSql, ((FavoriteRule.SQLCondition) whitelist.getConds().get(0)).getSql());
    }
}
