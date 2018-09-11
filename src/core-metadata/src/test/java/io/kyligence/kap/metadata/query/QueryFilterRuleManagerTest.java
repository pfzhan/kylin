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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class QueryFilterRuleManagerTest extends NLocalFileMetadataTestCase {
    private static String PROJECT = "default";
    private static QueryFilterRuleManager manager;

    @Before
    public void setUp() {
        createTestMetadata();
        manager = QueryFilterRuleManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        List<QueryFilterRule> rules = manager.getAll();
        Assert.assertEquals(1, rules.size());
        Assert.assertEquals("test", rules.get(0).getName());
        Assert.assertEquals(5, rules.get(0).getConds().size());
        Assert.assertTrue(rules.get(0).isEnabled());

        QueryFilterRule.QueryHistoryCond cond1 = new QueryFilterRule.QueryHistoryCond();
        cond1.setOp(QueryFilterRule.QueryHistoryCond.Operation.TO);
        cond1.setField("startTime");
        cond1.setLeftThreshold("0");
        cond1.setRightThreshold(String.valueOf(System.currentTimeMillis()));

        QueryFilterRule.QueryHistoryCond cond2 = new QueryFilterRule.QueryHistoryCond();
        cond2.setOp(QueryFilterRule.QueryHistoryCond.Operation.LESS);
        cond2.setField("latency");
        cond2.setRightThreshold("1000");

        QueryFilterRule.QueryHistoryCond cond3 = new QueryFilterRule.QueryHistoryCond();
        cond3.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond3.setField("accelerateStatus");
        cond3.setRightThreshold("WAITING");

        QueryFilterRule.QueryHistoryCond cond4 = new QueryFilterRule.QueryHistoryCond();
        cond4.setOp(QueryFilterRule.QueryHistoryCond.Operation.CONTAIN);
        cond4.setField("sql");
        cond4.setRightThreshold("test_table_1");

        QueryFilterRule.QueryHistoryCond cond5 = new QueryFilterRule.QueryHistoryCond();
        cond5.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond5.setField("frequency");
        cond5.setRightThreshold("4");

        List<QueryFilterRule.QueryHistoryCond> conds = Lists.newArrayList(cond1, cond2, cond3, cond4, cond5);
        QueryFilterRule newRule = new QueryFilterRule(conds, "new_rule", true);

        manager.save(newRule);
        Assert.assertEquals(2, manager.getAll().size());

        manager.delete(newRule);
        rules = manager.getAll();
        Assert.assertEquals(1, rules.size());
        Assert.assertEquals("test", rules.get(0).getName());
    }

    @Test
    public void testGetRule() {
        QueryFilterRule rule = manager.get("30a73dc4-b1b6-4744-a598-5735f52c249b");
        Assert.assertNotNull(rule);

        rule = manager.get("not_exist_rule");
        Assert.assertNull(rule);
    }
}
