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

package io.kyligence.kap.smart.query;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.common.AutoTestOnLearnKylinData;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.advisor.SqlSyntaxAdvisor;

public class DefaultQueryRunnerTest extends AutoTestOnLearnKylinData {

    @Test
    public void testExecute() throws Exception {
        String[] sqls = new String[] { "select sum(price * item_count), part_dt from kylin_sales group by part_dt",
                "select price, item_count, part_dt from kylin_sales" };
        AbstractQueryRunner queryRunner1 = new QueryRunnerBuilder(proj, getTestConfig(), sqls).build();
        queryRunner1.execute();
        Map<String, Collection<OLAPContext>> olapContexts = queryRunner1.getOlapContexts();
        Assert.assertEquals(2, olapContexts.size());

        Assert.assertEquals(1, olapContexts.get(sqls[0]).size());
        OLAPContext olapContext1 = olapContexts.get(sqls[0]).iterator().next();
        Assert.assertNull(olapContext1.getTopNode());
        Assert.assertNull(olapContext1.getParentOfTopNode());
        Assert.assertEquals(0, olapContext1.allOlapJoins.size());

        Assert.assertEquals(1, olapContexts.get(sqls[1]).size());
        OLAPContext olapContext2 = olapContexts.get(sqls[1]).iterator().next();
        Assert.assertNull(olapContext2.getTopNode());
        Assert.assertNull(olapContext2.getParentOfTopNode());
        Assert.assertEquals(0, olapContext2.allOlapJoins.size());
    }

    @Test
    public void testProposeWithMessage() {
        SqlSyntaxAdvisor sqlSyntaxAdvisor = new SqlSyntaxAdvisor();

        SQLResult sqlResult = new SQLResult();
        sqlResult.setMessage("Non-query expression encountered in illegal context");
        SQLAdvice advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("Not Supported SQL.", advice.getIncapableReason());

        sqlResult.setMessage("Encountered \"()\" at line 12, column 234. Was expecting one of: ");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("Syntax error occurred at line (), column 12: \"234\". Please modify it.",
                advice.getIncapableReason());

        sqlResult.setMessage("default message");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please contact Kyligence technical support for more details.", advice.getSuggestion());
        Assert.assertEquals("Something went wrong. default message", advice.getIncapableReason());

        sqlResult.setMessage(
                "From line 234, column 234 to line 23, column 234:  \"Object '234' not found( within '324')\nwhile executing SQL: \"234\"");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("The SQL has syntax error:  \"Object '234' not found( within '324') ",
                advice.getIncapableReason());
    }
}
