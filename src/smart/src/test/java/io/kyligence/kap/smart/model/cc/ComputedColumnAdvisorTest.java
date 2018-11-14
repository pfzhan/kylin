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

package io.kyligence.kap.smart.model.cc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.model.cc.ComputedColumnAdvisor.CCRuleVisitor;

public class ComputedColumnAdvisorTest {

    @Test
    public void sumCCTest() {
        String sql = "select SUM(CASE when MARA_MEDICARE_RISK_SCORE>1 then 1 else 0 end), PAYER as a from Z_PROVDASH_UM_ED group by PAYER";

        List<String> suggestion = suggestCC(sql);

        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("CASE WHEN MARA_MEDICARE_RISK_SCORE > 1 THEN 1 ELSE 0 END", suggestion.get(0));
    }

    @Test
    public void arrayItemCCTest() {
        String sql = "select ARRAY['123', '222'][1],  PAYER from Z_PROVDASH_UM_ED";

        List<String> suggestion = suggestCC(sql);

        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("ARRAY['123', '222'][1]", suggestion.get(0));
    }

    @Test
    public void duplicateCCTest() {
        String sql = "select SUM(CASE when MARA_MEDICARE_RISK_SCORE>1 then 1 else 0 end), "
                + "AVG(CASE when MARA_MEDICARE_RISK_SCORE>1 then 1 else 0 end), "
                + "PAYER as a from Z_PROVDASH_UM_ED group by PAYER";

        List<String> suggestion = suggestCC(sql);

        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("CASE WHEN MARA_MEDICARE_RISK_SCORE > 1 THEN 1 ELSE 0 END", suggestion.get(0));
    }

    @Test
    public void caseCCTest() {
        String sql = "select CASE when MARA_MEDICARE_RISK_SCORE>1 then 1 else 0 end, PAYER as a from Z_PROVDASH_UM_ED";

        List<String> suggestion = suggestCC(sql);

        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("CASE WHEN MARA_MEDICARE_RISK_SCORE > 1 THEN 1 ELSE 0 END", suggestion.get(0));
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> suggestCC(String sql) {

        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            return null;
        }

        CCRuleVisitor ruleVisitor = new CCRuleVisitor();
        sqlNode.accept(ruleVisitor);

        return new ArrayList<>(ruleVisitor.getSuggestions());
    }
}
