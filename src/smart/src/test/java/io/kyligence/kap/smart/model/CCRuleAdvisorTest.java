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

package io.kyligence.kap.smart.model;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.model.rule.CCRuleAdvisor;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

public class CCRuleAdvisorTest {

    private File tmpMeta;
    private KylinConfig.SetAndUnsetThreadLocalConfig localConfig;
    private NDataModel dataModel;

    @Before
    public void setUp() throws Exception {
        String metaDir = "src/test/resources/nsmart/cc_validation/meta";
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);

        Properties props = new Properties();
        props.setProperty("kylin.metadata.url", tmpMeta.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.createKylinConfig(props);
        kylinConfig.setProperty("kylin.env", "UT");
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);

        // mock dataModel
        String tableName = metaDir + "/tdvt/table/TDVT.CALCS.json";
        String tableDescContent = FileUtils.readFileToString(new File(tableName), Charset.defaultCharset());
        TableDesc tableDesc = JsonUtil.readValue(tableDescContent, TableDesc.class);
        tableDesc.init(getProjectTDVT());
        Map<String, TableDesc> tableDescMap = Maps.newHashMap();
        tableDescMap.putIfAbsent("TDVT.CALCS", tableDesc);

        String dataModelFilePath = metaDir + "/tdvt/model_desc/c8b64d3f-2fde-485d-9850-9e18fd290286.json";
        String dataModelContent = FileUtils.readFileToString(new File(dataModelFilePath), Charset.defaultCharset());

        dataModel = JsonUtil.readValue(dataModelContent, NDataModel.class);
        dataModel.init(KylinConfig.getInstanceFromEnv(), tableDescMap);
    }

    @After
    public void tearDown() throws Exception {
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
        ResourceStore.clearCache(localConfig.get());
        localConfig.close();
    }

    private String getProjectTDVT() {
        return "tdvt";
    }

    /**
     * This method test proposing CC with agg rule
     *      1. should contains special functions, such as TIMESTAMPDIFF | TIMESTAMPADD
     *      2. param of aggregation cannot nest aggregations
     */
    @Test
    public void testAggFunctionRuleOnSimpleExpressions() {
        String sql = "select sum(timestampdiff(second, time0, time1) ) as c1, \n" //
                + "count(distinct timestampadd(minute, 1, time1)) as c2, \n" //
                + "max(timestampdiff(hour, time1, time0)) as c3, \n" //
                + "min(timestampadd(second, 1, time1)) as c4, \n" //
                + "avg(timestampdiff(hour, time0, time1)) as c5, \n" //
                + "stddev(timestampdiff(minute, time0, time1)) as c6 \n" //
                + "from tdvt.calcs";
        List<String> ccSuggestions1 = collectCCSuggestions(sql);
        ccSuggestions1.sort(String::compareToIgnoreCase);
        Assert.assertEquals(6, ccSuggestions1.size());
        Assert.assertEquals("TIMESTAMPADD(MINUTE, 1, CALCS.TIME1)", ccSuggestions1.get(0));
        Assert.assertEquals("TIMESTAMPADD(SECOND, 1, CALCS.TIME1)", ccSuggestions1.get(1));
        Assert.assertEquals("TIMESTAMPDIFF(HOUR, CALCS.TIME0, CALCS.TIME1)", ccSuggestions1.get(2));
        Assert.assertEquals("TIMESTAMPDIFF(HOUR, CALCS.TIME1, CALCS.TIME0)", ccSuggestions1.get(3));
        Assert.assertEquals("TIMESTAMPDIFF(MINUTE, CALCS.TIME0, CALCS.TIME1)", ccSuggestions1.get(4));
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", ccSuggestions1.get(5));

        // case 2: aggregate expressions cannot be nested, failed for mock query;
        String sql2 = "select sum(sum(timestampdiff(second, time0, time1) ) ) as c1 from tdvt.calcs";
        List<String> ccSuggestions2 = Lists.newArrayList();
        try {
            MockupQueryExecutor executor = new MockupQueryExecutor();
            executor.execute(getProjectTDVT(), sql2);
            ccSuggestions2 = collectCCSuggestions(sql2);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().endsWith("Aggregate expressions cannot be nested"));
            Assert.assertTrue(ccSuggestions2.isEmpty());
        }
    }

    @Test
    public void testAggFunctionRuleOnComplexExpressions() {

        // case 1: timestampdiff with cast nested
        String sql1 = "select sum(timestampdiff(second, time0, cast(datetime1 as timestamp))) from tdvt.calcs";
        List<String> ccSuggestions1 = collectCCSuggestions(sql1);
        Assert.assertEquals(1, ccSuggestions1.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CAST(CALCS.DATETIME1 AS TIMESTAMP))",
                ccSuggestions1.get(0));

        // case 2: compound expression
        String sql2 = "select sum((int1-int2)/(int1+int2)) as c1, sum((int1-int2)/timestampdiff(second, time0, time1) ) as c2, "
                + "sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as c3 from tdvt.calcs";
        List<String> ccSuggestions2 = collectCCSuggestions(sql2);
        ccSuggestions2.sort(String::compareToIgnoreCase);
        Assert.assertEquals(2, ccSuggestions2.size());
        Assert.assertEquals("(CALCS.INT1 - CALCS.INT2) / TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)",
                ccSuggestions2.get(0));
        Assert.assertEquals(
                "TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1) / TIMESTAMPDIFF(SECOND, TIMESTAMPADD(YEAR, 1, CALCS.TIME1), CALCS.TIME1)",
                ccSuggestions2.get(1));

        // case 3: aggregation with param of caseWhenClause contains timestampdiff
        String sql3 = "select sum( case when time0 <> time1 then (int2-int1)/timestampdiff(second, time0, time1) * 60"
                + "    else (int2 - int1)/ timestampdiff(second, time1, datetime0)*60 end) from tdvt.calcs";
        List<String> ccSuggestions3 = collectCCSuggestions(sql3);
        Assert.assertEquals(1, ccSuggestions3.size());
        Assert.assertEquals(
                "CASE WHEN CALCS.TIME0 <> CALCS.TIME1 THEN (CALCS.INT2 - CALCS.INT1) / TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1) * 60 "
                        + "ELSE (CALCS.INT2 - CALCS.INT1) / TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.DATETIME0) * 60 END",
                ccSuggestions3.get(0));
    }

    @Test
    public void testAggRuleOfSupportedSubQuery() {
        String sql = "SELECT * FROM (\n" //
                + "     SELECT SUM(timestampdiff(second, time0, time1)) AS col1, sta.datetime0\n" //
                + "     FROM tdvt.calcs calcs\n" //
                + "     LEFT JOIN (\n" //
                + "       SELECT timestampdiff(minute, time1, time0), datetime0\n" //
                + "       FROM tdvt.calcs \n" //
                + "     ) sta ON calcs.datetime0 = sta.datetime0\n" //
                + "GROUP BY sta.datetime0\n" //
                + "ORDER BY datetime0)";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", ccSuggestions.get(0));
    }

    @Test
    public void testAggRuleShouldNotReplaceCase() {
        String sql = "SELECT * FROM (\n" //
                + "     SELECT SUM(timestampdiff(second, sta.time0, sta.time1)) AS col1, sta.datetime0\n" //
                + "     FROM tdvt.calcs calcs\n" //
                + "     LEFT JOIN (\n" //
                + "         SELECT timestampdiff(minute, time1, time0), datetime0, time1, time0\n" //
                + "         FROM tdvt.calcs \n" //
                + "         union\n" //
                + "         SELECT timestampdiff(minute, time1, time0), datetime0, time1, time0\n" //
                + "         FROM tdvt.calcs\n" //
                + "     ) sta\n" //
                + "     ON calcs.datetime0 = sta.datetime0\n" //
                + "GROUP BY sta.datetime0\n" //
                + "ORDER BY datetime0)";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertTrue(ccSuggestions.isEmpty());
    }

    @Test
    public void testAggRuleIgnoreWindowFunctionOfLiteralCall() {
        String sql = "select num1,\n" //
                + "max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP'1970-01-01 10:01:01')) MAXTIME,\n" //
                + "max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP'1970-01-01 10:01:01')) over() MAXTIME1\n" //
                + "from tdvt.calcs where num1 > 0 group by num1\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
        MockupQueryExecutor executor = new MockupQueryExecutor();
        executor.execute(getProjectTDVT(), sql);
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertTrue(ccSuggestions.isEmpty());
    }

    @Test
    public void testAggRuleIgnoreWindowFunctionOnColumn() {
        String sql = "select num1,\n" //
                + "max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1\n" //
                + "from tdvt.calcs where num1 > 0 group by num1, time0\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
        MockupQueryExecutor executor = new MockupQueryExecutor();
        executor.execute(getProjectTDVT(), sql);
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertTrue(ccSuggestions.isEmpty());

    }

    @Test
    public void testAggRuleIgnoreConflictByAggAndWindowFunction() {
        String sql = "select num1,\n" //
                + "max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) MAXTIME,\n" //
                + "max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1\n" //
                + "from tdvt.calcs where num1 > 0 group by num1, time0\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
        MockupQueryExecutor executor = new MockupQueryExecutor();
        executor.execute(getProjectTDVT(), sql);
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertTrue(ccSuggestions.isEmpty());
    }

    @Test
    public void testAggRuleEffectOnSelectStarSubQuery() {
        String sql = "select sum(timestampdiff(second, time1, time0)), datetime0 "
                + "  from (select * from tdvt.calcs) tb1 group by datetime0 ";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED: Only one part can recognize valid cc, sql acceleration will fail
    @Test
    public void testAggRuleNotSupportCaseForOnlyOnePartCanReplace() {
        String sql = "select * from (select sum(timestampdiff(second, time0, time1)) as col1, sta.datetime0 from tdvt.calcs as calcs "
                + "left join (select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0 "
                + "              from (select time0 as t0, time1, datetime0 from tdvt.calcs) as ca group by ca.datetime0) as sta "
                + "on calcs.datetime0 = sta.datetime0 group by sta.datetime0 order by datetime0)";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        ccSuggestions.sort(String::compareToIgnoreCase);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED: TIMESTAMPDIFF(SECOND, STA.TIME0, STA.TIME1)
    @Ignore
    @Test
    public void testAggRuleOfNotSupportedSubQuery() {
        String sql = "SELECT * FROM (\n" //
                + "     SELECT SUM(timestampdiff(second, sta.time0, sta.time1)) AS col1, sta.datetime0\n" //
                + "     FROM tdvt.calcs calcs\n" //
                + "     LEFT JOIN (\n" //
                + "        SELECT timestampdiff(minute, time1, time0), datetime0\n" //
                + "        FROM tdvt.calcs \n" //
                + "     ) sta ON calcs.datetime0 = sta.datetime0\n" //
                + "GROUP BY sta.datetime0\n" //
                + "ORDER BY datetime0)";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED: cannot get t1 and t0
    @Ignore
    @Test
    public void testAggRuleOnSqlWithSubQuery() {
        String sql = "select sum(timestampdiff(second, t1, t0)), datetime0 "
                + "  from (select time0 as t0, time1 as t1, datetime0 from tdvt.calcs) tb1 group by datetime0 ";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED: cannot get time1 and time0
    @Ignore
    @Test
    public void testAggRuleWithSelectStarOutermost() {
        String sql = "select * from (select sum(timestampdiff(second, time1, time0)), datetime0 "
                + "  from (select time0, time1, datetime0 from tdvt.calcs) tb1 group by datetime0 )";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED: only recognize part of them
    @Ignore
    @Test
    public void testAggRuleOnNotSupportWithClause() {
        String sql = "with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs), \n"
                + "sta as( select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0 from ca group by ca.datetime0 ), \n"
                + "da as (select sum(timestampdiff(second, time0, time1)) as col1, datetime0, num1 from tdvt.calcs group by datetime0, num1) \n"
                + " select col1, datetime0 from ( select da.col1, da.num1, sta.datetime0 from da left join sta on da.datetime0 = sta.datetime0 "
                + "            group by sta.datetime0, num1, col1 order by sta.datetime0) group by col1, datetime0";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        ccSuggestions.sort(String::compareToIgnoreCase);
        Assert.assertEquals(2, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(MINUTE, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", ccSuggestions.get(1));
    }

    // NOT SUPPORTED:
    @Ignore
    @Test
    public void testAggRuleOnWithClause() {
        String sql = "with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs) \n"
                + "select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0 from ca group by ca.datetime0";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
    }

    // NOT SUPPORTED:
    @Ignore
    @Test
    public void testAggRuleOnWithClauseOfSelectStarUsed() {
        String sql = "with ca as(select *from tdvt.calcs) \n"
                + "select sum(timestampdiff(minute, ca.time1, ca.time0)), ca.datetime0 from ca group by ca.datetime0";
        List<String> ccSuggestions = collectCCSuggestions(sql);
        Assert.assertEquals(1, ccSuggestions.size());
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME1, CALCS.TIME0)", ccSuggestions.get(0));
    }

    /**
     * This method test proposing CC with caseWhen rule
     *     1. case when expression appears in groupBy
     *     2. case when expression doesn't contains aggregation
     *     3. case when expression must contain special function like TIMESTAMPDIFF | TIMESTAMPADD
     */
    @Test
    public void testCaseWhenRule() {
        /*
         * case 1: group by caseWhenClause contains special function, with at least two whenCondition in caseWhenClause
         */
        String sql1 = "select case when int0 > 100 then timestampdiff(second, time0, time1) "
                + "                when int0 > 50 then timestampdiff(minute, time0, time1) "
                + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end "
                + "from tdvt.calcs group by case when int0 > 100 then timestampdiff(second, time0, time1)"
                + "                when int0 > 50 then timestampdiff(minute, time0, time1) "
                + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end";
        List<String> ccSuggestions1 = collectCCSuggestions(sql1);
        Assert.assertEquals(1, ccSuggestions1.size());
        Assert.assertEquals(
                "CASE WHEN CALCS.INT0 > 100 THEN TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1) "
                        + "WHEN CALCS.INT0 > 50 THEN TIMESTAMPDIFF(MINUTE, CALCS.TIME0, CALCS.TIME1) "
                        + "WHEN CALCS.INT0 > 0 THEN TIMESTAMPDIFF(HOUR, CALCS.TIME0, CALCS.TIME1) ELSE NULL END",
                ccSuggestions1.get(0));

        /*
         * case 2: not group by caseWhenClause, will not propose cc
         */
        String sql2 = "select case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end from tdvt.calcs";
        List<String> ccSuggestions2 = collectCCSuggestions(sql2);
        Assert.assertTrue(ccSuggestions2.isEmpty());

        /*
         * case 3: will not propose caseWhenClause as cc, but aggregations contains special function.
         * Unsupported case at present.
         */
        String sql3 = "select case when int0 > 10 then sum(timestampdiff(second, time0, time1)) "
                + "else sum(timestampdiff(minute, time0, time1)) end from tdvt.calcs group by int0";
        List<String> ccSuggestion3 = collectCCSuggestions(sql3);
        Assert.assertTrue(ccSuggestion3.isEmpty());
    }

    private List<String> collectCCSuggestions(String sql) {
        CCRuleAdvisor ccRuleAdvisor = new CCRuleAdvisor();
        return ccRuleAdvisor.suggestCandidate(getProjectTDVT(), dataModel, sql);
    }
}
