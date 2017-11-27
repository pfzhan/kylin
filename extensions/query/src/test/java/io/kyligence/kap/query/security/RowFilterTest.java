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


package io.kyligence.kap.query.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.Assert;
import org.junit.Test;


public class RowFilterTest {
    @Test
    public void testBrackets() {
        String sql = "select * from (select * from t inner join tt on t.a = tt.a inner join ttt on t.a = ttt.a where t.a = 0 and tt.a=1 ) t1 where t1.a = 2";
        String sql1 = "select * from t where t.a in (select * from tt where tt.b=1)";
        HashSet<String> tbls = Sets.newHashSet("DB.T", "DB.TT", "DB.TTT");
        String expectSQL = "select * from (select * from t inner join tt on t.a = tt.a inner join ttt on t.a = ttt.a where (t.a = 0 and tt.a=1) ) t1 where t1.a = 2";
        String expectSQL1 = "select * from t where (t.a in (select * from tt where (tt.b=1)))";
        Assert.assertEquals(expectSQL, RowFilter.whereClauseBracketsCompletion("DB", sql, tbls));
        Assert.assertEquals(expectSQL1, RowFilter.whereClauseBracketsCompletion("DB", sql1, tbls));
    }

    @Test
    public void testRowFilterWithMultiWhereConds() {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> whereCond1 = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> whereCond2 = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "( a > 0 OR b < 0 )");
        whereCond1.put("DB.T", "( a > 0 OR b < 0 )");
        whereCond2.put("DB.T", "( a > 0 OR b < 0 )");
        list.add(whereCond);
        list.add(whereCond1);
        list.add(whereCond2);
        String sql = "select * from (select * from t)";
        for (Map<String, String> whereCondWithTbls : list) {
            sql = RowFilter.rowFilter("DB", sql, whereCondWithTbls);
        }
        String expectedSQL = "select * from (select * from t WHERE ( T.a > 0 OR T.b < 0 ) AND ( T.a > 0 OR T.b < 0 ) AND ( T.a > 0 OR T.b < 0 ))";
        Assert.assertEquals(expectedSQL, sql);
    }

    @Test
    public void testSimpleRowFilter() {
        String sql = "select count(*) from t join tt on t.a = tt.a group by c";
        String sql2 = "select count(*) from t join tt on t.a = tt.a where (b1 = 'v1') group by c";
        String sql3 = "select count(*) from t where t.c > 10";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "(a > 0 OR b < 0)");
        whereCond.put("DB.TT", "(aa > 0 OR bb < 0)");

        String expectedSQL = "select count(*) from t join tt on t.a = tt.a WHERE (T.a > 0 OR T.b < 0) AND (TT.aa > 0 OR TT.bb < 0) group by c";
        String expectedSQL2 = "select count(*) from t join tt on t.a = tt.a where (b1 = 'v1') AND (T.a > 0 OR T.b < 0) AND (TT.aa > 0 OR TT.bb < 0) group by c";
        String expectedSQL3 = "select count(*) from t where t.c > 10 AND (T.a > 0 OR T.b < 0)";

        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
        Assert.assertEquals(expectedSQL2, RowFilter.rowFilter("DB", sql2, whereCond));
        Assert.assertEquals(expectedSQL3, RowFilter.rowFilter("DB", sql3, whereCond));
    }

    @Test
    public void testRowFilter() throws SqlParseException {
        String sql = "select a, (select count(*) from DB3.aa a1 order by a)\n"
                + "from ttt join (select a,b from (select * from DB.t t1), (select * from DB.bb)), tt\n"
                + "where c in (select * from tt) and d > 10 order by abc";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "(a > 0 OR b < 0)");
        whereCond.put("DB2.TT", "(aa > 0 OR bb < 0)");
        whereCond.put("DB2.TTT", "(aaa > 0 OR bbb < 0)");
        String expectedSQL = "select a, (select count(*) from DB3.aa a1 order by a)\n" +
                "from ttt join (select a,b from (select * from DB.t t1 WHERE (T1.a > 0 OR T1.b < 0)), (select * from DB.bb)), tt\n" +
                "where c in (select * from tt WHERE (TT.aa > 0 OR TT.bb < 0)) and d > 10 AND (TTT.aaa > 0 OR TTT.bbb < 0) AND (TT.aa > 0 OR TT.bb < 0) order by abc";
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB2", sql, whereCond));
    }

    @Test
    public void testWithFunc() throws SqlParseException {
        String sql =
                "with avg_tmp as (\n" +
                "    select\n" +
                "        avg(c_acctbal) as avg_acctbal\n" +
                "    from\n" +
                "        customer\n" +
                "    where\n" +
                "        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n" +
                "),\n" +
                "cus_tmp as (\n" +
                "    select c_custkey as noordercus\n" +
                "    from\n" +
                "        customer left join v_orders on c_custkey = o_custkey\n" +
                "    where o_orderkey is null\n" +
                ")\n" +
                "\n" +
                "select\n" +
                "    cntrycode,\n" +
                "    count(1) as numcust,\n" +
                "    sum(c_acctbal) as totacctbal\n" +
                "from (\n" +
                "    select\n" +
                "        substring(c_phone, 1, 2) as cntrycode,\n" +
                "        c_acctbal\n" +
                "    from \n" +
                "        customer inner join cus_tmp on c_custkey = noordercus, avg_tmp\n" +
                "    where \n" +
                "        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n" +
                "        and c_acctbal > avg_acctbal\n" +
                ") t\n" +
                "group by\n" +
                "    cntrycode\n" +
                "order by\n" +
                "    cntrycode";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String expectedSQL = "with avg_tmp as (\n" +
                "    select\n" +
                "        avg(c_acctbal) as avg_acctbal\n" +
                "    from\n" +
                "        customer\n" +
                "    where\n" +
                "        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17') AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0)\n" +
                "),\n" +
                "cus_tmp as (\n" +
                "    select c_custkey as noordercus\n" +
                "    from\n" +
                "        customer left join v_orders on c_custkey = o_custkey\n" +
                "    where o_orderkey is null AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0)\n" +
                ")\n" +
                "\n" +
                "select\n" +
                "    cntrycode,\n" +
                "    count(1) as numcust,\n" +
                "    sum(c_acctbal) as totacctbal\n" +
                "from (\n" +
                "    select\n" +
                "        substring(c_phone, 1, 2) as cntrycode,\n" +
                "        c_acctbal\n" +
                "    from \n" +
                "        customer inner join cus_tmp on c_custkey = noordercus, avg_tmp\n" +
                "    where \n" +
                "        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n" +
                "        and c_acctbal > avg_acctbal AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0) AND (CUS_TMP.aa > 0 OR CUS_TMP.bb < 0)\n" +
                ") t\n" +
                "group by\n" +
                "    cntrycode\n" +
                "order by\n" +
                "    cntrycode";
        whereCond.put("DB.CUSTOMER", "(a > 0 OR b < 0)");
        whereCond.put("DB.CUS_TMP", "(aa > 0 OR bb < 0)");
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testNeedEscape() throws SqlParseException {
        String sql = "select count(*) as \"m0\"";
        Assert.assertEquals(true, RowFilter.needEscape(true, sql, "DB2", Lists.<Map<String, String>>newArrayList(new HashMap<String, String>())));
    }

    @Test
    public void testRowFilterWithUnoin() {
        String sql =
                "select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01'\n" +
                "union\n" +
                "select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01'";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.TEST_KYLIN_FACT", "(a > 0 OR b < 0)");
        String expectedSQL =
                "select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' AND (TEST_KYLIN_FACT.a > 0 OR TEST_KYLIN_FACT.b < 0)\n" +
                "union\n" +
                "select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01' AND (TEST_KYLIN_FACT.a > 0 OR TEST_KYLIN_FACT.b < 0)";
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testJoinWithOutWhere() {
        String sql = "select * from T1 join (select * from T2) ta on T1.c=ta.c GROUP BY c";
        String exceptedSQL = "select * from T1 join (select * from T2) ta on T1.c=ta.c WHERE T1.OPS_REGION='Shanghai' GROUP BY c";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T1", "OPS_REGION='Shanghai'");
        whereCond.put("DB.T3", "OPS_REGION='Beijing'");
        Assert.assertEquals(exceptedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testInsertAliasInExpr() {
        String expr = "a > b and c = 10 and city ='上海'";
        String expr1 = "a > 0 OR b < 0";
        String expr2 = "aa > 0 OR bb < 0";
        String expr3 = "aaa > 0 OR bbb < 0";
        String excepted = "t1.a > t1.b and t1.c = 10 and t1.city ='上海'";
        String excepted1 = "t1.a > 0 OR t1.b < 0";
        String excepted2 = "t1.aa > 0 OR t1.bb < 0";
        String excepted3 = "t1.aaa > 0 OR t1.bbb < 0";
        Assert.assertEquals(excepted, CalciteParser.insertAliasInExpr(expr, "t1"));
        Assert.assertEquals(excepted1, CalciteParser.insertAliasInExpr(expr1, "t1"));
        Assert.assertEquals(excepted2, CalciteParser.insertAliasInExpr(expr2, "t1"));
        Assert.assertEquals(excepted3, CalciteParser.insertAliasInExpr(expr3, "t1"));
    }
}
