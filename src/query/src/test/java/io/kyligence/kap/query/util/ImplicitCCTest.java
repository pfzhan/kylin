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

package io.kyligence.kap.query.util;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;

public class ImplicitCCTest {

    @Test
    public void testGetSubqueries() throws SqlParseException {
        String s1 = "WITH customer_total_return AS\n" + "  (SELECT sr_customer_sk AS ctr_customer_sk,\n"
                + "          sr_store_sk AS ctr_store_sk,\n" + "          sum(sr_return_amt) AS ctr_total_return\n"
                + "   FROM store_returns\n" + "   JOIN date_dim ON sr_returned_date_sk = d_date_sk\n"
                + "   WHERE d_year = 1998\n" + "   GROUP BY sr_customer_sk,\n" + "            sr_store_sk),\n"
                + "     tmp AS (\n" + "     SELECT avg(ctr_total_return)*1.2 tmp_avg,\n" + "          ctr_store_sk\n"
                + "   FROM customer_total_return\n" + "   GROUP BY ctr_store_sk)\n" + "\n" + "SELECT c_customer_id\n"
                + "FROM customer_total_return ctr1\n" + "JOIN tmp ON tmp.ctr_store_sk = ctr1.ctr_store_sk\n"
                + "JOIN store ON s_store_sk = ctr1.ctr_store_sk\n"
                + "JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk\n" + "WHERE ctr1.ctr_total_return > tmp_avg\n"
                + "  AND s_state = 'TN'\n" + "ORDER BY c_customer_id\n" + "LIMIT 100";
        String s2 = "WITH a1 AS\n" + "  (WITH a1 AS\n" + "     (SELECT *\n" + "      FROM t) SELECT a1\n"
                + "   FROM t2\n" + "   ORDER BY c_customer_id)\n" + "SELECT a1\n" + "FROM t2\n"
                + "ORDER BY c_customer_id";
        String s3 = "WITH a1 AS\n" + "  (SELECT * FROM t)\n" + "SELECT a1\n" + "FROM\n"
                + "  (WITH a2 AS (SELECT * FROM t) \n" + "    SELECT a2 FROM t2)\n" + "ORDER BY c_customer_id";

        Assert.assertEquals(1, SqlSubqueryFinder.getSubqueries(s1).size());
        Assert.assertEquals(3, SqlSubqueryFinder.getSubqueries(s2).size());
        Assert.assertEquals(3, SqlSubqueryFinder.getSubqueries(s3).size());
    }

    @Test
    public void testErrorCase() throws SqlParseException {
        BiMap<String, String> mockMapping = HashBiMap.create();
        mockMapping.put("t", "t");
        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(mockMapping, null);

        {
            //computed column is null or empty
            String sql = "select a from t";
            List<ComputedColumnDesc> list = Lists.newArrayList();
            List<SqlCall> sqlSelects = SqlSubqueryFinder.getSubqueries(sql);
            Assert.assertEquals("select a from t", ConvertToComputedColumn
                    .replaceComputedColumn(sql, sqlSelects.get(0), null, queryAliasMatchInfo).getFirst());
            Assert.assertEquals("select a from t", ConvertToComputedColumn
                    .replaceComputedColumn(sql, sqlSelects.get(0), list, queryAliasMatchInfo).getFirst());
        }

        //        {
        //            //input is null or empty or parse error
        //            String sql = "select sum(a from t";
        //            ComputedColumnDesc mockedCC = Mockito.mock(ComputedColumnDesc.class);
        //            Mockito.when(mockedCC.getColumnName()).thenReturn("cc");
        //            Mockito.when(mockedCC.getExpression()).thenReturn("a + b");
        //
        //            List<ComputedColumnDesc> list = Lists.newArrayList(mockedCC);
        //            List<ComputedColumnDesc> computedColumns2 = ConvertToComputedColumn.getCCListSortByLength(list);
        //
        //            List<SqlCall> sqlSelects = SqlSubqueryFinder.getSubqueries(sql);
        //
        //            Assert.assertEquals(null, ConvertToComputedColumn.replaceComputedColumn(null, sqlSelects.get(0),
        //                    computedColumns2, queryAliasMatchInfo));
        //            Assert.assertEquals("", ConvertToComputedColumn.replaceComputedColumn("", sqlSelects.get(0),
        //                    computedColumns2, queryAliasMatchInfo));
        //            Assert.assertEquals("select sum(a from t", ConvertToComputedColumn.replaceComputedColumn(sql,
        //                    sqlSelects.get(0), computedColumns2, queryAliasMatchInfo));
        //        }
    }

    private ComputedColumnDesc mockComputedColumnDesc(String name, String expr, String tableAlias) {
        ComputedColumnDesc mockedCC = Mockito.mock(ComputedColumnDesc.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                throw new RuntimeException(invocation.getMethod().getName() + " is not stubbed");
            }
        });
        Mockito.doReturn(name).when(mockedCC).getColumnName();
        Mockito.doReturn(expr).when(mockedCC).getExpression();
        Mockito.doReturn(tableAlias).when(mockedCC).getTableAlias();

        return mockedCC;
    }

    @Test
    public void testReplaceComputedColumn() throws SqlParseException {

        String sql0 = "select (t1 . a + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z from table1 as t1 group by t1.a+   t1.b +     t1.c having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        //String sql0 = "select (\"t1\" . \"a\" + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z from table1 as t1 group by t1.a+   t1.b +     t1.c having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        String sql1 = "select sum(sum(a)) from table1 as t1";
        String sql2 = "select substring(substring(t1.d,1,3),1,3) from table1 as t1";
        String sql3 = "select a + b + (c+d   \t\n) from table1";

        List<ComputedColumnDesc> mockCCs = Lists.newArrayList(
                mockComputedColumnDesc("cc0", "table1.a + table1.b + table1.c", "TABLE1"),
                mockComputedColumnDesc("cc1", "sum(table1.a)", "TABLE1"), //
                mockComputedColumnDesc("cc2", "table1.a + table1.b", "TABLE1"),
                mockComputedColumnDesc("cc3", "table2.c + table2.d", "TABLE2"),
                mockComputedColumnDesc("cc", "substring(substring(table1.d,1,3),1,3)", "TABLE1"),
                mockComputedColumnDesc("cc4", "(table1.a + table1.b) + (table1.c + table1.d)", "TABLE1"),
                mockComputedColumnDesc("cc5", "CAST(table1.a AS double)", "TABLE1"),
                mockComputedColumnDesc("cc6", "{fn convert(table1.a, double)}", "TABLE1"));
        mockCCs = ConvertToComputedColumn.getCCListSortByLength(mockCCs);
        for (ComputedColumnDesc cc : mockCCs) {
            System.out.println(cc.getColumnName());
        }

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("T1", "TABLE1");
        aliasMapping.put("T2", "TABLE2");

        ColumnRowType columnRowType1 = ColumnRowTypeMockUtil.mock("TABLE1", "T1", //
                Pair.newPair("A", "integer"), //
                Pair.newPair("B", "integer"), //
                Pair.newPair("C", "integer"), //
                Pair.newPair("D", "integer"));

        LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
        mockQueryAlias.put("TABLE1", columnRowType1);

        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        Assert.assertEquals(
                "select (T1.cc0) as c, T1.cc as z from table1 as t1 group by T1.cc0 having T1.cc0 > 100 order by T1.cc0",
                ConvertToComputedColumn.replaceComputedColumn(sql0, SqlSubqueryFinder.getSubqueries(sql0).get(0),
                        mockCCs, queryAliasMatchInfo).getFirst());

        Assert.assertEquals("select sum(T1.cc1) from table1 as t1", ConvertToComputedColumn
                .replaceComputedColumn(sql1, SqlSubqueryFinder.getSubqueries(sql1).get(0), mockCCs, queryAliasMatchInfo)
                .getFirst());

        Assert.assertEquals("select T1.cc from table1 as t1", ConvertToComputedColumn
                .replaceComputedColumn(sql2, SqlSubqueryFinder.getSubqueries(sql2).get(0), mockCCs, queryAliasMatchInfo)
                .getFirst());

        Assert.assertEquals("select T1.cc4 from table1", ConvertToComputedColumn
                .replaceComputedColumn(sql3, SqlSubqueryFinder.getSubqueries(sql3).get(0), mockCCs, queryAliasMatchInfo)
                .getFirst());

        //Case SUM(CAST(...)) and sum({fn convert(...)})
        String sqlWithSum = "select sum(CAST(T1.a AS double)) from table1";
        Assert.assertEquals("select sum(T1.cc5) from table1", ConvertToComputedColumn.replaceComputedColumn(sqlWithSum,
                SqlSubqueryFinder.getSubqueries(sqlWithSum).get(0), mockCCs, queryAliasMatchInfo).getFirst());
        String sqlWithfnconvert = "select sum({fn convert(T1.a, double)}) from table1";
        Assert.assertEquals("select sum(T1.cc6) from table1",
                ConvertToComputedColumn
                        .replaceComputedColumn(sqlWithfnconvert,
                                SqlSubqueryFinder.getSubqueries(sqlWithfnconvert).get(0), mockCCs, queryAliasMatchInfo)
                        .getFirst());

        //more tables
        String sql2tables = "select t1.a + t1.b as aa, t2.c + t2.d as bb from table1 t1 inner join table2 t2 on t1.x = t2.y where t1.a + t1.b > t2.c + t2.d order by t1.a + t1.b";

        ColumnRowType columnRowType2 = ColumnRowTypeMockUtil.mock("TABLE2", "T2", //
                Pair.newPair("A", "integer"), //
                Pair.newPair("B", "integer"), //
                Pair.newPair("C", "integer"), //
                Pair.newPair("D", "integer"));

        mockQueryAlias.put("TABLE2", columnRowType2);
        queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        Assert.assertEquals(
                "select T1.cc2 as aa, T2.cc3 as bb from table1 t1 inner join table2 t2 on t1.x = t2.y where T1.cc2 > T2.cc3 order by T1.cc2",
                ConvertToComputedColumn.replaceComputedColumn(sql2tables,
                        SqlSubqueryFinder.getSubqueries(sql2tables).get(0), mockCCs, queryAliasMatchInfo)
                        .getFirst());

        String sql2tableswithquote = "\r\n select \"T1\".\"A\" + \"T1\".\"B\" as aa, \"T2\".\"C\" + \"T2\".\"D\" as bb from \r\n table1 \"T1\" inner join table2 \"T2\" on \"T1\".\"X\" = \"T2\".\"Y\" where \"T1\".\"A\" + \"T1\".\"B\" > \"T2\".\"C\" + \"T2\".\"D\" order by \"T1\".\"A\" + \"T1\".\"B\"";
        Assert.assertEquals(
                "\r\n select T1.cc2 as aa, T2.cc3 as bb from \r\n table1 \"T1\" inner join table2 \"T2\" on \"T1\".\"X\" = \"T2\".\"Y\" where T1.cc2 > T2.cc3 order by T1.cc2",
                ConvertToComputedColumn.replaceComputedColumn(sql2tableswithquote,
                        SqlSubqueryFinder.getSubqueries(sql2tableswithquote).get(0), mockCCs, queryAliasMatchInfo)
                        .getFirst());

        //        //sub query cannot be mocked here
        //        String sqlwithsubquery = "select count(*), sum(t1.a + t1.b), sum(t22.w) from table1 t1 inner join (select t11.a + t11.b as aa, t22.c + t22.d as bb from table1 t11 inner join table2 t22 on t11.x = t22.y where t11.a + t11.b > t22.c + t22.d order by t11.a + t11.b) as t2 on t1.x = t2.aa group by substring(substring(t1.d,1,3),1,3) order by sum(t1.a) ";
        //        Assert.assertEquals(
        //            "select T1.cc2 as aa, T2.cc3 as bb from table1 \"T1\" inner join table2 \"T2\" on \"T1\".\"X\" = \"T2\".\"Y\" where T1.cc2 > T2.cc3 order by T1.cc2",
        //            ConvertToComputedColumn.replaceComputedColumn(sqlwithsubquery,
        //                SqlSubqueryFinder.getSubqueries(sqlwithsubquery).get(0), mockCCs, queryAliasMatchInfo));
    }
}
