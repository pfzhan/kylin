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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

public class ImplicitCCOnModelViewTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    // tests from ImplicitCCTest
    @Test
    public void testReplaceComputedColumn() throws SqlParseException {

        String sql0 = "select (t1 . a + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z from newten.view as t1 "
                + "group by t1.a+   t1.b +     t1.c, d having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        //String sql0 = "select (\"t1\" . \"a\" + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z " +
        // "from table1 as t1 group by t1.a+   t1.b +     t1.c having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        String sql1 = "select sum(sum(a)) from newten.view as t1";
        String sql2 = "select substring(substring(t1.d,1,3),1,3) from newten.view as t1";
        String sql3 = "select a + b + (c+d   \t\n) from newten.view";

        List<ComputedColumnDesc> mockCCs = Lists.newArrayList(
                mockComputedColumnDesc("cc0", "table1.a + table1.b + table1.c", "TABLE1"),
                mockComputedColumnDesc("cc1", "sum(table1.a)", "TABLE1"), //
                mockComputedColumnDesc("cc2", "table1.a + table1.b", "TABLE1"),
                mockComputedColumnDesc("cc3", "table2.c + table2.d", "TABLE2"),
                mockComputedColumnDesc("cc", "substring(substring(table1.d,1,3),1,3)", "TABLE1"),
                mockComputedColumnDesc("cc4", "(table1.a + table1.b) + (table1.c + table1.d)", "TABLE1"),
                mockComputedColumnDesc("cc5", "CAST(table1.a AS double)", "TABLE1"),
                mockComputedColumnDesc("cc6", "\"0910_TABLE3\".\"0910_A\" * \"0910_TABLE3\".\"0910_B\"", "0910_TABLE3"));
        mockCCs = ConvertToComputedColumn.getCCListSortByLength(mockCCs);
        for (ComputedColumnDesc cc : mockCCs) {
            System.out.println(cc.getColumnName());
        }
        List<NDataModel.NamedColumn> namedColumns1 = Lists.newArrayList(
                mockNamedCol("A", "TABLE1", "A"),
                mockNamedCol("B", "TABLE1", "B"),
                mockNamedCol("C", "TABLE1", "C"),
                mockNamedCol("D", "TABLE1", "D")
        );

        QueryAliasMatchInfo viewWithAliasMatchInfo = QueryAliasMatchInfo.fromModelView("T1", mockDataModel("view", mockCCs, namedColumns1));
        QueryAliasMatchInfo viewAliasMatchInfo = QueryAliasMatchInfo.fromModelView("view", mockDataModel("view", mockCCs, namedColumns1));

        Assert.assertEquals(
                "select (T1.cc0) as c, substring(substring(d,1,3),1,3) as z from newten.view "
                        + "as t1 group by T1.cc0, d having T1.cc0 > 100 order by T1.cc0",
                ConvertToComputedColumn.replaceComputedColumn(sql0, SqlSubqueryFinder.getSubqueries(sql0).get(0),
                        mockCCs, viewWithAliasMatchInfo).getFirst());

        Assert.assertEquals("select sum(T1.cc1) from newten.view as t1", ConvertToComputedColumn
                .replaceComputedColumn(sql1, SqlSubqueryFinder.getSubqueries(sql1).get(0), mockCCs, viewWithAliasMatchInfo)
                .getFirst());

        Assert.assertEquals("select T1.cc from newten.view as t1", ConvertToComputedColumn
                .replaceComputedColumn(sql2, SqlSubqueryFinder.getSubqueries(sql2).get(0), mockCCs, viewWithAliasMatchInfo)
                .getFirst());

        Assert.assertEquals("select view.cc4 from newten.view", ConvertToComputedColumn
                .replaceComputedColumn(sql3, SqlSubqueryFinder.getSubqueries(sql3).get(0), mockCCs, viewAliasMatchInfo)
                .getFirst());


        //Case SUM(CAST(...)) and sum({fn convert(...)})
        String sqlWithSum = "select sum(CAST(T1.a AS double)) from newten.view";
        Assert.assertEquals("select sum(view.cc5) from newten.view", ConvertToComputedColumn.replaceComputedColumn(sqlWithSum,
                SqlSubqueryFinder.getSubqueries(sqlWithSum).get(0), mockCCs, viewAliasMatchInfo).getFirst());

        //more tables
        List<NDataModel.NamedColumn> namedColumns2 = Lists.newArrayList(
                mockNamedCol("TABLE2_A", "TABLE2", "A"),
                mockNamedCol("TABLE2_B", "TABLE2", "B"),
                mockNamedCol("TABLE2_C", "TABLE2", "C"),
                mockNamedCol("TABLE2_D", "TABLE2", "D")
        );
        namedColumns2.addAll(namedColumns1);
        QueryAliasMatchInfo viewAliasMoreTablesMatchInfo =
                QueryAliasMatchInfo.fromModelView("v1", mockDataModel("view", mockCCs, namedColumns2));

        String sql2tables = "select v1.a + v1.b as aa, v1.TABLE2_C + v1.TABLE2_D as bb from newten.view v1 order by v1.a + v1.b";
        String sql2tablesExpected = "select v1.cc2 as aa, v1.cc3 as bb from newten.view v1 order by v1.cc2";
        Assert.assertEquals(
                sql2tablesExpected,
                ConvertToComputedColumn.replaceComputedColumn(sql2tables,
                        SqlSubqueryFinder.getSubqueries(sql2tables).get(0), mockCCs, viewAliasMoreTablesMatchInfo).getFirst());

        String sql2tableswithquote = "\r\n select \"v1\".\"A\" + \"v1\".\"B\" as aa, \"v1\".\"TABLE2_C\" + \"v1\".\"TABLE2_D\" as bb "
                + "from newten.view v1 "
                + "order by \"v1\".\"A\" + \"v1\".\"B\"";
        String sql2tableswithquoteExpected = "\r\n select v1.cc2 as aa, v1.cc3 as bb from newten.view v1 order by v1.cc2";
        Assert.assertEquals(
                sql2tableswithquoteExpected,
                ConvertToComputedColumn.replaceComputedColumn(sql2tableswithquote,
                        SqlSubqueryFinder.getSubqueries(sql2tableswithquote).get(0), mockCCs, viewAliasMoreTablesMatchInfo)
                        .getFirst());

        List<NDataModel.NamedColumn> namedColumns3 = Lists.newArrayList(
                mockNamedCol("0910_A", "0910_TABLE3", "0910_A"),
                mockNamedCol("0910_B", "0910_TABLE3", "0910_B"),
                mockNamedCol("0910_TABLE3_C", "0910_TABLE3", "C")
        );

        String sql4 = "select sum(\"0910_a\" * \"0910_b\"), \"0910_TABLE3_C\" from newten.view group by \"0910_TABLE3_C\"";
        QueryAliasMatchInfo viewAliasSql4MatchInfo =
                QueryAliasMatchInfo.fromModelView("view", mockDataModel("view", mockCCs, namedColumns3));
        Assert.assertEquals("select sum(view.cc6), \"0910_TABLE3_C\" from newten.view group by \"0910_TABLE3_C\"",
                ConvertToComputedColumn.replaceComputedColumn(sql4, SqlSubqueryFinder.getSubqueries(sql4).get(0), mockCCs, viewAliasSql4MatchInfo).getFirst());

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

    private int idx = 0;
    private NDataModel.NamedColumn mockNamedCol(String modelColname, String tableAlias, String columnName) {
        NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
        namedColumn.setId(idx++);
        namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        namedColumn.setName(modelColname);
        namedColumn.setAliasDotColumn(tableAlias + "." + columnName);
        return namedColumn;
    }

    private NDataModel mockDataModel(String name, List<ComputedColumnDesc> cc, List<NDataModel.NamedColumn> namedColumns) {
        NDataModel model = new NDataModel();
        model.setAlias(name);
        model.setComputedColumnDescs(cc);
        model.setAllNamedColumns(namedColumns);
        return model;
    }

}
