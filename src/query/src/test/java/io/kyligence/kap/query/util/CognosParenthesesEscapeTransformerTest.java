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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.query.util.QueryUtil;
import org.junit.Assert;
import org.junit.Test;

public class CognosParenthesesEscapeTransformerTest {

    @Test
    public void basicTest() {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String data = " from ((a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3) inner join c as cc on a.x1=cc.z1 ) join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String expected = " from a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3 inner join c as cc on a.x1=cc.z1  join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String transformed = escape.completion(data);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced1Test() throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced2Test() throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query02.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query02.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advanced3Test() throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query03.sql"),
                Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query03.sql.expected"),
                Charset.defaultCharset());
        String transformed = escape.completion(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void proguardTest() throws IOException {
        CognosParenthesesEscapeTransformer escape = new CognosParenthesesEscapeTransformer();
        Collection<File> files = FileUtils.listFiles(new File("../../src/kap-it/src/test/resources/query"),
                new String[] { "sql" }, true);
        for (File f : files) {
            System.out.println("checking " + f.getCanonicalPath());
            if (f.getCanonicalPath().contains("sql_parentheses_escape")) {
                continue;
            }
            if (f.getCanonicalPath().contains("sql_count_distinct_expr")) {
                continue;
            }
            // KAP#16063 CognosParenthesesEscapeTransformer wrongly removes
            // parentheses for sql in sql_except directory
            // exclude sql_except directory from the test until the issue is fixed
            if (f.getCanonicalPath().contains("sql_except")) {
                continue;
            }
            String query = FileUtils.readFileToString(f, Charset.defaultCharset());
            String transformed = escape.completion(query);
            Assert.assertEquals(query, transformed);
        }
    }

    @Test
    public void advanced4Test() throws Exception {
        CognosParenthesesEscapeTransformer converter = new CognosParenthesesEscapeTransformer();

        String originalSql = "select count(*) COU FrOm --comment0\n" + "---comment1\n" + "(  --comment2\n"
                + "test_kylin_fact left join EDW.TEST_CAL_DT on TEST_CAL_DT.CAL_DT = test_kylin_fact.CAL_DT\n" + ")\n"
                + "left join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID";
        String expectedSql = "select count(*) COU FrOm \n" + "\n" + "  \n"
                + "test_kylin_fact left join EDW.TEST_CAL_DT on TEST_CAL_DT.CAL_DT = test_kylin_fact.CAL_DT\n" + "\n"
                + "left join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID";
        originalSql = QueryUtil.removeCommentInSql(originalSql);
        String transformed = converter.completion(originalSql);
        Assert.assertEquals(expectedSql, transformed);

        String transformedSecond = converter.completion(transformed);
        Assert.assertEquals(expectedSql, transformedSecond);
    }

    @Test
    public void advanced5Test() throws Exception {
        CognosParenthesesEscapeTransformer convertTransformer = new CognosParenthesesEscapeTransformer();
        String sql2 = FileUtils.readFileToString(
                new File("../../src/kap-it/src/test/resources/query/sql_parentheses_escape/query05.sql"),
                Charset.defaultCharset());
        String expectedSql2 = FileUtils.readFileToString(
                new File("../../src/kap-it/src/test/resources/query/sql_parentheses_escape/query05.sql.expected"),
                Charset.defaultCharset());
        sql2 = QueryUtil.removeCommentInSql(sql2);
        String transform2 = convertTransformer.completion(sql2).replaceAll("[\n]+", "");
        expectedSql2 = expectedSql2.replaceAll("[\n]+", "");
        Assert.assertEquals(expectedSql2, transform2);
    }

    @Test
    public void advanced6Test() throws Exception {
        CognosParenthesesEscapeTransformer convertTransformer = new CognosParenthesesEscapeTransformer();

        String originalSql = FileUtils.readFileToString(
                new File("../../src/kap-it/src/test/resources/query/sql_parentheses_escape/query06.sql"),
                Charset.defaultCharset());
        String expectedSql = FileUtils.readFileToString(
                new File("../../src/kap-it/src/test/resources/query/sql_parentheses_escape/query06.sql.expected"),
                Charset.defaultCharset());
        originalSql = QueryUtil.removeCommentInSql(originalSql);
        String transformed = convertTransformer.completion(originalSql).replaceAll("[\n]+", "");
        expectedSql = expectedSql.replaceAll("[\n]+", "");
        Assert.assertEquals(expectedSql, transformed);
    }
}
