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
package org.apache.kylin.source.adhocquery;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DoubleQuotePushDownConverterTest {

    @Test
    public void testConvertDoubleQuoteSuccess() {
        List<List<String>> convertSuccessUTs = Arrays.asList(
                //[0] is input,[1] is expect result

                //test for blank space with Identifier
                Arrays.asList("select atbale .a as \"ACOL\",atbale. b as Bcol,\"c\" as \"acol\" from atbale",
                        "select \"ATBALE\".\"A\" as \"ACOL\",\"ATBALE\".\"B\" as \"BCOL\",\"c\" as \"acol\" from \"ATBALE\""),
                Arrays.asList("select a as \"ACOL\",b as Bcol,\"c\" as \"acol\" from atbale",
                        "select \"A\" as \"ACOL\",\"B\" as \"BCOL\",\"c\" as \"acol\" from \"ATBALE\""),
                //test for with as...(select ....)
                Arrays.asList(
                        "WITH 中文tb AS (\n" + "\t\tSELECT *\n" + "\t\tFROM abc\n" + "\t)\n" + "SELECT *\n" + "FROM t1",
                        "WITH \"中文TB\" AS (\n" + "\t\tSELECT *\n" + "\t\tFROM \"ABC\"\n" + "\t)\n" + "SELECT *\n"
                                + "FROM \"T1\""),
                //test for with as...(union all)
                Arrays.asList(
                        "WITH t1 AS (\n" + "\t\tSELECT *\n" + "\t\tFROM abc\n" + "\t\tUNION ALL\n" + "\t\tSELECT *\n"
                                + "\t\tFROM dfs\n" + "\t)\n" + "SELECT *\n" + "FROM t1\n" + "LIMIT 500",
                        "WITH \"T1\" AS (\n" + "\t\tSELECT *\n" + "\t\tFROM \"ABC\"\n" + "\t\tUNION ALL\n"
                                + "\t\tSELECT *\n" + "\t\tFROM \"DFS\"\n" + "\t)\n" + "SELECT *\n" + "FROM \"T1\"\n"
                                + "LIMIT 500"),

                //test for select * ...
                Arrays.asList("select a.* from a", "select \"A\".* from \"A\""),
                Arrays.asList("select 中文表.* from 中文表", "select \"中文表\".* from \"中文表\""),
                Arrays.asList("select 中文表 . * from 中文表", "select \"中文表\".* from \"中文表\""),

                //test chinese column
                Arrays.asList("select a.中文列 from a", "select \"A\".\"中文列\" from \"A\""),
                //test for unquoted and limit
                Arrays.asList(
                        "SELECT ACCOUNT_ID AS 中文id, ACCOUNT_COUNTRY AS country\n" + "FROM \"DEFAULT\".TEST_ACCOUNT\n"
                                + "WHERE ACCOUNT_COUNTRY = 'RU'\n" + "LIMIT 500",
                        "SELECT \"ACCOUNT_ID\" AS \"中文ID\", \"ACCOUNT_COUNTRY\" AS \"COUNTRY\"\n"
                                + "FROM \"DEFAULT\".\"TEST_ACCOUNT\"\n" + "WHERE \"ACCOUNT_COUNTRY\" = 'RU'\n"
                                + "LIMIT 500"),
                //test for quoted
                Arrays.asList(
                        "select ACCOUNT_ID as \"中文id\", ACCOUNT_COUNTRY as \"country\" from  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'",
                        "select \"ACCOUNT_ID\" as \"中文id\", \"ACCOUNT_COUNTRY\" as \"country\" from  \"DEFAULT\".\"TEST_ACCOUNT\" where \"ACCOUNT_COUNTRY\"='RU'"),
                //test for unchanged for  all expressions
                Arrays.asList("select (a + b) * c", "select (\"A\" + \"B\") * \"C\""),
                Arrays.asList("select a + b * c", "select \"A\" + \"B\" * \"C\""),
                Arrays.asList("select 1 + b * c", "select 1 + \"B\" * \"C\""));
        convertSuccessUTs.forEach(this::testConvertSuccess);

    }

    @Test
    public void testConvertDoubleQuoteFailure() {
        List<String> convertFailureUTs = Arrays.asList(
                //quoted by `
                "select ACCOUNT_ID as 中文id, ACCOUNT_COUNTRY as `country` from  `DEFAULT`.TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'",
                //syntax error
                "select ACCOUNT_ID as 中文id, ACCOUNT_COUNTRY as country from1  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'");

        convertFailureUTs.forEach(this::testConvertFailure);

    }

    private void testConvertSuccess(List<String> inputArrayList) {
        String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(inputArrayList.get(0));
        String expectedSql = inputArrayList.get(1);
        Assert.assertEquals(convertSql, expectedSql);
    }

    private void testConvertFailure(String inputSql) {
        String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(inputSql);
        Assert.assertEquals(convertSql, inputSql);
    }
}
