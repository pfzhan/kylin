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
    public void testConvertDouleQuoteSuccess() {
        List<List<String>> convertSuccessUTs = Arrays.asList(
                //[0] is input,[1] is expect result
                //test for unquoted and limit
                Arrays.asList(
                        "Select ACCOUNT_ID as 中文id, ACCOUNT_COUNTRY as country from  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU' LIMIT 500",
                        "SELECT \"ACCOUNT_ID\" AS \"中文ID\", \"ACCOUNT_COUNTRY\" AS \"COUNTRY\"\n"
                                + "FROM \"DEFAULT\".\"TEST_ACCOUNT\"\n" + "WHERE \"ACCOUNT_COUNTRY\" = 'RU'\n"
                                + "LIMIT 500"),
                //test for quoted
                Arrays.asList(
                        "select ACCOUNT_ID as \"中文id\", ACCOUNT_COUNTRY as \"country\" from  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'",
                        "SELECT \"ACCOUNT_ID\" AS \"中文id\", \"ACCOUNT_COUNTRY\" AS \"country\"\n"
                                + "FROM \"DEFAULT\".\"TEST_ACCOUNT\"\n" + "WHERE \"ACCOUNT_COUNTRY\" = 'RU'"),
                //test for setAlwaysUseParentheses=false
                Arrays.asList("select (a + b) * c", "SELECT (\"A\" + \"B\") * \"C\""),
                //test for setAlwaysUseParentheses=false
                Arrays.asList("select a + b * c", "SELECT \"A\" + \"B\" * \"C\""));
        convertSuccessUTs.forEach(this::testConvertSuccess);

    }

    @Test
    public void testConvertDouleQuoteFailure() {
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
