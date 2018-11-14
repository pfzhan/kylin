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

package io.kyligence.kap.query.validator;

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.kyligence.kap.smart.query.validator.SqlSyntaxValidator;

public class SqlSyntaxValidatorTest extends SqlValidateTestBase {

    private final MockupQueryExecutor queryExecutor = new MockupQueryExecutor();

    @Test
    public void testGoodCases() throws IOException {

        String[] goodSqls = new String[] { //
                "select 1",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = {d '2012-01-01'} group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                "select part_dt, sum(item_count) from kylin_sales group by part_dt" };

        SqlSyntaxValidator validator = new SqlSyntaxValidator(kylinConfig, proj, queryExecutor);
        final Map<String, SQLValidateResult> goodResults = validator.batchValidate(goodSqls);
        printSqlValidateResults(goodResults);
        goodResults.forEach((key, sqlValidateResult) -> Assert.assertTrue(sqlValidateResult.isCapable()));
    }

    @Test
    public void testBadCases() throws IOException {

        String[] badSqls = new String[] { //
                "create table a", // not select statement
                "select columnA, price from kylin_sales", // 'columnA' not found
                "select price from kylin_sales group by", // incomplete sql
                "select sum(lstg_format_name) from kylin_sales" // can not apply sum to 'lstg_format_name'
        };

        SqlSyntaxValidator validator = new SqlSyntaxValidator(kylinConfig, proj, queryExecutor);
        final Map<String, SQLValidateResult> badResults = validator.batchValidate(badSqls);
        printSqlValidateResults(badResults);
        badResults.forEach((key, sqlValidateResult) -> Assert.assertFalse(sqlValidateResult.isCapable()));
    }
}
