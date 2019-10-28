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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.util;

import java.sql.SQLException;
import java.util.Properties;

import io.kyligence.kap.query.util.ConvertToComputedColumn;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.query.util.SparkSQLFunctionConverter;

public class QueryUtilTest {

    @Test
    public void testMassageSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());

            String sql = "SELECT * FROM TABLE";
            String newSql = QueryUtil.massageSql(config, sql, "", 100, 20, "", true);
            Assert.assertEquals("SELECT * FROM TABLE\nLIMIT 100\nOFFSET 20", newSql);

            String sql2 = "SELECT SUM({fn convert(0, INT)}) from TABLE";
            String newSql2 = QueryUtil.massageSql(config, sql2, "", 0, 0, "", true);
            Assert.assertEquals("SELECT SUM({fn convert(0, INT)}) from TABLE", newSql2);
        }
    }

    @Test
    public void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";
            String massagedSql = QueryUtil.massagePushDownSql(config, sql, "", "default", false);
            String expectedSql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM(CAST(0 AS BIGINT)) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testInit() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());
            Assert.assertEquals(0, QueryUtil.queryTransformers.size());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof DefaultQueryTransformer);

            config.setProperty("kylin.query.transformers", KeywordDefaultDirtyHack.class.getCanonicalName());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof KeywordDefaultDirtyHack);

            QueryUtil.initQueryTransformersIfNeeded(config, false);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof KeywordDefaultDirtyHack);

            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName() + "," + ConvertToComputedColumn.class.getCanonicalName());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(2, QueryUtil.queryTransformers.size());
            QueryUtil.initQueryTransformersIfNeeded(config, false);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof DefaultQueryTransformer);
        }
    }

    @Test
    public void testMakeErrorMsgUserFriendly() {
        Assert.assertTrue(
                QueryUtil.makeErrorMsgUserFriendly(new SQLException(new NoSuchTableException("default", "test_ab")))
                        .contains("default"));

        final Exception exception = new IllegalStateException("\tThere is no column\t'age' in table 'test_kylin_fact'.\n"
                + "Please contact Kyligence Enterprise technical support for more details.\n");
        final String errorMsg = QueryUtil.makeErrorMsgUserFriendly(exception);
        Assert.assertEquals("There is no column\t'age' in table 'test_kylin_fact'.\n"
                + "Please contact Kyligence Enterprise technical support for more details.", errorMsg);
    }

    @Test
    public void testRemoveCommentInSql() {
        //test remove comment when last comment is --
        Assert.assertEquals(
                "select sum(ITEM_COUNT) \nfrom TEST_KYLIN_FACT  \ngroup by CAL_DT  \n" + "order by SELLER_ID;  \n\n\n",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */\n--6"));
        Assert.assertEquals(
                "select sum(ITEM_COUNT) \nfrom TEST_KYLIN_FACT  \ngroup by CAL_DT  \n" + "order by SELLER_ID  \n\n\n",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */\n--6"));

        //test remove comment when last comment is /* */
        Assert.assertEquals(
                "select sum(ITEM_COUNT) \nfrom TEST_KYLIN_FACT  \ngroup by CAL_DT  \n" + "order by SELLER_ID;  \n\n",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */"));
        Assert.assertEquals(
                "select sum(ITEM_COUNT) \nfrom TEST_KYLIN_FACT  \ngroup by CAL_DT  \n" + "order by SELLER_ID  \n\n",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */"));

        //test remove comment when comment contain ''
        Assert.assertEquals(
                "select sum(ITEM_COUNT) 'sum_count' \nfrom TEST_KYLIN_FACT 'table' ",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) 'sum_count' -- 'comment' \nfrom TEST_KYLIN_FACT 'table' --comment"));
        Assert.assertEquals(
                "select sum(ITEM_COUNT) ",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) -- 'comment' --"));

        //test remove comment when comment contain , \t /
        Assert.assertEquals(
                "select sum(ITEM_COUNT) ",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) -- , --\t --/ --"));

        Assert.assertEquals("select 1 ", QueryUtil.removeCommentInSql("select 1 --注释"));
        Assert.assertEquals("select 1 ", QueryUtil.removeCommentInSql("select 1 /* 注释 */"));
        Assert.assertEquals("select 1\t", QueryUtil.removeCommentInSql("select 1\t--注释"));
        Assert.assertEquals("select 1\t", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assert.assertEquals("select 1\n", QueryUtil.removeCommentInSql("select 1\n--注释"));
        Assert.assertEquals("select 1\t", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assert.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,--注释\n2"));
        Assert.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,/* 注释 */\n2"));
        Assert.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4/-- 注释\n2"));
        Assert.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4//* 注释 */\n2"));

        Assert.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'--注释''"));
        Assert.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'/* 注释 */"));

        Assert.assertEquals("select 1;", QueryUtil.removeCommentInSql("select 1;--注释"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1--注释"));

        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'"));
        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'--注释"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'--注释"));
        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'/*注释*/"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'/*注释*/"));

        Assert.assertEquals("select 1 \"--注释\"", QueryUtil.removeCommentInSql("select 1 \"--注释\""));
        Assert.assertEquals("select 1 \"apache's kylin\"", QueryUtil.removeCommentInSql("select 1 \"apache's kylin\""));
    }
}
