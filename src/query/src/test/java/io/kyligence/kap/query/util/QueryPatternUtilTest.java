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
package io.kyligence.kap.query.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class QueryPatternUtilTest {
    private static final String SQL_DIR = "../query/src/test/resources/query_pattern";

    @Test
    public void testJdbcFn() {
        String expected1 = "SELECT {fn SECOND({fn CONVERT({fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) }, "
                + "SQL_TIMESTAMP) }) } \"TEMP_TEST__2143701310__0_\", 1 \"X__ALIAS__0\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" //
                + "GROUP BY 1";
        String sql1 = "SELECT {fn SECOND({fn CONVERT({fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) }, SQL_TIMESTAMP) }) } "
                + "TEMP_TEST__2143701310__0_, 1 X__ALIAS__0\n" //
                + "FROM TDVT.CALCS CALCS GROUP BY 1";
        checkPattern(expected1, sql1);

        String expected2 = "SELECT {fn CONVERT(1, SQL_DOUBLE) }\n" + "FROM \"KYLIN_SALES\"\n"
                + "WHERE {fn CONVERT('1', SQL_DOUBLE) } = 1";
        String sql2 = "select {fn convert(1, double)} from kylin_sales where {fn convert('2', double)} = 2.0";
        checkPattern(expected2, sql2);

        String expected3 = "SELECT {fn CONVERT(1, SQL_CHAR) }\n" + "FROM \"KYLIN_SALES\"\n"
                + "WHERE {fn CONVERT('A', SQL_CHAR) } = 'A'";
        String sql3 = "select {fn convert(1, char)} from kylin_sales where {fn convert('abc', char)} = 'abc'";
        checkPattern(expected3, sql3);

        String expected4 = "SELECT {fn CONVERT('123.34', SQL_DOUBLE) }\nFROM \"KYLIN_SALES\"\n"
                + "WHERE {fn CONVERT('A', SQL_VARCHAR) } = 'A'";
        String sql4 = "select {fn convert('123.34', double)} from kylin_sales where {fn convert('apple', varchar)} = 'apple'";
        checkPattern(expected4, sql4);
    }

    @Test
    public void testCastClause() {
        String sql1 = "SELECT period_code, period_name, premium_flag  FROM (SELECT DISTINCT dim_period_week_d.period_code AS period_code, dim_period_week_d.period_name AS period_name, '1060101' AS premium_flag  FROM frpdb. dim_period_week_d dim_period_week_d   UNION  SELECT DISTINCT dim_period_week_view.period_code AS period_code,  dim_period_week_view.period_name AS period_name, '1060102' AS premium_flag    FROM frpdb. dim_period_week_view dim_period_week_view) period_d  WHERE period_d.premium_flag = '1060101'  AND substring(period_d.period_code, 1,   6) || '01' BETWEEN   CAST(CAST('20190430' AS bigint) - 50000 AS  VARCHAR(10)) AND '20190430' AND period_d.period_code >= '200001-1'  AND CAST(substring(period_d.period_code, 1, 4) AS sql_bigint) > 2016  ORDER BY period_code DESC";
        String expected1 = "SELECT \"PERIOD_CODE\", \"PERIOD_NAME\", \"PREMIUM_FLAG\"\n"
                + "FROM (SELECT DISTINCT \"DIM_PERIOD_WEEK_D\".\"PERIOD_CODE\" \"PERIOD_CODE\", \"DIM_PERIOD_WEEK_D\".\"PERIOD_NAME\" \"PERIOD_NAME\", '1060101' \"PREMIUM_FLAG\"\n"
                + "FROM \"FRPDB\".\"DIM_PERIOD_WEEK_D\" \"DIM_PERIOD_WEEK_D\"\n" + "UNION\n"
                + "SELECT DISTINCT \"DIM_PERIOD_WEEK_VIEW\".\"PERIOD_CODE\" \"PERIOD_CODE\", \"DIM_PERIOD_WEEK_VIEW\".\"PERIOD_NAME\" \"PERIOD_NAME\", '1060102' \"PREMIUM_FLAG\"\n"
                + "FROM \"FRPDB\".\"DIM_PERIOD_WEEK_VIEW\" \"DIM_PERIOD_WEEK_VIEW\") \"PERIOD_D\"\n"
                + "WHERE \"PERIOD_D\".\"PREMIUM_FLAG\" = 'A' AND SUBSTRING(\"PERIOD_D\".\"PERIOD_CODE\" FROM 1 FOR 1) || 'A' BETWEEN CAST(CAST('1' AS BIGINT) - 1 AS VARCHAR(10)) AND 'A' AND \"PERIOD_D\".\"PERIOD_CODE\" >= 'A' AND CAST(SUBSTRING(\"PERIOD_D\".\"PERIOD_CODE\" FROM 1 FOR 1) AS \"SQL_BIGINT\") > 1\n"
                + "ORDER BY \"PERIOD_CODE\" DESC";
        checkPattern(expected1, sql1);

        String sql2 = "SELECT period_code, period_name, premium_flag  FROM (SELECT DISTINCT dim_period_week_d.period_code AS period_code, dim_period_week_d.period_name AS period_name, '1060101' AS premium_flag  FROM frpdb. dim_period_week_d dim_period_week_d   UNION  SELECT DISTINCT dim_period_week_view.period_code AS period_code,  dim_period_week_view.period_name AS period_name, '1060102' AS premium_flag    FROM frpdb. dim_period_week_view dim_period_week_view) period_d  WHERE period_d.premium_flag = '1060101'  AND substring(period_d.period_code, 1,   6) || '01' BETWEEN   CAST(CAST('2019-04-30' AS sql_date) - 50000 AS  VARCHAR(10)) AND '20190430' AND period_d.period_code >= '200001-1'  AND CAST('2048.0' AS sql_decimal) > 2016  ORDER BY period_code DESC";
        String expected2 = "SELECT \"PERIOD_CODE\", \"PERIOD_NAME\", \"PREMIUM_FLAG\"\n"
                + "FROM (SELECT DISTINCT \"DIM_PERIOD_WEEK_D\".\"PERIOD_CODE\" \"PERIOD_CODE\", \"DIM_PERIOD_WEEK_D\".\"PERIOD_NAME\" \"PERIOD_NAME\", '1060101' \"PREMIUM_FLAG\"\n"
                + "FROM \"FRPDB\".\"DIM_PERIOD_WEEK_D\" \"DIM_PERIOD_WEEK_D\"\n" + "UNION\n"
                + "SELECT DISTINCT \"DIM_PERIOD_WEEK_VIEW\".\"PERIOD_CODE\" \"PERIOD_CODE\", \"DIM_PERIOD_WEEK_VIEW\".\"PERIOD_NAME\" \"PERIOD_NAME\", '1060102' \"PREMIUM_FLAG\"\n"
                + "FROM \"FRPDB\".\"DIM_PERIOD_WEEK_VIEW\" \"DIM_PERIOD_WEEK_VIEW\") \"PERIOD_D\"\n"
                + "WHERE \"PERIOD_D\".\"PREMIUM_FLAG\" = 'A' AND SUBSTRING(\"PERIOD_D\".\"PERIOD_CODE\" FROM 1 FOR 1) || 'A' BETWEEN CAST(CAST('2010-01-01' AS \"SQL_DATE\") - 1 AS VARCHAR(10)) AND 'A' AND \"PERIOD_D\".\"PERIOD_CODE\" >= 'A' AND CAST('1' AS \"SQL_DECIMAL\") > 1\n"
                + "ORDER BY \"PERIOD_CODE\" DESC";
        checkPattern(expected2, sql2);
    }

    @Test
    public void testIdempotence() {
        String expected = "SELECT \"Calcs\".\"KEY\" \"KEY\", SUM(\"Calcs\".\"NUM2\") \"sum_num2_ok\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" //
                + "GROUP BY \"Calcs\".\"KEY\"\n" //
                + "ORDER BY SUM(\"Calcs\".\"NUM2\") DESC\n" //
                + "LIMIT 1\n" //
                + "OFFSET 1";
        String sql = "SELECT \"Calcs\".\"KEY\" AS \"KEY\", SUM(\"Calcs\".\"NUM2\") AS \"sum_num2_ok\"\n"
                + "FROM \"TDVT\".\"CALCS\" Calcs\n" //
                + "GROUP BY \"Calcs\".\"KEY\"\n\n" //
                + "ORDER BY SUM(\"Calcs\".\"NUM2\") DESC\n" //
                + "LIMIT 10 " //
                + "OFFSET 2";
        checkPattern(expected, sql);

        String normalizeSQLPattern = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        checkPattern(expected, normalizeSQLPattern);
    }

    @Test
    public void testComputedColumnCompatibility() throws IOException {
        String expected = retrieveSql("query01.sql.expected");
        String sql = retrieveSql("query01.sql");
        checkPattern(expected, sql);

        String expected2 = retrieveSql("query02.sql.expected");
        String sql2 = retrieveSql("query02.sql");
        checkPattern(expected2, sql2);

        String expected3 = retrieveSql("query03.sql.expected");
        String sql3 = retrieveSql("query03.sql");
        checkPattern(expected3, sql3);

        String expected4 = retrieveSql("query04.sql.expected");
        String sql4 = retrieveSql("query04.sql");
        checkPattern(expected4, sql4);
    }

    @Test
    public void testGroupBy() {
        String expected = "SELECT SUM(1) \"COL\", 2 \"COL2\"\n"
                + "FROM (SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\", \"TEST_CAL_DT\".\"WEEK_BEG_DT\", "
                + "SUM(\"TEST_KYLIN_FACT\".\"PRICE\") \"GMV\", COUNT(*) \"TRANS_CNT\"\n" //
                + "FROM \"TEST_KYLIN_FACT\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" \"TEST_CAL_DT\" ON \"TEST_KYLIN_FACT\".\"CAL_DT\" = \"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "WHERE \"TEST_CAL_DT\".\"WEEK_BEG_DT\" BETWEEN DATE '2010-01-01' AND DATE '2010-01-01'\n"
                + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\", \"TEST_CAL_DT\".\"WEEK_BEG_DT\"\n"
                + "HAVING SUM(\"PRICE\" + 2 * 4) * 1 > 1) \"TABLEAUSQL\"\n" //
                + "GROUP BY 2\n" //
                + "HAVING COUNT(1) > 1";
        String sql = "select sum(1) as col, 2 as col2 \n" //
                + " from ( \n" //
                + "   select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt, "
                + "     sum(test_kylin_fact.price) as gmv, count(*) as trans_cnt \n" //
                + "   from test_kylin_fact \n"
                + "   inner join edw.test_cal_dt as test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt \n"
                + "   where test_cal_dt.week_beg_dt between date '2013-05-01' and date '2013-08-01' \n"
                + "   group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt having sum(price+2*4) * 2>500 \n"
                + " ) tableausql \n" //
                + " group by 2 having count(1)>0 ";
        checkPattern(expected, sql);
    }

    @Test
    public void testHaving() throws IOException {
        String expected = retrieveSql("query05.sql.expected");
        String sql = retrieveSql("query05.sql");
        checkPattern(expected, sql);
    }

    @Test
    public void testSelectLiteral() {

        // test select SqlNumericLiteral
        String expected1 = "SELECT 1, 2, 3\nFROM \"A\"";
        String sql1 = "select 1, 2, 3 from a";
        checkPattern(expected1, sql1);

        // test select SqlCharStringLiteral
        String expected2 = "SELECT 'text', 'test', 'task'\nFROM \"KYLIN_SALES\"";
        String sql2 = "select 'text', 'test', 'task' from kylin_sales";
        checkPattern(expected2, sql2);

        // test select SqlAbstractDateTimeLiteral
        String expected3 = "SELECT DATE '2012-01-01', TIMESTAMP '2012-02-01 00:00:00.456', TIME '00:00:01'\n"
                + "FROM \"KYLIN_SALES\"";
        String sql3 = "select date '2012-01-01', TIMESTAMP '2012-02-01 00:00:00.456', time '00:00:01' from kylin_sales";
        checkPattern(expected3, sql3);
        sql3 = "select {d '2012-01-01'}, {ts '2012-02-01 00:00:00.456'}, {t '00:00:01'} from kylin_sales";
        checkPattern(expected3, sql3);

        // test all
        String expected4 = "SELECT 1 \"num_literal\", 'test' \"str_literal\", DATE '2012-01-01' \"datetime_literal\"\n"
                + "FROM \"KYLIN_SALES\"";
        String sql4 = "select 1 as \"num_literal\", 'test' as \"str_literal\", DATE '2012-01-01' as \"datetime_literal\""
                + " \t from kylin_sales";
        checkPattern(expected4, sql4);
    }

    @Test
    public void testLiteralComparison() {

        // test with SqlNumericalLiteral
        {
            String expected1 = "SELECT \"KS\".\"PRICE\" \"PRICE\", \"KS\".\"PART_DT\" \"DT\"\n"
                    + "FROM \"KYLIN_SALES\" \"KS\"\n"
                    + "WHERE NOT \"KS\".\"PRICE\" <= 2 OR \"KS\".\"PRICE\" > 1 AND \"KS\".\"PRICE\" < 2 OR \"KS\".\"PRICE\" <= 2";
            String sql1 = "select ks.price as price, ks.part_dt as dt from kylin_sales as ks "
                    + "where not(ks.price <= 60.0) or (ks.price > 10 and ks.price < 20) or ks.price <= 5";
            checkPattern(expected1, sql1);

            String expected2 = "SELECT \"SELLER_ID\", \"PRICE\" > 10, SUM(\"ITEM_COUNT\")\n"
                    + "FROM \"TEST_KYLIN_FACT\"\n" //
                    + "GROUP BY \"SELLER_ID\", \"PRICE\"";
            String sql2 = "select seller_id, price > 10, sum(item_count) from test_kylin_fact group by seller_id, price";
            checkPattern(expected2, sql2);

            String expected3 = "SELECT {fn CONVERT('123', SQL_INTEGER) }\n" //
                    + "FROM \"KYLIN_SALES\"\n" //
                    + "WHERE {fn CONVERT('1', SQL_INTEGER) } = 1";
            String sql3 = "select {fn convert('123', integer)} from kylin_sales where {fn convert('123', integer)} = 123";
            checkPattern(expected3, sql3);

            String expected4 = "SELECT {fn CONVERT('123.34', SQL_DOUBLE) }\n" //
                    + "FROM \"KYLIN_SALES\"\n" //
                    + "WHERE {fn CONVERT('1', SQL_DOUBLE) } = 1";
            String sql4 = "select {fn convert('123.34', double)} from kylin_sales where {fn convert('123.34', double)} = 123.34";
            checkPattern(expected4, sql4);
        }

        // test with SqlAbstractDateTimeLiteral
        {
            String expected1 = "SELECT \"KS\".\"PART_DT\" <= '2008-04-23'\n" //
                    + "FROM \"KYLIN_SALES\" \"KS\"";
            String sql1 = "select ks.part_dt <= '2008-04-23' from kylin_sales as ks";
            checkPattern(expected1, sql1);

            String expected2 = "SELECT \"PRICE\", \"DT\"\n" //
                    + "FROM \"SALES\"\n" //
                    + "WHERE \"DT\" <= '2010-01-02' AND \"DT\" > DATE '2010-01-01'";
            String sql2 = "select price, dt from sales where dt <= '2020-08-08' and dt > date '2002-02-08'";
            checkPattern(expected2, sql2);

            String expected3 = "SELECT \"LDT\"\n" //
                    + "FROM \"SALES\"\n" //
                    + "WHERE \"LDT\" > TIMESTAMP '2010-01-01 00:00:00' OR \"LDT\" < TIMESTAMP '2010-01-02 00:00:00'";
            String sql3 = "select ldt from sales where ldt > {ts '2012-01-01 00:00:00'} or ldt < TIMESTAMP '2008-02-12 23:59:59'";
            checkPattern(expected3, sql3);

            String expected4 = "SELECT 1\n" //
                    + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" //
                    + "WHERE {fn SECOND({fn CONVERT({fn CONVERT('2010-01-01 00:00:00', SQL_TIMESTAMP) }, SQL_TIMESTAMP) }) } > 1";
            String sql4 = "SELECT 1 FROM TDVT.CALCS CALCS "
                    + "where {fn SECOND({fn CONVERT({fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) }, SQL_TIMESTAMP) }) } > 10";
            checkPattern(expected4, sql4);

            String expected5 = "SELECT {fn CONVERT('2016-07-15 10:11:12.123', SQL_TIMESTAMP) } \"Calculation_958703807427547136\"\n"
                    + "FROM \"TDVT\".\"CALCS\" \"Calcs\"\n"
                    + "WHERE {fn CONVERT('2010-01-01 00:00:00', SQL_TIMESTAMP) } = TIMESTAMP '2010-01-01 00:00:00'\n"
                    + "HAVING COUNT(1) > 1";
            String sql5 = "SELECT {fn CONVERT('2016-07-15 10:11:12.123', SQL_TIMESTAMP)} AS \"Calculation_958703807427547136\"\n"
                    + "FROM \"TDVT\".\"CALCS\" \"Calcs\"\n"
                    + "WHERE ({fn CONVERT('2016-07-15 10:11:12.123', SQL_TIMESTAMP)} = {ts '2016-07-15 10:11:12.123'})\n"
                    + "HAVING (COUNT(1) > 0)";
            checkPattern(expected5, sql5);

            String expected6 = "SELECT CAST('2013-01-01 00:00:00' AS TIMESTAMP), CAST('2013-01-01' AS DATE), CAST('10:00:00' AS TIME)\n"
                    + "FROM \"TEST_KYLIN_FACT\"\n" //
                    + "LIMIT 1";
            String sql6 = "select CAST('2013-01-01 00:00:00' AS TIMESTAMP), CAST('2013-01-01' AS DATE), CAST('10:00:00' AS TIME)"
                    + " from test_kylin_fact limit 10";
            checkPattern(expected6, sql6);

            String expected7 = "SELECT CAST('2013-01-01 00:00:00' AS TIMESTAMP), CAST('2013-01-01' AS DATE), CAST('10:00:00' AS TIME)\n"
                    + "FROM \"TEST_KYLIN_FACT\"\n"
                    + "WHERE CAST('2010-01-01 00:00:00' AS TIMESTAMP) = TIMESTAMP '2010-01-01 00:00:00'";
            String sql7 = "select CAST('2013-01-01 00:00:00' AS TIMESTAMP), CAST('2013-01-01' AS DATE), CAST('10:00:00' AS TIME)"
                    + " from test_kylin_fact "
                    + " where (CAST('2013-01-01 00:00:00' AS TIMESTAMP) = {ts '2013-01-01 00:00:00'})";
            checkPattern(expected7, sql7);
        }

        // test with SqlCharStringLiteral
        {
            String expected = "SELECT \"SELLER_NAME\" > 'abc', \"LSTG_FORMAT_NAME\"\n" + "FROM \"SALES\"\n"
                    + "WHERE \"LSTG_FORMAT_NAME\" = 'A'";
            String sql = "select seller_name > 'abc', lstg_format_name from sales where lstg_format_name = 'kylin'";
            checkPattern(expected, sql);
        }
        // test two SqlCharStringLiterals
        {
            String expected1 = "SELECT DISTINCT '1103102' \"PREMIUM_CODE\", '否' \"PREMIUM_NAME\"\n"
                    + "FROM \"FRPDB\".\"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE 'A' = 'A' AND 'A' = 'Z'\n" + "UNION ALL\n"
                    + "SELECT \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" \"PREMIUM_CODE\", \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_NAME\" \"PREMIUM_NAME\"\n"
                    + "FROM \"FRPDB\".\"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" IN ('A') AND 'A' <> 'A'";
            String sql1 = "SELECT DISTINCT '1103102' AS PREMIUM_CODE, '否' AS PREMIUM_NAME  FROM FRPDB.DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE '1030101' = '1030101' AND '104292' = '392101' UNION ALL SELECT DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE AS PREMIUM_CODE,       DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_NAME AS PREMIUM_NAME  FROM FRPDB.DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE IN       ('1103101', '1103102')   AND '1030101' <> '1030101'";
            checkPattern(expected1, sql1);

            String expected2 = "SELECT DISTINCT '1103102' \"PREMIUM_CODE\", '否' \"PREMIUM_NAME\"\n"
                    + "FROM \"FRPDB\".\"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE 'A' = 'A' AND 'A' = 'A' AND 'A' = 'Z'\n" + "UNION ALL\n"
                    + "SELECT \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" \"PREMIUM_CODE\", \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_NAME\" \"PREMIUM_NAME\"\n"
                    + "FROM \"FRPDB\".\"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" IN ('A') AND 'A' <> 'Z'";
            String sql2 = "SELECT DISTINCT '1103102' AS PREMIUM_CODE, '否' AS PREMIUM_NAME  FROM FRPDB.DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE 'kylin' = 'kylin' AND 2 = 2 AND 'table' = 'desk' UNION ALL SELECT DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE AS PREMIUM_CODE,       DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_NAME AS PREMIUM_NAME  FROM FRPDB.DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE IN       ('1103101', '1103102')   AND 'dimension' <> 'measure'";
            checkPattern(expected2, sql2);
        }
    }

    @Test
    public void testColumnComparison() {
        String expected = "SELECT \"B\".\"FIRSTNAME\" \"FIRSTNAME1\", \"B\".\"LASTNAME\" \"LASTNAME1\", "
                + "\"A\".\"FIRSTNAME\" \"FIRSTNAME2\", \"A\".\"LASTNAME\" \"LASTNAME2\", \"B\".\"CITY\", \"B\".\"COUNTRY\"\n"
                + "FROM \"SALES\".\"CUSTOMER\" \"A\",\n" //
                + "\"SALES\".\"CUSTOMER\" \"B\"\n" //
                + "WHERE \"A\".\"ID\" <> \"B\".\"ID\" AND \"A\".\"CITY\" = \"B\".\"CITY\" "
                + "AND \"A\".\"COUNTRY\" = \"B\".\"COUNTRY\"";
        String sql = "select b.firstname as firstname1, b.lastname as lastname1,\n"
                + "a.firstname as firstname2, a.lastname as lastname2, b.city, b.country\n"
                + "from sales.customer a, sales.customer b\n"
                + "where (a.id <> b.id and a.city = b.city and a.country = b.country)\n";
        // SqlIdentifiers should remain unchanged
        checkPattern(expected, sql);
    }

    @Test
    public void testStringComparison() {
        String expected = "SELECT \"KS\".\"PRICE\" \"PRICE\"\n" + "FROM \"KYLIN_SALES\" \"KS\"\n"
                + "WHERE \"KS\".\"DT\" <= 'Z' AND \"KS\".\"DT\" > 'A'";
        String sql = "select ks.price as price from kylin_sales as ks \n"
                + "where ks.dt <= '200310' and ks.dt > '2012-02'";
        checkPattern(expected, sql);
    }

    @Test
    public void testQueryWithSamePattern() {
        String sql1 = "select s.name, s.price " //
                + "from kylin_sales as s inner join kylin_account as a " //
                + "on s.name = a.name and s.price > 100" //
                + "where s.account is not null";
        String sql2 = "select s.name, s.price " //
                + "from kylin_sales as s inner join kylin_account a " //
                + "on s.name = a.name and s.price > 8000" //
                + "where s.account is not null";
        String pattern1 = QueryPatternUtil.normalizeSQLPatternImpl(sql1);
        String pattern2 = QueryPatternUtil.normalizeSQLPatternImpl(sql2);
        Assert.assertEquals(pattern1, pattern2);
    }

    @Test
    public void testInAndNotIn() {

        // test with SqlNumericLiteral
        {
            String expected = "SELECT \"FIRST_NAME\", \"LAST_NAME\"\n" //
                    + "FROM \"SALES\"\n" //
                    + "WHERE \"PHONE_NUMBER\" NOT IN (1)";
            String sql1 = "select first_name, last_name from sales where phone_number not in (10, 20, 30)";
            String sql2 = "select first_name, last_name from sales where phone_number not in (1000, 2000, 3000)";
            checkPattern(expected, sql1);
            checkPattern(expected, sql2);

            String expected3 = "SELECT \"PRODUCTNAME\"\n" //
                    + "FROM \"PRODUCT\"\n" //
                    + "WHERE \"ID\" IN (SELECT \"PRODUCTID\"\n" //
                    + "FROM \"ORDERITEM\"\n" //
                    + "WHERE \"QUANTITY\" IN (1))";
            String sql3 = "SELECT ProductName FROM Product WHERE Id IN (SELECT ProductId \n"
                    + "FROM OrderItem WHERE Quantity IN (100, 200))";
            checkPattern(expected3, sql3);
        }

        // test with SqlCharStringLiteral
        {
            String expected1 = "SELECT \"FIRST_NAME\", \"LAST_NAME\"\n" //
                    + "FROM \"SALES\"\n" //
                    + "WHERE \"FIRST_NAME\" IN ('A')";
            String sql1 = "select first_name, last_name from sales where first_name in ('Sam', 'Jobs')";
            checkPattern(expected1, sql1);

            String expected2 = "SELECT SUM(\"PRICE\") \"GMV\", \"LSTG_FORMAT_NAME\" \"FORMAT_NAME\"\n"
                    + "FROM \"TEST_KYLIN_FACT\"\n"
                    + "WHERE \"LSTG_FORMAT_NAME\" IN ('A') OR \"LSTG_FORMAT_NAME\" >= 'A' AND \"LSTG_FORMAT_NAME\" <= 'Z'\n"
                    + "GROUP BY \"LSTG_FORMAT_NAME\"";
            String sql2 = "select sum(PRICE) as GMV, LSTG_FORMAT_NAME as FORMAT_NAME from test_kylin_fact "
                    + "where (LSTG_FORMAT_NAME in ('ABIN')) or (LSTG_FORMAT_NAME >= 'FP-GTC' "
                    + "and LSTG_FORMAT_NAME <= 'Others') group by LSTG_FORMAT_NAME";
            checkPattern(expected2, sql2);

            String expected3 = "SELECT \"PRODUCTNAME\"\n" //
                    + "FROM \"PRODUCT\"\n" //
                    + "WHERE \"ID\" IN ('A', (SELECT \"PRODUCTID\"\n" //
                    + "FROM \"ORDERITEM\"\n" //
                    + "WHERE \"QUANTITY\" IN (1)))";
            String sql3 = "SELECT ProductName FROM Product WHERE Id IN ('a', 'b', (SELECT ProductId "
                    + "FROM OrderItem WHERE Quantity IN (100, 200)))";
            checkPattern(expected3, sql3);
        }

        // test with SqlAbstractDateTimeLiteral
        {
            String expected1 = "SELECT \"PART_DT\", SUM(\"PRICE\")\n" //
                    + "FROM \"KYLIN_SALES\"\n" //
                    + "WHERE \"PART_DT\" IN (DATE '2010-01-01')\n" //
                    + "GROUP BY \"PART_DT\"";
            String sql1 = "select part_dt, sum(price) from kylin_sales"
                    + " where part_dt in ({d '2012-01-01'}, {d '2012-01-01'}, {d '2012-01-01'})" //
                    + " group by part_dt";
            checkPattern(expected1, sql1);
            sql1 = "select part_dt, sum(price) from kylin_sales"
                    + " where part_dt in (DATE '2012-01-01', DATE '2012-01-01', DATE '2012-01-01')" //
                    + " group by part_dt";
            checkPattern(expected1, sql1);

            String expected2 = "SELECT \"T_COL\"\n" //
                    + "FROM \"TBL\"\n" //
                    + "WHERE \"T_COL\" IN (TIME '01:00:00')\n" //
                    + "GROUP BY \"T_COL\"";
            String sql2 = "select t_col from tbl "
                    + "where t_col in ({t '21:07:32'}, {t '22:42:43'}, {t '04:57:51'}, {t '18:51:48'}) "
                    + "group by t_col";
            checkPattern(expected2, sql2);
            sql2 = "select t_col from tbl "
                    + "where t_col in (TIME '21:07:32', TIME '22:42:43', TIME '04:57:51', TIME '18:51:48') "
                    + "group by t_col";
            checkPattern(expected2, sql2);

            String expected3 = "SELECT \"TS_COL\", COUNT(\"ITEM_ID\")\n" //
                    + "FROM \"TBL\"\n" //
                    + "WHERE \"PART_DT\" IN (TIMESTAMP '2010-01-01 00:00:00')\n" //
                    + "GROUP BY \"PART_DT\"";
            String sql3 = "select ts_col, count(item_id) from tbl"
                    + " where part_dt in ({ts '1899-12-30 21:07:32'}, {ts '1900-01-01 04:57:51'}, {ts '1900-01-01 18:51:48'})"
                    + " group by part_dt";
            checkPattern(expected3, sql3);
            sql3 = "select ts_col, count(item_id) from tbl"
                    + " where part_dt in (timestamp '1899-12-30 21:07:32', timestamp '1900-01-01 04:57:51', timestamp '1900-01-01 18:51:48')"
                    + " group by part_dt";
            checkPattern(expected3, sql3);
        }
        // test SqlCharStringLiteral in ('blah', 'blah')
        {
            String sql = "SELECT distinct '1050101' AS TIER_CODE, '分公司' AS TIER_NAME FROM DIM_OPTIONAL_DIMENSION_D WHERE '54' not in ('52','62','63') and '56' in ('45', '56', '75') UNION ALL SELECT DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE AS TIER_CODE, DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_NAME AS TIER_NAME FROM DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE '54' in('52','63') and '36' not in ('36', '45') and case when '54'='52' then DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE IN ('1050101', '1050102', '1050103', '1050104', '1050105', '1050107') when '54'='63' then DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE IN ('1050101', '1050102', '1050103') end union all SELECT DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE AS TIER_CODE, DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_NAME AS TIER_NAME FROM DIM_OPTIONAL_DIMENSION_D DIM_OPTIONAL_DIMENSION_D WHERE '54' = '62' and DIM_OPTIONAL_DIMENSION_D.OPTIONAL_DIMENSION_CODE IN ('1050106', '1050108', '1050109')";
            String expected = "SELECT DISTINCT '1050101' \"TIER_CODE\", '分公司' \"TIER_NAME\"\n"
                    + "FROM \"DIM_OPTIONAL_DIMENSION_D\"\n" + "WHERE 'Z' NOT IN ('A') AND 'A' IN ('A')\n"
                    + "UNION ALL\n"
                    + "SELECT \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" \"TIER_CODE\", \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_NAME\" \"TIER_NAME\"\n"
                    + "FROM \"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE 'Z' IN ('A') AND 'A' NOT IN ('A') AND CASE WHEN 'A' = 'Z' THEN \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" IN ('A') WHEN 'A' = 'Z' THEN \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" IN ('A') ELSE NULL END\n"
                    + "UNION ALL\n"
                    + "SELECT \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" \"TIER_CODE\", \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_NAME\" \"TIER_NAME\"\n"
                    + "FROM \"DIM_OPTIONAL_DIMENSION_D\" \"DIM_OPTIONAL_DIMENSION_D\"\n"
                    + "WHERE 'A' = 'Z' AND \"DIM_OPTIONAL_DIMENSION_D\".\"OPTIONAL_DIMENSION_CODE\" IN ('A')";
            checkPattern(expected, sql);
        }
    }

    @Test
    public void testLikeAndNotLike() {
        String expected = "SELECT \"META_CATEG_NAME\", \"META_CATEG_ID\", COUNT(*) \"CNT\"\n"
                + "FROM \"TEST_KYLIN_FACT\"\n" //
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" \"TEST_CAL_DT\" "
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\" = \"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "WHERE \"META_CATEG_NAME\" NOT LIKE 'A' AND \"META_CATE_ID\" LIKE 'A'\n"
                + "GROUP BY \"META_CATEG_NAME\"";
        String sql = "select META_CATEG_NAME, META_CATEG_ID, count(*) as cnt \n" //
                + "from test_kylin_fact \n" //
                + "inner JOIN edw.test_cal_dt as test_cal_dt\n" //
                + "ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "where META_CATEG_NAME not like '%ab%'\n " //
                + "and META_CATE_ID like '%08'" //
                + "group by META_CATEG_NAME";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBetweenAnd() {
        String expected = "SELECT SUM(\"L_EXTENDEDPRICE\") - SUM(\"L_SALEPRICE\") \"REVENUE\"\n"
                + "FROM \"V_LINEITEM\"\n"
                + "WHERE \"L_SHIPDATE\" >= DATE '2010-01-01' AND \"L_SHIPDATE\" < '2010-01-02' "
                + "AND \"L_DISCOUNT\" BETWEEN 1 AND 1 AND \"L_QUANTITY\" < 2";
        String sql = "select sum(l_extendedprice) - sum(l_saleprice) as revenue\n" //
                + "from v_lineitem\n" //
                + "where l_shipdate >= date '1993-01-01' and l_shipdate < '1994-01-01'\n"
                + "   and l_discount between 0.05 and 0.07 and l_quantity < 25";
        checkPattern(expected, sql);
    }

    @Test
    public void testDistinct() {
        String expected = "SELECT DISTINCT \"LSTG_FORMAT_NAME\"\n" + "FROM \"TEST_KYLIN_FACT\"";
        String sql = "SELECT distinct LSTG_FORMAT_NAME from test_kylin_fact";
        checkPattern(expected, sql);
    }

    @Test
    public void testH2Incapable() {
        String expected = "SELECT TIMESTAMPADD(DAY, 1, \"WEEK_BEG_DT\") \"X\", \"WEEK_BEG_DT\"\n"
                + "FROM \"TEST_KYLIN_FACT\"\n" //
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" \"TEST_CAL_DT\" "
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\" = \"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "GROUP BY \"TEST_CAL_DT\".\"WEEK_BEG_DT\"";
        String sql = "SELECT timestampadd(DAY,1,WEEK_BEG_DT) as x ,WEEK_BEG_DT\n" //
                + " FROM TEST_KYLIN_FACT \n"
                + " inner JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON (TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT) \n"
                + " GROUP BY TEST_CAL_DT.WEEK_BEG_DT";
        checkPattern(expected, sql);
    }

    @Test
    public void testUnion() {
        String expected = "SELECT \"KS\".\"PRICE\" \"PRICE\", \"KS\".\"PART_DT\" \"DT\"\n"
                + "FROM \"KYLIN_SALES\" \"KS\"\n"
                + "WHERE \"KS\".\"PRICE\" > 1 AND \"KS\".\"PART_DT\" > DATE '2010-01-01'\n" //
                + "UNION\n" //
                + "SELECT \"KY\".\"PRICE\" \"PRICE\", \"KY\".\"PART_DT\" \"DT\"\n" //
                + "FROM \"KYLIN_SALES\" \"KY\"\n" //
                + "WHERE \"KY\".\"PRICE\" < 2 AND \"KY\".\"PART_DT\" < DATE '2010-01-02'";
        String unionSql = "select ks.price as price, ks.part_dt as dt " //
                + "from kylin_sales ks " //
                + "where ks.price > 50 and ks.part_dt > date '2012-08-23' " + "union " //
                + "select ky.price as price, ky.part_dt as dt " //
                + "from kylin_sales ky " //
                + "where ky.price < 40 and ky.part_dt < date '2015-08-22'";
        checkPattern(expected, unionSql);
    }

    @Test
    public void testReverseComparison() {
        String expected = "SELECT \"KY\".\"PRICE\" \"PRICE\", \"KY\".\"PART_DT\" \"DT\"\n"
                + "FROM \"KYLIN_SALES\" \"KY\"\n"
                + "WHERE 2 >= \"KY\".\"PRICE\" AND DATE '2010-01-01' < \"KY\".\"PART_DT\" "
                + "AND '2010-01-02' > \"KY\".\"PART_DT\"";
        String sql = "select ky.price as price, ky.part_dt as dt " //
                + "from kylin_sales ky "
                + "where 40 >= ky.price and date '2015-08-22' < ky.part_dt and '2018-01-09' > ky.part_dt";
        checkPattern(expected, sql);
    }

    @Test
    public void testFunction() {
        String expected = "SELECT UPPER(\"LSTG_FORMAT_NAME\") \"LSTG_FORMAT_NAME\", COUNT(*) \"CNT\"\n"
                + "FROM \"TEST_KYLIN_FACT\"\n" //
                + "WHERE LOWER(\"LSTG_FORMAT_NAME\") = 'A' " //
                + "AND SUBSTRING(\"LSTG_FORMAT_NAME\" "
                + "FROM 1 FOR 1) IN ('A') AND UPPER(\"LSTG_FORMAT_NAME\") > 'A' "
                + "AND LOWER(\"LSTG_FORMAT_NAME\") LIKE 'A' " //
                + "AND CHAR_LENGTH(\"LSTG_FORMAT_NAME\") < 2 " //
                + "AND CHAR_LENGTH(\"LSTG_FORMAT_NAME\") > 1 " //
                + "AND 'A' || \"LSTG_FORMAT_NAME\" || 'A' || 'A' || 'A' = 'A'\n" //
                + "GROUP BY \"LSTG_FORMAT_NAME\"";
        String sql = "select upper(lstg_format_name) as lstg_format_name, count(*) as cnt from test_kylin_fact\n"
                + "where lower(lstg_format_name)='abin' and substring(lstg_format_name,1,3) in ('ABI') "
                + "and upper(lstg_format_name) > 'AAAA' and lower(lstg_format_name) like '%b%' "
                + "and char_length(lstg_format_name) < 10 and char_length(lstg_format_name) > 3 "
                + "and 'abc'||lstg_format_name||'a'||'b'||'c'='abcABINabc'\n" //
                + "group by lstg_format_name";
        checkPattern(expected, sql);
    }

    @Test
    public void testWindowFunction() {
        String expected = "SELECT *\n" //
                + "FROM (SELECT \"CAL_DT\", \"LSTG_FORMAT_NAME\", "
                + "SUM(\"PRICE\") \"GMV\", 100 * SUM(\"PRICE\") / (FIRST_VALUE(SUM(\"PRICE\")) "
                + "OVER (PARTITION BY \"LSTG_FORMAT_NAME\" ORDER BY CAST(\"CAL_DT\" AS TIMESTAMP) "
                + "RANGE INTERVAL '2' DAY PRECEDING)) \"last_day\", FIRST_VALUE(SUM(\"PRICE\")) "
                + "OVER (PARTITION BY \"LSTG_FORMAT_NAME\" ORDER BY CAST(\"CAL_DT\" AS TIMESTAMP) "
                + "RANGE CAST(366 AS INTERVAL DAY) PRECEDING)\n" //
                + "FROM \"TEST_KYLIN_FACT\" \"last_year\"\n" //
                + "WHERE \"CAL_DT\" BETWEEN '2010-01-01' AND '2010-01-01' OR \"CAL_DT\" "
                + "BETWEEN '2010-01-01' AND '2010-01-01' OR \"CAL_DT\" "
                + "BETWEEN '2010-01-01' AND '2010-01-01'\n"
                + "GROUP BY \"CAL_DT\", \"LSTG_FORMAT_NAME\") \"T\"\n"
                + "WHERE \"CAL_DT\" BETWEEN '2010-01-01' AND '2010-01-01'";
        String sql = "select * from(\n" //
                + "select cal_dt, lstg_format_name, sum(price) as GMV,\n"
                + "100 * sum(price) / first_value(sum(price)) over (partition by lstg_format_name "
                + "order by cast(cal_dt as timestamp) range interval '2' day PRECEDING) as \"last_day\",\n"
                + "first_value(sum(price)) over (partition by lstg_format_name "
                + "order by cast(cal_dt as timestamp) range cast(366 as INTERVAL day) preceding)\n"
                + "from test_kylin_fact as \"last_year\"\n"
                + "where cal_dt between '2013-01-08' and '2013-01-15' or cal_dt "
                + "between '2013-01-07' and '2013-01-15' or cal_dt between '2012-01-01' and '2012-01-15'\n"
                + "group by cal_dt, lstg_format_name)t\n" //
                + "where cal_dt between '2013-01-06' and '2013-01-15'";
        checkPattern(expected, sql);
    }

    @Test
    public void testWithSelect() {
        String expected = "WITH \"A\" AS (SELECT \"KS\".\"PRICE\" \"PRICE\", \"KS\".\"PART_DT\" \"DT\"\n"
                + "FROM \"KYLIN_SALES\" \"KS\"\n" + "WHERE \"KS\".\"PRICE\" > 1 AND \"KS\".\"PART_DT\" > '2010-01-01') "
                + "(SELECT \"A\".\"PRICE\", \"A\".\"DT\"\n" + "FROM \"A\"\n" + "WHERE \"A\".\"DT\" < '2010-01-02')";
        String withSelect = "with a as (" + "select ks.price as price, ks.part_dt as dt " + "from kylin_sales ks "
                + "where ks.price > 50 and ks.part_dt > '2012-07-08') "
                + "select a.price, a.dt from a where a.dt < '2015-08-09'";
        checkPattern(expected, withSelect);
    }

    @Test
    public void testSubQuery() {
        String expected = "SELECT \"A\".\"PRICE\", \"A\".\"DT\"\n"
                + "FROM (SELECT \"KS\".\"PRICE\" \"PRICE\", \"KS\".\"PART_DT\" \"DT\"\n"
                + "FROM \"KYLIN_SALES\" \"KS\"\n"
                + "WHERE \"KS\".\"PRICE\" > 1 AND \"KS\".\"PART_DT\" > '2010-01-01') \"A\"\n"
                + "WHERE \"A\".\"DT\" < '2010-01-02'";
        String sql = "select a.price, a.dt from ( " //
                + "select ks.price as price, ks.part_dt as dt " + "from kylin_sales ks "//
                + "where ks.price > 50 and ks.part_dt > '2012-07-08') a " //
                + "where a.dt < '2015-08-09'";
        checkPattern(expected, sql);
    }

    @Test
    public void testComplicatedCase() {
        String expected = "SELECT \"T1\".\"WEEK_BEG_DT\", \"T1\".\"SUM_PRICE\", \"T2\".\"CNT\"\n"
                + "FROM (SELECT \"TEST_CAL_DT\".\"WEEK_BEG_DT\", SUM(\"TEST_KYLIN_FACT\".\"PRICE\") \"SUM_PRICE\"\n"
                + "FROM \"DB1\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" \"TEST_CAL_DT\" "
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\" = \"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CATEGORY_GROUPINGS\" \"TEST_CATEGORY_GROUPINGS\" "
                + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\" = \"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" "
                + "AND \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\" = \"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SITES\" \"TEST_SITES\" "
                + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\" = \"TEST_SITES\".\"SITE_ID\"\n"
                + "WHERE \"TEST_KYLIN_FACT\".\"PRICE\" > 1\n" //
                + "GROUP BY \"TEST_CAL_DT\".\"WEEK_BEG_DT\"\n" //
                + "HAVING SUM(\"TEST_KYLIN_FACT\".\"PRICE\") > 1\n"
                + "ORDER BY SUM(\"TEST_KYLIN_FACT\".\"PRICE\")) \"T1\"\n"
                + "INNER JOIN (SELECT \"TEST_CAL_DT\".\"WEEK_BEG_DT\", COUNT(*) \"CNT\"\n"
                + "FROM \"DB1\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" \"TEST_CAL_DT\" "
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\" = \"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CATEGORY_GROUPINGS\" \"TEST_CATEGORY_GROUPINGS\" "
                + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\" = \"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" "
                + "AND \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\" = \"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SITES\" \"TEST_SITES\" "
                + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\" = \"TEST_SITES\".\"SITE_ID\"\n"
                + "GROUP BY \"TEST_CAL_DT\".\"WEEK_BEG_DT\") \"T2\" "
                + "ON \"T1\".\"WEEK_BEG_DT\" = \"T2\".\"WEEK_BEG_DT\"\n"
                + "WHERE \"T1\".\"WEEK_BEG_DT\" > '2010-01-01'";
        String complexSql = "select t1.week_beg_dt, t1.sum_price, t2.cnt\n" //
                + "from (\n"//
                + "    select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as sum_price\n"//
                + "    from db1.test_kylin_fact test_kylin_fact\n"//
                + "        inner join edw.test_cal_dt test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"//
                + "        inner join edw.test_category_groupings test_category_groupings\n"//
                + "        on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"//
                + "            and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"//
                + "        inner join edw.test_sites test_sites "
                + "        on test_kylin_fact.lstg_site_id = test_sites.site_id\n"//
                + "    where test_kylin_fact.price > 100\n" //
                + "    group by test_cal_dt.week_beg_dt\n" //
                + "    having sum(test_kylin_fact.price) > 1000\n" //
                + "    order by sum(test_kylin_fact.price)\n" //
                + ") t1\n" //
                + "    inner join (\n" //
                + "        select test_cal_dt.week_beg_dt, count(*) as cnt\n" //
                + "        from db1.test_kylin_fact test_kylin_fact\n" //
                + "            inner join edw.test_cal_dt test_cal_dt "
                + "            on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "            inner join edw.test_category_groupings test_category_groupings\n" //
                + "            on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n" //
                + "                and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" //
                + "            inner join edw.test_sites test_sites "
                + "            on test_kylin_fact.lstg_site_id = test_sites.site_id\n" //
                + "        group by test_cal_dt.week_beg_dt\n" //
                + "    ) t2\n" //
                + "    on t1.week_beg_dt = t2.week_beg_dt\n" //
                + "where t1.week_beg_dt > '2012-08-23'";
        checkPattern(expected, complexSql);
    }

    private void checkPattern(String expectedPattern, String originalInput) {
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(originalInput);
        Assert.assertEquals(expectedPattern, actual);
    }

    private static String retrieveSql(String fileName) throws IOException {
        File file = new File(SQL_DIR + File.separator + fileName);
        return FileUtils.readFileToString(file);
    }
}
