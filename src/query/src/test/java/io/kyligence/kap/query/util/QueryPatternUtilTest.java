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

import org.junit.Assert;
import org.junit.Test;

public class QueryPatternUtilTest {

    @Test
    public void testJdbcFn() {
        String sql = "SELECT {fn SECOND({fn CONVERT({fn CONVERT('2010-10-10 10:10:10.4', SQL_TIMESTAMP) }, SQL_TIMESTAMP) }) } "
                + "TEMP_TEST__2143701310__0_, 1 X__ALIAS__0\n"
                + "FROM TDVT.CALCS CALCS\n"
                + "GROUP BY 1";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(sql, actual);
    }

    @Test
    public void testGroupBy() {
        String sql = "SELECT SUM(1) AS COL, \n"
                + " 2 AS COL2 \n"
                + " FROM ( \n"
                + " select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV \n"
                + " , count(*) as TRANS_CNT \n"
                + " from test_kylin_fact \n"
                + " inner JOIN edw.test_cal_dt as test_cal_dt\n"
                + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt \n"
                + " where test_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' \n"
                + " group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt \n"
                + " having sum(price)>500 \n"
                + " ) TableauSQL \n"
                + " GROUP BY 2 \n"
                + " HAVING COUNT(1)>0 ";
        String expected = "SELECT SUM(1) COL, 2 COL2\n"
                + "FROM (SELECT TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_CAL_DT.WEEK_BEG_DT, SUM(TEST_KYLIN_FACT.PRICE) GMV, COUNT(*) TRANS_CNT\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "WHERE TEST_CAL_DT.WEEK_BEG_DT BETWEEN ASYMMETRIC DATE '2010-01-01' AND DATE '2010-01-01'\n"
                + "GROUP BY TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_CAL_DT.WEEK_BEG_DT\n"
                + "HAVING SUM(PRICE) > 1) TABLEAUSQL\n"
                + "GROUP BY 2\n"
                + "HAVING COUNT(1) > 1";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSelectNumericColumn() {
        String sql1 = "SELECT 1, 2, 3\nFROM A";
        String actual1 = QueryPatternUtil.normalizeSQLPatternImpl(sql1);
        Assert.assertEquals(sql1, actual1);

        String sql2 = "SELECT 97 AS TEMP_Test__415603459__0_,\n"
                + "1 AS X__alias__0\n"
                + "FROM TDVT.CALCS CALCS\n"
                + "GROUP BY 1";
        String expected2 = "SELECT 97 TEMP_TEST__415603459__0_, 1 X__ALIAS__0\n"
                + "FROM TDVT.CALCS CALCS\n"
                + "GROUP BY 1";
        String actual2 = QueryPatternUtil.normalizeSQLPatternImpl(sql2);
        Assert.assertEquals(expected2, actual2);
    }

    @Test
    public void testLiteralComparison() {
        String sql1 = "select ks.price as price, ks.part_dt as dt from kylin_sales as ks "
                + "where NOT(ks.price <= 60.0) and ks.part_dt > '2012-02-05' and ks.name <> 'Baby'";
        String actual1 = QueryPatternUtil.normalizeSQLPatternImpl(sql1);

        String expected1 = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE NOT KS.PRICE <= 2 AND KS.PART_DT > '2010-01-01' AND KS.NAME <> 'A'";
        Assert.assertEquals(expected1, actual1);

        String sql2 = "select ks.price > 50, ks.part_dt <= '2008-04-23' "
                + "from kylin_sales as ks";
        String actual2 = QueryPatternUtil.normalizeSQLPatternImpl(sql2);
        String expected2 = "SELECT KS.PRICE > 1, KS.PART_DT <= '2010-01-02'\nFROM KYLIN_SALES KS";
        Assert.assertEquals(expected2, actual2);
    }

    @Test
    public void testColumnComparison() {
        String sql = "select b.firstname as firstname1, b.lastname as lastname1,\n"
                + "a.firstname as firstname2, a.lastname as lastname2, b.city, b.country\n"
                + "from sales.customer a, sales.customer b\n"
                + "where (a.id <> b.id and a.city = b.city and a.country = b.country)\n";

        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT B.FIRSTNAME FIRSTNAME1, B.LASTNAME LASTNAME1, "
                + "A.FIRSTNAME FIRSTNAME2, A.LASTNAME LASTNAME2, B.CITY, B.COUNTRY\n"
                + "FROM SALES.CUSTOMER A,\n"
                + "SALES.CUSTOMER B\n"
                + "WHERE A.ID <> B.ID AND A.CITY = B.CITY AND A.COUNTRY = B.COUNTRY";
        // SqlIdentifiers should remain unchanged
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testStringComparison() {
        String sql = "select ks.price as price from kylin_sales as ks \n"
                + "where ks.dt <= '200310' and ks.dt > '2012-02'";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT KS.PRICE PRICE\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.DT <= 'Z' AND KS.DT > 'A'";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testQueryWithSamePattern() {
        String sql1 = "select s.name, s.price "
                + "from kylin_sales as s inner join kylin_account as a "
                + "on s.name = a.name and s.price > 100"
                + "where s.account is not null";
        String sql2 = "select s.name, s.price "
                + "from kylin_sales as s inner join kylin_account a "
                + "on s.name = a.name and s.price > 8000"
                + "where s.account is not null";
        String pattern1 = QueryPatternUtil.normalizeSQLPatternImpl(sql1);
        String pattern2 = QueryPatternUtil.normalizeSQLPatternImpl(sql2);
        Assert.assertEquals(pattern1, pattern2);
    }

    @Test
    public void testInAndNotIn() {
        String sql0 = "select first_name, last_name from sales "
                + "where phone_number not in (10, 20, 30) "
                + "and last_name in ('Swift', 'Bloomberg')";
        String actual0 = QueryPatternUtil.normalizeSQLPatternImpl(sql0);
        String expected0 = "SELECT FIRST_NAME, LAST_NAME\nFROM SALES\n"
                + "WHERE PHONE_NUMBER NOT IN (1) "
                + "AND LAST_NAME IN ('A')";
        Assert.assertEquals(expected0, actual0);
        String sql1 = "select first_name, last_name from sales "
                + "where phone_number not in (103952, 202342, 422583) "
                + "and last_name in ('Sam', 'Jobs')";
        String actual1 = QueryPatternUtil.normalizeSQLPatternImpl(sql1);
        Assert.assertEquals(actual0, actual1);

        String sql2 = "select sum(PRICE) as GMV, LSTG_FORMAT_NAME as FORMAT_NAME\n"
                + "from test_kylin_fact\n"
                + "where (LSTG_FORMAT_NAME in ('ABIN')) or (LSTG_FORMAT_NAME >= 'FP-GTC' "
                + "and LSTG_FORMAT_NAME <= 'Others')\n"
                + "group by LSTG_FORMAT_NAME";
        String expected2 = "SELECT SUM(PRICE) GMV, LSTG_FORMAT_NAME FORMAT_NAME\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "WHERE LSTG_FORMAT_NAME IN ('A') OR LSTG_FORMAT_NAME >= 'A' "
                + "AND LSTG_FORMAT_NAME <= 'Z'\n"
                + "GROUP BY LSTG_FORMAT_NAME";
        String actual2 = QueryPatternUtil.normalizeSQLPatternImpl(sql2);
        Assert.assertEquals(expected2, actual2);

        String sql3 = "SELECT ProductName\n"
                + "FROM Product \n"
                + "WHERE Id IN (SELECT ProductId \n"
                + "FROM OrderItem\n"
                + "WHERE Quantity IN (100, 200))";
        String expected3 = "SELECT PRODUCTNAME\n"
                + "FROM PRODUCT\n"
                + "WHERE ID IN (SELECT PRODUCTID\n"
                + "FROM ORDERITEM\n"
                + "WHERE QUANTITY IN (1))";
        String actual3 = QueryPatternUtil.normalizeSQLPatternImpl(sql3);
        Assert.assertEquals(expected3, actual3);

        String sql4 = "SELECT ProductName\n"
                + "FROM Product \n"
                + "WHERE Id IN ('a', 'b', (SELECT ProductId \n"
                + "FROM OrderItem\n"
                + "WHERE Quantity IN (100, 200)))";
        String expected4 = "SELECT PRODUCTNAME\n"
                + "FROM PRODUCT\n"
                + "WHERE ID IN ('A', (SELECT PRODUCTID\n"
                + "FROM ORDERITEM\n"
                + "WHERE QUANTITY IN (1)))";
        String actual4 = QueryPatternUtil.normalizeSQLPatternImpl(sql4);
        Assert.assertEquals(expected4, actual4);
    }

    @Test
    public void testLikeAndNotLike() {
        String sql = "select META_CATEG_NAME, META_CATEG_ID, count(*) as cnt \n"
                + "from test_kylin_fact \n"
                + "inner JOIN edw.test_cal_dt as test_cal_dt\n"
                + "ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "where META_CATEG_NAME not like '%ab%'\n "
                + "and META_CATE_ID like '%08'"
                + "group by META_CATEG_NAME";
        String expected = "SELECT META_CATEG_NAME, META_CATEG_ID, COUNT(*) CNT\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT "
                + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "WHERE META_CATEG_NAME NOT LIKE 'A' AND META_CATE_ID LIKE 'A'\n"
                + "GROUP BY META_CATEG_NAME";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBetweenAnd() {
        String sql = "select sum(l_extendedprice) - sum(l_saleprice) as revenue\n"
                + "from v_lineitem\n"
                + "where l_shipdate >= date '1993-01-01'\n"
                + "and l_shipdate < '1994-01-01'\n"
                + "and l_discount between 0.05 and 0.07\n"
                + "and l_quantity < 25";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT SUM(L_EXTENDEDPRICE) - SUM(L_SALEPRICE) REVENUE\n"
                + "FROM V_LINEITEM\n"
                + "WHERE L_SHIPDATE >= DATE '2010-01-01' AND L_SHIPDATE < '2010-01-02' "
                + "AND L_DISCOUNT BETWEEN ASYMMETRIC 1 AND 1 AND L_QUANTITY < 2";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDistinct() {
        String sql = "SELECT distinct LSTG_FORMAT_NAME from test_kylin_fact";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT DISTINCT LSTG_FORMAT_NAME\n"
                + "FROM TEST_KYLIN_FACT";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testH2Incapable() {
        String sql = "SELECT timestampadd(DAY,1,WEEK_BEG_DT) as x ,WEEK_BEG_DT\n"
                + " FROM TEST_KYLIN_FACT \n"
                + " inner JOIN EDW.TEST_CAL_DT AS TEST_CAL_DT ON (TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT) \n"
                + " GROUP BY TEST_CAL_DT.WEEK_BEG_DT";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT TIMESTAMPADD(DAY, 1, WEEK_BEG_DT) X, WEEK_BEG_DT\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testUnion() {
        String unionSql = "select ks.price as price, ks.part_dt as dt "
                + "from kylin_sales ks "
                + "where ks.price > 50 and ks.part_dt > date '2012-08-23' "
                + "union "
                + "select ky.price as price, ky.part_dt as dt "
                + "from kylin_sales ky "
                + "where ky.price < 40 and ky.part_dt < date '2015-08-22'";
        String expected = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.PRICE > 1 AND KS.PART_DT > DATE '2010-01-01'\n"
                + "UNION\n"
                + "SELECT KY.PRICE PRICE, KY.PART_DT DT\n"
                + "FROM KYLIN_SALES KY\n"
                + "WHERE KY.PRICE < 2 AND KY.PART_DT < DATE '2010-01-02'";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(unionSql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReverseComparison() {
        String sql = "select ky.price as price, ky.part_dt as dt "
                + "from kylin_sales ky "
                + "where 40 >= ky.price and date '2015-08-22' < ky.part_dt "
                + "and '2018-01-09' > ky.part_dt";

        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT KY.PRICE PRICE, KY.PART_DT DT\n"
                + "FROM KYLIN_SALES KY\n"
                + "WHERE 2 >= KY.PRICE AND DATE '2010-01-01' < KY.PART_DT "
                + "AND '2010-01-02' > KY.PART_DT";
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testFunction() {
        String sql = "select upper(lstg_format_name) as lstg_format_name, "
                + "count(*) as cnt from test_kylin_fact\n"
                + "where lower(lstg_format_name)='abin' "
                + "and substring(lstg_format_name,1,3) in ('ABI') "
                + "and upper(lstg_format_name) > 'AAAA' "
                + "and lower(lstg_format_name) like '%b%' "
                + "and char_length(lstg_format_name) < 10 "
                + "and char_length(lstg_format_name) > 3 "
                + "and 'abc'||lstg_format_name||'a'||'b'||'c'='abcABINabc'\n"
                + "group by lstg_format_name";
        String expected = "SELECT UPPER(LSTG_FORMAT_NAME) LSTG_FORMAT_NAME, "
                + "COUNT(*) CNT\n"
                + "FROM TEST_KYLIN_FACT\n"
                + "WHERE LOWER(LSTG_FORMAT_NAME) = 'A' "
                + "AND SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 1) "
                + "IN ('A') AND UPPER(LSTG_FORMAT_NAME) > 'A' "
                + "AND LOWER(LSTG_FORMAT_NAME) LIKE 'A' "
                + "AND CHAR_LENGTH(LSTG_FORMAT_NAME) < 2 "
                + "AND CHAR_LENGTH(LSTG_FORMAT_NAME) > 1 "
                + "AND 'A' || LSTG_FORMAT_NAME || 'A' || 'A' || 'A' = 'A'\n"
                + "GROUP BY LSTG_FORMAT_NAME";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testWindowFunction() {
        String sql = "select * from(\n"
                + "select cal_dt, lstg_format_name, sum(price) as GMV,\n"
                + "100 * sum(price) / first_value(sum(price)) over (partition by lstg_format_name "
                + "order by cast(cal_dt as timestamp) range interval '2' day PRECEDING) as \"last_day\",\n"
                + "first_value(sum(price)) over (partition by lstg_format_name "
                + "order by cast(cal_dt as timestamp) range cast(366 as INTERVAL day) preceding)\n"
                + "from test_kylin_fact as \"last_year\"\n"
                + "where cal_dt between '2013-01-08' and '2013-01-15' or cal_dt "
                + "between '2013-01-07' and '2013-01-15' or cal_dt between '2012-01-01' and '2012-01-15'\n"
                + "group by cal_dt, lstg_format_name)t\n"
                + "where cal_dt between '2013-01-06' and '2013-01-15'";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT *\n"
                + "FROM (SELECT CAL_DT, LSTG_FORMAT_NAME, SUM(PRICE) GMV, "
                + "1 * SUM(PRICE) / (FIRST_VALUE(SUM(PRICE)) OVER (PARTITION BY LSTG_FORMAT_NAME "
                + "ORDER BY CAST(CAL_DT AS TIMESTAMP) RANGE INTERVAL '1' DAY PRECEDING)) last_day, "
                + "FIRST_VALUE(SUM(PRICE)) OVER (PARTITION BY LSTG_FORMAT_NAME "
                + "ORDER BY CAST(CAL_DT AS TIMESTAMP) RANGE CAST(1 AS INTERVAL DAY) PRECEDING)\n"
                + "FROM TEST_KYLIN_FACT last_year\n"
                + "WHERE CAL_DT BETWEEN ASYMMETRIC '2010-01-01' AND '2010-01-01' OR CAL_DT "
                + "BETWEEN ASYMMETRIC '2010-01-01' AND '2010-01-01' "
                + "OR CAL_DT BETWEEN ASYMMETRIC '2010-01-01' AND '2010-01-01'\n"
                + "GROUP BY CAL_DT, LSTG_FORMAT_NAME) T\n"
                + "WHERE CAL_DT BETWEEN ASYMMETRIC '2010-01-01' AND '2010-01-01'";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testWithSelect() {
        String withSelect = "with a as ("
                + "select ks.price as price, ks.part_dt as dt "
                + "from kylin_sales ks "
                + "where ks.price > 50 and ks.part_dt > '2012-07-08') "
                + "select a.price, a.dt from a where a.dt < '2015-08-09'";
        String expected = "WITH A AS (SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.PRICE > 1 AND KS.PART_DT > '2010-01-01') (SELECT A.PRICE, A.DT\n"
                + "FROM A\n"
                + "WHERE A.DT < '2010-01-02')";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(withSelect);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSubQuery() {
        String sql = "select a.price, a.dt from ( "
                + "select ks.price as price, ks.part_dt as dt "
                + "from kylin_sales ks "//
                + "where ks.price > 50 and ks.part_dt > '2012-07-08' "
                + ") a "
                + "where a.dt < '2015-08-09'";
        String expected = "SELECT A.PRICE, A.DT\n"
                + "FROM (SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.PRICE > 1 AND KS.PART_DT > '2010-01-01') A\n"
                + "WHERE A.DT < '2010-01-02'";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDate() {
        String sql = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.PART_DT <= '2020-08-08' "
                + "AND KS.PART_DT > date '2002-02-08'";
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(sql);
        String expected = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n"
                + "FROM KYLIN_SALES KS\n"
                + "WHERE KS.PART_DT <= '2010-01-02' "
                + "AND KS.PART_DT > DATE '2010-01-01'";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testComplicatedCase() {
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
        String actual = QueryPatternUtil.normalizeSQLPatternImpl(complexSql);
        String expected = "SELECT T1.WEEK_BEG_DT, T1.SUM_PRICE, T2.CNT\n"
                + "FROM (SELECT TEST_CAL_DT.WEEK_BEG_DT, SUM(TEST_KYLIN_FACT.PRICE) SUM_PRICE\n"
                + "FROM DB1.TEST_KYLIN_FACT TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN EDW.TEST_CATEGORY_GROUPINGS TEST_CATEGORY_GROUPINGS "
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID "
                + "AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "WHERE TEST_KYLIN_FACT.PRICE > 1\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT\n"
                + "HAVING SUM(TEST_KYLIN_FACT.PRICE) > 1\n"
                + "ORDER BY SUM(TEST_KYLIN_FACT.PRICE)) T1\n"
                + "INNER JOIN (SELECT TEST_CAL_DT.WEEK_BEG_DT, COUNT(*) CNT\n"
                + "FROM DB1.TEST_KYLIN_FACT TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN EDW.TEST_CATEGORY_GROUPINGS TEST_CATEGORY_GROUPINGS "
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID "
                + "AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT) T2 ON T1.WEEK_BEG_DT = T2.WEEK_BEG_DT\n"
                + "WHERE T1.WEEK_BEG_DT > '2010-01-01'";
        Assert.assertEquals(expected, actual);
    }

}
