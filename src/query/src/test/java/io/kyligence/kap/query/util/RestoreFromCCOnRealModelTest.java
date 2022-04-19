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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class RestoreFromCCOnRealModelTest extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(RestoreFromCCOnRealModelTest.class);

    @Before
    public void setup() throws Exception {
        super.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        super.cleanupTestMetadata();
    }

    @Test
    public void testRestoreComputedColumnToExprSingleTable() {
        RestoreFromComputedColumn restoreFromComputedColumn = new RestoreFromComputedColumn();

        //simple query
        {
            String ret = restoreFromComputedColumn.convert("select DEAL_AMOUNT,DEAL_YEAR from test_kylin_fact",
                    "default", "DEFAULT");
            Assert.assertEquals(
                    "select \"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\" AS DEAL_AMOUNT,year(\"TEST_KYLIN_FACT\".\"CAL_DT\") AS DEAL_YEAR from test_kylin_fact",

                    ret);
        }
        //group by 
        {
            String ret = restoreFromComputedColumn.convert(
                    "select DEAL_AMOUNT,DEAL_YEAR from test_kylin_fact group by DEAL_YEAR", "default", "DEFAULT");
            Assert.assertEquals(
                    "select \"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\" AS DEAL_AMOUNT,year(\"TEST_KYLIN_FACT\".\"CAL_DT\") AS DEAL_YEAR from test_kylin_fact group by year(\"TEST_KYLIN_FACT\".\"CAL_DT\")",

                    ret);
        }
        //order by non-alias
        {
            String ret = restoreFromComputedColumn.convert(
                    "select sum(DEAL_AMOUNT) ,cal_dt as x from test_kylin_fact group by cal_dt order by sum(DEAL_AMOUNT)",
                    "default", "DEFAULT");
            Assert.assertEquals(
                    "select sum(\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\") ,cal_dt as x from test_kylin_fact group by cal_dt order by sum(\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\")",
                    ret);
        }
        //order by
        {
            String ret = restoreFromComputedColumn.convert(
                    "select sum(DEAL_AMOUNT) as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact group by cal_dt order by DEAL_AMOUNTx",
                    "default", "DEFAULT");
            Assert.assertEquals(
                    "select sum(\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\") as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact group by cal_dt order by DEAL_AMOUNTx",
                    ret);
        }
        //col with alias
        {
            String ret = restoreFromComputedColumn.convert(
                    "select sum(\"F\".\"DEAL_AMOUNT\") as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact as \"F\" group by cal_dt order by DEAL_AMOUNTx",
                    "default", "DEFAULT");
            Assert.assertEquals(
                    "select sum(\"F\".\"PRICE\" * \"F\".\"ITEM_COUNT\") as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact as \"F\" group by cal_dt order by DEAL_AMOUNTx",
                    ret);
        }
        //col with alias, one less AS
        {
            String ret = restoreFromComputedColumn.convert(
                    "select sum(\"F\".\"DEAL_AMOUNT\") as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact \"F\" group by cal_dt order by DEAL_AMOUNTx",
                    "default", "DEFAULT");
            Assert.assertEquals(
                    "select sum(\"F\".\"PRICE\" * \"F\".\"ITEM_COUNT\") as DEAL_AMOUNTx,cal_dt as x from test_kylin_fact \"F\" group by cal_dt order by DEAL_AMOUNTx",
                    ret);
        }

        // nested cc
        {
            String ret = restoreFromComputedColumn.convert("select NEST3 from TEST_KYLIN_FACT", "default", "DEFAULT");

            Assert.assertEquals(
                    "select round((\"TEST_KYLIN_FACT\".\"PRICE\" + 11) * 12, 0) AS NEST3 from TEST_KYLIN_FACT", ret);

        }

        // nested cc
        {
            String ret = restoreFromComputedColumn.convert("select NEST4 from TEST_KYLIN_FACT", "default", "DEFAULT");

            Assert.assertEquals(
                    "select (round((\"TEST_KYLIN_FACT\".\"PRICE\" + 11) * 12, 0)) * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\" AS NEST4 from TEST_KYLIN_FACT",
                    ret);

        }
    }

    @Test(expected = ComparisonFailure.class)
    public void testUnsupportedOrderBy() {
        RestoreFromComputedColumn restoreFromComputedColumn = new RestoreFromComputedColumn();

        //order by alias, which happens to be a cc (this is in theory valid syntax, however not supported)
        String ret = restoreFromComputedColumn.convert(
                "select sum(DEAL_AMOUNT) as DEAL_AMOUNT,cal_dt as x from test_kylin_fact group by cal_dt order by DEAL_AMOUNT",
                "default", "DEFAULT");
        Assert.assertEquals(
                "select sum(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) as DEAL_AMOUNT,cal_dt as x from test_kylin_fact group by cal_dt order by DEAL_AMOUNT",
                ret);

    }

    @Test
    public void testConvertSingleTableCC() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();

        {
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(F.PRICE * F.ITEM_COUNT)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(F.DEAL_AMOUNT)";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY" + " union"
                    + " select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY union select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by SUBSTR(A.ACCOUNT_COUNTRY,0,1)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by F.LEFTJOIN_BUYER_COUNTRY_ABBR";
            check(converter, originSql, ccSql);

        }

    }

    @Test
    public void testConvertCrossTableCC() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();

        {
            //buyer
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by CONCAT(A.ACCOUNT_ID, C.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by F.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by CONCAT(A.ACCOUNT_ID, C.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by F.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller, but swap join condition
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by CONCAT(A.ACCOUNT_ID, C.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by F.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }
    }

    @Test
    public void testSubquery() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();
        {
            String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                    + " left join " //
                    + "( "//
                    + "     select count(*), sum (F2.PRICE * F2.ITEM_COUNT) ,country from test_kylin_fact f2"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by CONCAT(A2.ACCOUNT_ID, C2.NAME), country"
                    + ") s on s.country = c.country  group by CONCAT(A.ACCOUNT_ID, C.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID left join (      select count(*), sum (F2.DEAL_AMOUNT) ,country from test_kylin_fact f2     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by F2.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by F.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from test_kylin_fact)";
            String ccSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from test_kylin_fact) f";
            String ccSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from test_kylin_fact) f";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from (select * from test_kylin_fact)";
            String ccSql = "select sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from (select * from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";
            String ccSql = "select sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum(DEAL_AMOUNT) as \"c1\" from (select F.PRICE * F.ITEM_COUNT AS DEAL_AMOUNT from test_kylin_fact f left join test_order o "
                    + "on f.order_id = o.account_id where cal_dt = '2012-01-02') as tbl";
            String ccSql = "select sum(DEAL_AMOUNT) as \"c1\" from (select f.DEAL_AMOUNT from test_kylin_fact f left join test_order o "
                    + "on f.order_id = o.account_id where cal_dt = '2012-01-02') as tbl";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select * from (select * from (select count(*) as num, sum (TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) as summary "
                    + "from test_kylin_fact) f where num > 0) where num > 0";
            String ccSql = "select * from (select * from (select count(*) as num, sum (TEST_KYLIN_FACT.DEAL_AMOUNT) as summary "
                    + "from test_kylin_fact) f where num > 0) where num > 0";
            check(converter, originSql, ccSql);
        }
    }

    @Test
    public void testMixModel() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();

        String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country"
                + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " left join " //
                + "( "//
                + "     select count(*), sum (F2.PRICE * F2.ITEM_COUNT) ,country from test_kylin_fact f2"
                + "     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by CONCAT(A2.ACCOUNT_ID, C2.NAME), country"
                + ") s on s.country = c.country  group by A.ACCOUNT_ID";
        String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID left join (      select count(*), sum (F2.DEAL_AMOUNT) ,country from test_kylin_fact f2     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by F2.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by A.ACCOUNT_ID";
        check(converter, originSql, ccSql);
    }

    @Test
    public void testNestedCC() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();
        String originSql = "select count(*), sum ((round((F.PRICE + 11) * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";

        {
            String ccSql = "select count(*), sum ((round(F.NEST1 * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String ccSql = "select count(*), sum ((round(F.NEST2, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String ccSql = "select count(*), sum (F.NEST3 * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String ccSql = "select count(*), sum (F.NEST4) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }
    }

    @Test
    public void testParenthesis() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();
        String originSql = "select F.PRICE * F.ITEM_COUNT AS DEAL_AMOUNT from test_kylin_fact F group by (F.PRICE * F.ITEM_COUNT) limit 100";
        String ccSql = "select deal_amount from test_kylin_fact F group by deal_amount limit 100";
        check(converter, originSql, ccSql);
    }

    @Test
    public void testNonEqualJoinCondition() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();
        String originSql = "select count(*), sum (F.PRICE * F.ITEM_COUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on ((o.buyer_id = a.account_id) or ((o.buyer_id is NULL) and (a.account_id is NULL))) group by ACCOUNT_COUNTRY";
        String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on ((o.buyer_id = a.account_id) or ((o.buyer_id is NULL) and (a.account_id is NULL))) group by ACCOUNT_COUNTRY";
        check(converter, originSql, ccSql);
    }

    @Test
    public void testConvertCaseWhen() {
        RestoreFromComputedColumn converter = new RestoreFromComputedColumn();
        String originSql = "select case CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) when '10000000Russia' then 'RUSSIA' when '10000001United States' then 'USA' else 'Other COUNTRY' end "
                + "from TEST_KYLIN_FACT " + "left join TEST_ACCOUNT SELLER_ACCOUNT "
                + "on TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID " + "left join TEST_COUNTRY SELLER_COUNTRY "
                + "on SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY "
                + "order by CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)";
        String ccSql = "select case LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME when '10000000Russia' then 'RUSSIA' when '10000001United States' then 'USA' else 'Other COUNTRY' end "
                + "from TEST_KYLIN_FACT " + "left join TEST_ACCOUNT SELLER_ACCOUNT "
                + "on TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID " + "left join TEST_COUNTRY SELLER_COUNTRY "
                + "on SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY "
                + "order by LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
        check(converter, originSql, ccSql);
    }

    @Test
    public void testConvertToComputedColumnForUnion() {
        String originSql = "select sum(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from test_kylin_fact "
                + "union all select sum(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) from test_kylin_fact limit 1";
        String ccSql = "select sum(DEAL_AMOUNT) from test_kylin_fact "
                + "union all select sum(DEAL_AMOUNT) from test_kylin_fact limit 1";
        check(new RestoreFromComputedColumn(), originSql, ccSql);
    }

    private void check(RestoreFromComputedColumn converter, String originSql, String ccSql) {
        String transform = converter.convert(ccSql, "default", "DEFAULT");
        Assert.assertEquals(originSql.replaceAll("\"", ""), transform.replaceAll("\"", ""));
    }

}
