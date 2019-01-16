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
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

/**
 * test against real models
 */
public class ImplicitCCOnRealModelTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("../core-metadata/src/test/resources/ut_meta/ccjointest");
    }

    @After
    public void after() throws Exception {
        super.cleanupTestMetadata();
    }

    @Test
    public void testConvertSingleTableCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(price * item_count)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(F._CC_DEAL_AMOUNT)";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY" + " union"
                    + " select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY union select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by substr(ACCOUNT_COUNTRY,0,1)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by F._CC_LEFTJOIN_BUYER_COUNTRY_ABBR";
            check(converter, originSql, ccSql);

        }

    }

    @Test
    public void testConvertCrossTableCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        {
            //buyer
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by F._CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by F._CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller, but swap join condition
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by F._CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }
    }

    @Test
    public void testSubquery() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                    + " left join " //
                    + "( "//
                    + "     select count(*), sum (price * item_count) ,country from test_kylin_fact f2"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by concat(ACCOUNT_ID, NAME), country"
                    + ") s on s.country = c.country  group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID left join (      select count(*), sum (F2._CC_DEAL_AMOUNT) ,country from test_kylin_fact f2     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by F2._CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by F._CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*) from (select count(*), sum (price * item_count) from test_kylin_fact)";
            String ccSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT._CC_DEAL_AMOUNT) from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from (select count(*), sum (price * item_count) from test_kylin_fact) f";
            String ccSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT._CC_DEAL_AMOUNT) from test_kylin_fact) f";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (price * item_count) from (select * from test_kylin_fact)";
            String ccSql = "select sum (TEST_KYLIN_FACT._CC_DEAL_AMOUNT) from (select * from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (price * item_count) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";
            String ccSql = "select sum (TEST_KYLIN_FACT._CC_DEAL_AMOUNT) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";

            check(converter, originSql, ccSql);
        }

    }

    @Test
    public void testMixModel() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                + " left join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " left join test_account a on o.buyer_id = a.account_id "
                + " left join test_country c on a.account_country = c.country"
                + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " inner join ( "//
                + "     select count(*), sum (price * item_count), country from test_kylin_fact f2"
                + "     left join test_account a2 on f2.seller_id = a2.account_id"
                + "     left join test_country c2 on account_country = country"
                + "     group by concat(ACCOUNT_ID, NAME), country"
                + " ) s on s.country = c.country"
                + " group by a.ACCOUNT_ID";
        String ccSql = "select count(*), sum (F._CC_DEAL_AMOUNT) from test_kylin_fact f"
                + " left join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " left join test_account a on o.buyer_id = a.account_id "
                + " left join test_country c on a.account_country = c.country"
                + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"
                + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " inner join ( "
                + "     select count(*), sum (F2._CC_DEAL_AMOUNT), country from test_kylin_fact f2"
                + "     left join test_account a2 on f2.seller_id = a2.account_id"
                + "     left join test_country c2 on account_country = country"
                + "     group by F2._CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country"
                + " ) s on s.country = c.country"
                + " group by a.ACCOUNT_ID";
        check(converter, originSql, ccSql);

    }

    @Test
    @Ignore("Not support CC on Join condition")
    public void testJoinOnCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();
        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT._CC_ORDER_ID_PLUS_1 = TEST_ORDER._CC_ID_PLUS_1";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT._CC_ORDER_ID_PLUS_1 = TEST_ORDER._CC_ID_PLUS_1\n"
                    + "left join TEST_ACCOUNT on (TEST_ACCOUNT._CC_BUYER_ACCOUNT_CASE_WHEN) = (TEST_ORDER._CC_ACCOUNT_CASE_WHEN)\n";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n"
                    + "left join TEST_COUNTRY on UPPER(TEST_ACCOUNT.ACCOUNT_COUNTRY) = TEST_COUNTRY.COUNTRY";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT._CC_ORDER_ID_PLUS_1 = TEST_ORDER._CC_ID_PLUS_1\n"
                    + "left join TEST_ACCOUNT on (TEST_ACCOUNT._CC_BUYER_ACCOUNT_CASE_WHEN) = (TEST_ORDER._CC_ACCOUNT_CASE_WHEN)\n"
                    + "left join TEST_COUNTRY on TEST_ACCOUNT._CC_COUNTRY_UPPER = TEST_COUNTRY.COUNTRY";
            check(converter, originSql, ccSql);
        }
    }

    @Test
    public void testNoFrom() throws Exception {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        String originSql = "select sum(price * item_count),(SELECT 1 as VERSION) from test_kylin_fact";
        String ccSql = "select sum(TEST_KYLIN_FACT._CC_DEAL_AMOUNT),(SELECT 1 as VERSION) from test_kylin_fact";

        check(converter, originSql, ccSql);
    }

    @Test
    public void testFromValues() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        String originSql = "select sum(price * item_count),(SELECT 1 FROM (VALUES(1))) from test_kylin_fact";
        String ccSql = "select sum(TEST_KYLIN_FACT._CC_DEAL_AMOUNT),(SELECT 1 FROM (VALUES(1))) from test_kylin_fact";

        check(converter, originSql, ccSql);
    }
    
    @Test
    public void testNestedCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();
        
        String ccSql = "select count(*), sum (F._CC_NEST4) from test_kylin_fact F";

        {
            String originSql = "select count(*), sum ((round((F.PRICE + 11) * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum ((round(F.NEST1 * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum ((round(F.NEST2, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum (F.NEST3 * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }
    }

    private void check(ConvertToComputedColumn converter, String originSql, String ccSql) {
        String transform = converter.transform(originSql, "default", "DEFAULT");
        Assert.assertEquals(ccSql, transform);
    }

}
