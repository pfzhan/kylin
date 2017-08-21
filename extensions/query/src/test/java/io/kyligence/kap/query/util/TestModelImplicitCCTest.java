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

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * test against real models 
 */
public class TestModelImplicitCCTest extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestModelImplicitCCTest.class);

    @Before
    public void setup() throws Exception {
        System.setProperty("needCheckCC", "true");
        super.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        System.setProperty("needCheckCC", "");
        super.cleanupTestMetadata();
    }

    @Test
    public void testConvertSingleTableCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        String transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY",
                "default", "DEFAULT");
        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY",
                transform);

        transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(price * item_count)",
                "default", "DEFAULT");
        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(F.DEAL_AMOUNT)",
                transform);

        transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY" + " union"
                        + " select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY",
                "default", "DEFAULT");
        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY union select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY",
                transform);

        transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by substr(ACCOUNT_COUNTRY,0,1)",
                "default", "DEFAULT");
        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id group by F.BUYER_COUNTRY_ABBR",
                transform);

    }

    @Test
    public void testConvertCrossTableCC() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();

        //buyer
        String transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                        + " inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)",
                "default", "DEFAULT");

        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country group by F.BUYER_ID_AND_COUNTRY_NAME",
                transform);

        //seller
        transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f"
                        + " inner join test_account a on f.seller_id = a.account_id  inner join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)",
                "default", "DEFAULT");

        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_account a on f.seller_id = a.account_id  inner join test_country c on a.account_country = c.country group by F.SELLER_ID_AND_COUNTRY_NAME",
                transform);

        //seller, but swap join condition
        transform = converter.transform(
                "select count(*), sum (price * item_count) from test_kylin_fact f"
                        + " inner join test_account a on f.seller_id = a.account_id  inner join test_country c on country = account_country group by concat(a.ACCOUNT_ID, c.NAME)",
                "default", "DEFAULT");

        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_account a on f.seller_id = a.account_id  inner join test_country c on country = account_country group by F.SELLER_ID_AND_COUNTRY_NAME",
                transform);
    }

    @Test
    public void testSubquery() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();
        String s = "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country"
                + " inner join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                + " inner join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " inner join " //
                + "( "//
                + "     select count(*), sum (price * item_count) ,country from test_kylin_fact f2"
                + "     inner join test_account a2 on f2.seller_id = a2.account_id  inner join test_country c2 on account_country = country group by concat(ACCOUNT_ID, NAME), country"
                + ") s on s.country = c.country  group by concat(a.ACCOUNT_ID, c.NAME)";
        String transform = converter.transform(s, "default", "DEFAULT");
        Assert.assertEquals(
                "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country inner join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt inner join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID inner join (      select count(*), sum (F2.DEAL_AMOUNT) ,country from test_kylin_fact f2     inner join test_account a2 on f2.seller_id = a2.account_id  inner join test_country c2 on account_country = country group by F2.SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by F.BUYER_ID_AND_COUNTRY_NAME",
                transform);
    }


    @Test
    public void testMixModel() {
        ConvertToComputedColumn converter = new ConvertToComputedColumn();
        String s = "select count(*), sum (price * item_count) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID"
            + " inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country"
            + " inner join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
            + " inner join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
            + " inner join " //
            + "( "//
            + "     select count(*), sum (price * item_count) ,country from test_kylin_fact f2"
            + "     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by concat(ACCOUNT_ID, NAME), country"
            + ") s on s.country = c.country  group by concat(a.ACCOUNT_ID, c.NAME)";
        String transform = converter.transform(s, "default", "DEFAULT");
        Assert.assertEquals(
            "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f inner join test_order o on f.ORDER_ID = o.ORDER_ID inner join test_account a on o.buyer_id = a.account_id  inner join test_country c on a.account_country = c.country inner join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt inner join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID inner join (      select count(*), sum (F2.DEAL_AMOUNT) ,country from test_kylin_fact f2     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by F2.SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by F.BUYER_ID_AND_COUNTRY_NAME",
            transform);
    }
}
