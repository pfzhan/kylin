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

package io.kyligence.kap.query.security;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class HackSelectStarWithColumnACLTest extends LocalFileMetadataTestCase {
    private final static String PROJECT = "default";
    private final static String SCHEMA = "DEFAULT";

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @Test
    public void testTransform() throws IOException {
        enableQueryPushDown();
        QueryContext.current().setUsername("u1");
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        ColumnACLManager.getInstance(KylinConfig.getInstanceFromEnv()).addColumnACL(PROJECT, "u1",
                "DEFAULT.TEST_KYLIN_FACT", Sets.newHashSet("PRICE", "ITEM_COUNT"));
        String sql = transformer.transform(
                "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID", PROJECT, SCHEMA);
        String expectSQL = "select T1.TRANS_ID, T1.ORDER_ID, T1.CAL_DT, T1.LSTG_FORMAT_NAME, T1.LEAF_CATEG_ID, T1.LSTG_SITE_ID, T1.SLR_SEGMENT_CD, T1.SELLER_ID, T1.TEST_COUNT_DISTINCT_BITMAP, T1.DEAL_AMOUNT, T1.DEAL_YEAR, T1.BUYER_ID_AND_COUNTRY_NAME, T1.SELLER_ID_AND_COUNTRY_NAME, T1.BUYER_COUNTRY_ABBR, T1.SELLER_COUNTRY_ABBR, T2.ORDER_ID, T2.BUYER_ID, T2.TEST_DATE_ENC, T2.TEST_TIME_ENC, T2.TEST_EXTENDED_COLUMN from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        Assert.assertEquals(expectSQL, sql);
    }

    @Test
    public void testGetNewSelectClause() {
        enableQueryPushDown();
        String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID ";
        TreeSet<String> mockBlackList = new TreeSet<>();
        String newSelectClause = HackSelectStarWithColumnACL.getNewSelectClause(sql, PROJECT, SCHEMA, mockBlackList);
        String expect = "T1.TRANS_ID, T1.ORDER_ID, T1.CAL_DT, T1.LSTG_FORMAT_NAME, T1.LEAF_CATEG_ID, T1.LSTG_SITE_ID, T1.SLR_SEGMENT_CD, T1.SELLER_ID, T1.PRICE, T1.ITEM_COUNT, T1.TEST_COUNT_DISTINCT_BITMAP, T1.DEAL_AMOUNT, T1.DEAL_YEAR, T1.BUYER_ID_AND_COUNTRY_NAME, T1.SELLER_ID_AND_COUNTRY_NAME, T1.BUYER_COUNTRY_ABBR, T1.SELLER_COUNTRY_ABBR, T2.ORDER_ID, T2.BUYER_ID, T2.TEST_DATE_ENC, T2.TEST_TIME_ENC, T2.TEST_EXTENDED_COLUMN";
        Assert.assertEquals(expect, newSelectClause);

        mockBlackList.add("DEFAULT.TEST_KYLIN_FACT.PRICE");
        mockBlackList.add("DEFAULT.TEST_ORDER.BUYER_ID");

        String newSelectClause1 = HackSelectStarWithColumnACL.getNewSelectClause(sql, PROJECT, SCHEMA, mockBlackList);
        String expect1 ="T1.TRANS_ID, T1.ORDER_ID, T1.CAL_DT, T1.LSTG_FORMAT_NAME, T1.LEAF_CATEG_ID, T1.LSTG_SITE_ID, T1.SLR_SEGMENT_CD, T1.SELLER_ID, T1.ITEM_COUNT, T1.TEST_COUNT_DISTINCT_BITMAP, T1.DEAL_AMOUNT, T1.DEAL_YEAR, T1.BUYER_ID_AND_COUNTRY_NAME, T1.SELLER_ID_AND_COUNTRY_NAME, T1.BUYER_COUNTRY_ABBR, T1.SELLER_COUNTRY_ABBR, T2.ORDER_ID, T2.TEST_DATE_ENC, T2.TEST_TIME_ENC, T2.TEST_EXTENDED_COLUMN";
        Assert.assertEquals(expect1, newSelectClause1);
    }

    @Test
    public void testGetColsCanAccess() {
        List<String> empty = HackSelectStarWithColumnACL
                .getColsCanAccess("select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID " //
                        , PROJECT //
                        , SCHEMA //
                        , new HashSet<String>());
        Assert.assertEquals(0, empty.size());

        enableQueryPushDown();
        List<String> colsCanAccess = HackSelectStarWithColumnACL
                .getColsCanAccess("select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID " //
                        , PROJECT //
                        , SCHEMA //
                        , new HashSet<String>());
        Assert.assertEquals(22, colsCanAccess.size());
    }

    private void enableQueryPushDown() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
    }
}
