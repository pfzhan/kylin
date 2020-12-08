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

package io.kyligence.kap.smart;

import static io.kyligence.kap.smart.model.GreedyModelTreesBuilderTest.smartUtHook;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmartContextPartitionTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testAllFullLoadTableJoin() {
        /*
         * case 1: both are full load tables, result have two contexts
         * --     join
         * --    /    \
         * --  join    A
         * --  /  \
         * -- A    B
         */
        String[] sqls = new String[] {
                "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                        + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                        + "JOIN kylin_account seller_account ON buyer_account.account_country = seller_account.account_country\n"
                        + "LIMIT 500" };
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(2, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 2: both are full load tables, but result have one context
         * --     join
         * --    /    \
         * --  join    A
         * --  /  \
         * -- B    A
         */
        sqls = new String[] { "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                + "JOIN kylin_country seller_country ON buyer_country.country = seller_country.country\n"
                + "LIMIT 500" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context2);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(1, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(1, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 3: all tables are full load table, should have three contexts
         * --             join
         * --            /    \
         * --         join    join
         * --         /  \    /   \
         * --        A    B join  join
         * --               / \    /  \
         * --              A   C  E   join
         * --                         /  \
         * --                        D    A
         */
        sqls = new String[] { "SELECT SUM(price) AS sum_price\n" //
                + "FROM kylin_sales\n" //
                + "\tJOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_category_groupings ON kylin_category_groupings.site_id = kylin_sales.lstg_site_id\n"
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_country.country AS country, t3.account_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_country\n" //
                + "\t\t\t\t\tJOIN (\n" //
                + "\t\t\t\t\t\tSELECT kylin_account.account_id AS account_id, kylin_account.account_country AS account_country\n"
                + "\t\t\t\t\t\tFROM kylin_account\n"
                + "\t\t\t\t\t\t\tJOIN kylin_sales ON kylin_sales.seller_id = kylin_account.account_id\n"
                + "\t\t\t\t\t) t3\n" //
                + "\t\t\t\t\tON kylin_country.country = t3.account_country\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.seller_id = kylin_sales.seller_id\n" + "WHERE kylin_sales.part_dt < '2012-09-10'" };
        val context3 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context3);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(3, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(3, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 4: all tables are full load table, should have one context
         * --        join
         * --       /    \
         * --    join    join
         * --    /  \    /   \
         * --   A    B join  join
         * --         /  \   /  \
         * --        B    D  B   D
         */
        sqls = new String[] { "SELECT kylin_cal_dt.cal_dt AS cal_dt, SUM(kylin_sales.price) AS price\n" //
                + "FROM kylin_cal_dt\n" //
                + "\tJOIN kylin_sales ON kylin_cal_dt.cal_dt = kylin_sales.part_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id, t1.part_dt AS part_dt\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id, kylin_sales.part_dt AS part_dt\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_account ON kylin_sales.seller_id = kylin_account.account_id\n" //
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_sales.buyer_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\t\tJOIN kylin_account ON kylin_sales.buyer_id = kylin_account.account_id\n" //
                + "\t\t\t\tWHERE kylin_sales.part_dt > '2014-01-01'\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.part_dt = kylin_sales.part_dt\n" //
                + "GROUP BY kylin_cal_dt.cal_dt" };
        val context4 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context4);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(1, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(1, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }
        log.debug("ending testAllFullLoadTableJoin()");
    }

    @Test
    public void testIsRightSideIncrementalLoadTable() {

        /*
         * case 1: incremental table marks with '*', should have three contexts, but only two layouts
         * --     join
         * --    /    \
         * --  join    *A
         * --  /  \
         * -- B    *A
         */
        String[] sqls = new String[] {
                "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                        + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                        + "JOIN kylin_account seller_account ON buyer_account.account_country = seller_account.account_country\n"
                        + "LIMIT 500" };
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context1);

        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), proj);
        val kylinCountry = tableManager.getTableDesc("DEFAULT.KYLIN_COUNTRY");
        kylinCountry.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinCountry);

        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(3, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 2: incremental load table marks with '*', should have five contexts, and five layouts
         * --             join
         * --            /    \
         * --         join    join
         * --         /  \    /   \
         * --       *A    B join  join
         * --               / \    /  \
         * --             *A   C *E   join
         * --                         /  \
         * --                        D   *A
         */
        sqls = new String[] { "SELECT SUM(price) AS sum_price\n" //
                + "FROM kylin_sales\n" //
                + "\tJOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_category_groupings ON kylin_category_groupings.site_id = kylin_sales.lstg_site_id\n"
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_country.country AS country, t3.account_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_country\n" //
                + "\t\t\t\t\tJOIN (\n" //
                + "\t\t\t\t\t\tSELECT kylin_account.account_id AS account_id, kylin_account.account_country AS account_country\n"
                + "\t\t\t\t\t\tFROM kylin_account\n"
                + "\t\t\t\t\t\t\tJOIN kylin_sales ON kylin_sales.seller_id = kylin_account.account_id\n"
                + "\t\t\t\t\t) t3\n" //
                + "\t\t\t\t\tON kylin_country.country = t3.account_country\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.seller_id = kylin_sales.seller_id\n" + "WHERE kylin_sales.part_dt < '2012-09-10'" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context2);

        val kylinSales = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinSales.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinSales);

        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(5, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(5, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 3: incremental load table marks with '*', should have five contexts, but five layouts
         * --        join
         * --       /    \
         * --    left    join
         * --    /  \    /   \
         * --   A   *B join   join
         * --         /  \    /  \
         * --        *B   D   D   *B
         */
        sqls = new String[] { "SELECT kylin_cal_dt.cal_dt AS cal_dt, SUM(kylin_sales.price) AS price\n" //
                + "FROM kylin_cal_dt\n" //
                + "\tLEFT JOIN kylin_sales ON kylin_cal_dt.cal_dt = kylin_sales.part_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id, t1.part_dt AS part_dt\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id, kylin_sales.part_dt AS part_dt\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_account ON kylin_sales.seller_id = kylin_account.account_id\n" //
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_sales.buyer_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_account\n" //
                + "\t\t\t\t\tJOIN kylin_sales ON kylin_account.account_id = kylin_sales.buyer_id\n" //
                + "\t\t\t\tWHERE kylin_sales.part_dt > '2014-01-01'\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.part_dt = kylin_sales.part_dt\n" //
                + "GROUP BY kylin_cal_dt.cal_dt" };
        val context3 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context3);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(5, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(5, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        kylinCountry.setIncrementLoading(false);
        kylinSales.setIncrementLoading(false);
        tableManager.updateTableDesc(kylinCountry);
        tableManager.updateTableDesc(kylinSales);
        log.debug("ending testIsRightSideIncrementalLoadTable()");
    }

    @Test
    public void testCrossJoin() {
        /*
         * -- case 1: inner join as cross join
         * --    join
         * --   /    \
         * --  A      B
         */
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " inner join kylin_cal_dt on part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(2, smartContext.getModelContexts().size());
            Assert.assertEquals(2, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * -- case 2: cross join
         * --    join
         * --   /    \
         * --  A      B
         */
        sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from \n"
                + " kylin_sales, kylin_cal_dt where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context2);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(2, collectAllOlapContexts(smartContext).size());
            Assert.assertEquals(2, smartContext.getModelContexts().size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }
        log.debug("ending testCrossJoin()");
    }

    @Test
    public void testAllIncrementalLoadTableJoin() {

        /*
         * -- case 1: self join
         * --    join
         * --   /    \
         * --  A      A (alias B)
         */
        String[] sqls = new String[] { "SELECT t1.seller_id, t2.part_dt FROM kylin_sales t1\n"
                + "JOIN kylin_sales t2 ON t1.seller_id = t2.seller_id LIMIT 500" };

        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context1);

        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), proj);
        val kylinSalesTable = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinSalesTable.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinSalesTable);

        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(1, smartContext.getModelContexts().size());
            Assert.assertEquals(2, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * -- case 2: both table are incremental load, result have two context
         * --    join
         * --   /    \
         * --  A      B
         */
        sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " inner join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new SmartMaster(context2);

        val kylinCalDtTable = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        kylinCalDtTable.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinCalDtTable);

        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext smartContext = smartMaster.getContext();
            Assert.assertEquals(2, collectAllOlapContexts(smartContext).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        // reset to initial state
        kylinSalesTable.setIncrementLoading(false);
        kylinCalDtTable.setIncrementLoading(false);
        tableManager.updateTableDesc(kylinSalesTable);
        tableManager.updateTableDesc(kylinCalDtTable);
        log.debug("ending testAllIncrementalLoadTableJoin()");
    }
}
