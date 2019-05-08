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

package io.kyligence.kap.smart.model;

import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.PartitionDesc;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import lombok.val;

public class NModelMasterTest extends NAutoTestOnLearnKylinData {
    @Test
    public void testNormal() {
        preparePartition();

        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales left join kylin_cal_dt on cal_dt = part_dt group by part_dt" //
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.analyzeSQLs();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        NModelMaster modelMaster = new NModelMaster(mdCtx);

        // propose initial cube
        NDataModel dataModel = modelMaster.proposeInitialModel();
        {
            Assert.assertNotNull(dataModel);
            List<NDataModel.Measure> allMeasures = dataModel.getAllMeasures();
            Assert.assertTrue(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(1, allMeasures.size());
            Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
        }

        dataModel = modelMaster.proposeJoins(dataModel);
        {
            val joins = dataModel.getJoinTables();
            Assert.assertNotNull(joins);
            Assert.assertEquals(1, joins.size());
            Assert.assertEquals(NDataModel.TableKind.FACT, joins.get(0).getKind());
            Assert.assertEquals("left", joins.get(0).getJoin().getType());
            Assert.assertEquals("DEFAULT.KYLIN_CAL_DT", joins.get(0).getTable());
            Assert.assertEquals("KYLIN_CAL_DT", joins.get(0).getAlias());
            Assert.assertArrayEquals(new String[] { "KYLIN_CAL_DT.CAL_DT" }, joins.get(0).getJoin().getPrimaryKey());
            Assert.assertArrayEquals(new String[] { "KYLIN_SALES.PART_DT" }, joins.get(0).getJoin().getForeignKey());
        }

        // propose again, should return same result
        NDataModel dm1 = modelMaster.proposeJoins(dataModel);
        Assert.assertEquals(dm1, dataModel);

        dataModel = modelMaster.proposePartition(dataModel);
        {
            PartitionDesc partition = dataModel.getPartitionDesc();
            Assert.assertNotNull(partition);
            Assert.assertTrue(partition.isPartitioned());
            Assert.assertEquals("KYLIN_SALES.PART_DT", partition.getPartitionDateColumn());
        }

        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            List<NDataModel.Measure> allMeasures = dataModel.getAllMeasures();
            Assert.assertFalse(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(3, allMeasures.size());
            Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
            Assert.assertEquals("SUM_PRICE", allMeasures.get(1).getName());
        }

        // propose again, should return same result
        NDataModel dm2 = modelMaster.proposeScope(dataModel);
        Assert.assertEquals(dm2, dataModel);
    }

    @Test
    public void testSqlWithoutPartition() {

        String[] sqls = new String[] {
                "SELECT kylin_category_groupings.meta_categ_name, kylin_category_groupings.categ_lvl2_name, "
                        + " sum(kylin_sales.price) as GMV, count(*) as trans_cnt"
                        + " FROM kylin_sales inner JOIN kylin_category_groupings"
                        + " ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id"
                        + " AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id"
                        + " group by kylin_category_groupings.meta_categ_name ,kylin_category_groupings.categ_lvl2_name" //
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.analyzeSQLs();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        NModelMaster modelMaster = new NModelMaster(mdCtx);

        // propose model without partition column
        NDataModel dataModel = modelMaster.proposeInitialModel();
        dataModel = modelMaster.proposeJoins(dataModel);
        dataModel = modelMaster.proposePartition(dataModel);
        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(48, dataModel.getAllNamedColumns().size());
            Assert.assertEquals(43, dataModel.getColumnIdByColumnName("KYLIN_SALES.PART_DT"));
            Assert.assertEquals(2, dataModel.getAllMeasures().size());
            Assert.assertEquals(1, dataModel.getJoinTables().size());
        }

        // propose model with partition column
        preparePartition();
        dataModel = modelMaster.proposeJoins(dataModel);
        dataModel = modelMaster.proposePartition(dataModel);
        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(48, dataModel.getAllNamedColumns().size());
            Assert.assertEquals(2, dataModel.getAllMeasures().size());
            Assert.assertEquals(1, dataModel.getJoinTables().size());
        }
    }

    @Test
    public void testErrorInNQueryScopeProposer() {
        // we expect sqls can be accelerated will not blocked by its counterpart
        String[] sqls = new String[] { //
                " SELECT SUM((CASE WHEN 1.1000000000000001 = 0 THEN CAST(NULL AS DOUBLE) "
                        + "ELSE \"TEST_KYLIN_FACT\".\"PRICE\" / 1.1000000000000001 END)) "
                        + "AS \"sum_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                " SELECT SUM(\"TEST_KYLIN_FACT\".\"PRICE\" * 2 + 2) "
                        + "AS \"double_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"",

        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", sqls);
        smartMaster.runAll();

        final NSmartContext smartContext = smartMaster.getContext();
        final List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());

        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sqls[0]).isFailed());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerateInfoMap.get(sqls[0]).getFailedCause().getMessage());

        Assert.assertTrue(accelerateInfoMap.get(sqls[1]).isFailed());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerateInfoMap.get(sqls[1]).getFailedCause().getMessage());

        Assert.assertFalse(accelerateInfoMap.get(sqls[2]).isFailed());
        Assert.assertEquals(1, accelerateInfoMap.get(sqls[2]).getRelatedLayouts().size());
    }

    @Test
    public void testSqlWithEmptyAllColsInContext() {
        String[] sqls = new String[] { "SELECT count(*) as cnt from " + "KYLIN_SALES as KYLIN_SALES  \n"
                + "INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT \n"
                + "INNER JOIN KYLIN_CATEGORY_GROUPINGS as KYLIN_CATEGORY_GROUPINGS \n"
                + "    ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID \n"
                + "INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID \n"
                + "INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY \n" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.analyzeSQLs();

        NSmartContext ctx = smartMaster.getContext();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(0,
                ctx.getModelContexts().get(0).getModelTree().getOlapContexts().iterator().next().allColumns.size());
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        NModelMaster modelMaster = new NModelMaster(mdCtx);
        // propose model
        NDataModel dataModel = modelMaster.proposeInitialModel();
        dataModel = modelMaster.proposeJoins(dataModel);
        dataModel = modelMaster.proposePartition(dataModel);
        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(157, dataModel.getAllNamedColumns().size());
        }
    }

    private void preparePartition() {
        String tableName = "DEFAULT.KYLIN_SALES";
        String columnName = "KYLIN_SALES.PART_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);

        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), proj);
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
    }
}