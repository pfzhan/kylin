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

import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;
import lombok.val;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class NModelMasterTest extends NTestBase {
    @Test
    public void testNormal() throws IOException {
        preparePartition();

        String[] sqls = new String[]{ //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales left join kylin_cal_dt on cal_dt = part_dt group by part_dt" //
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
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
            Assert.assertEquals(NDataModel.TableKind.LOOKUP, joins.get(0).getKind());
            Assert.assertEquals("left", joins.get(0).getJoin().getType());
            Assert.assertEquals("DEFAULT.KYLIN_CAL_DT", joins.get(0).getTable());
            Assert.assertEquals("KYLIN_CAL_DT", joins.get(0).getAlias());
            Assert.assertArrayEquals(new String[]{"KYLIN_CAL_DT.CAL_DT"}, joins.get(0).getJoin().getPrimaryKey());
            Assert.assertArrayEquals(new String[]{"KYLIN_SALES.PART_DT"}, joins.get(0).getJoin().getForeignKey());
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
            Assert.assertEquals("SUM_PRICE", allMeasures.get(1).getName());
            Assert.assertEquals("SUM_ITEM_COUNT", allMeasures.get(2).getName());
        }

        // propose again, should return same result
        NDataModel dm2 = modelMaster.proposeScope(dataModel);
        Assert.assertEquals(dm2, dataModel);
    }

    @Test
    public void testSqlWithoutPartition() throws IOException {

        String[] sqls = new String[]{
                "SELECT kylin_category_groupings.meta_categ_name, kylin_category_groupings.categ_lvl2_name, "
                        + " sum(kylin_sales.price) as GMV, count(*) as trans_cnt"
                        + " FROM kylin_sales inner JOIN kylin_category_groupings"
                        + " ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id"
                        + " AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id"
                        + " group by kylin_category_groupings.meta_categ_name ,kylin_category_groupings.categ_lvl2_name" //
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
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
            Assert.assertEquals(5, dataModel.getAllNamedColumns().size());
            Assert.assertEquals(-1, dataModel.getColumnIdByColumnName("KYLIN_SALES.PART_DT"));
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
            Assert.assertEquals(6, dataModel.getAllNamedColumns().size());
            Assert.assertNotEquals(-1, dataModel.getColumnIdByColumnName("KYLIN_SALES.PART_DT"));
            Assert.assertEquals(2, dataModel.getAllMeasures().size());
            Assert.assertEquals(1, dataModel.getJoinTables().size());
        }
    }

    @Test
    public void testSqlWithEmptyAllColsInContext() throws IOException {
        String[] sqls = new String[]{
                "SELECT count(*) as cnt from " +
                        "KYLIN_SALES as KYLIN_SALES  \n" +
                        "INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT \n" +
                        "INNER JOIN KYLIN_CATEGORY_GROUPINGS as KYLIN_CATEGORY_GROUPINGS \n" +
                        "    ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID \n" +
                        "INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID \n" +
                        "INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY \n"
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.analyzeSQLs();

        NSmartContext ctx = smartMaster.getContext();
        Assert.assertEquals(1, ctx.getOlapContexts().values().size());
        Assert.assertEquals(0, Lists.newArrayList(ctx.getOlapContexts().values()).get(0).iterator().next().allColumns.size());
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
            Assert.assertEquals(5, dataModel.getAllNamedColumns().size());
        }
    }

    private NDataLoadingRange preparePartition() throws IOException {
        String tableName = "DEFAULT.KYLIN_SALES";
        String columnName = "KYLIN_SALES.PART_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(proj);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2013-01-01");

        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig, proj);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        return savedDataLoadingRange;
    }
}