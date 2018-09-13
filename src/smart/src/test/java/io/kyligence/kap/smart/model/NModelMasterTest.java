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

import java.io.IOException;

import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;

public class NModelMasterTest extends NTestBase {
    @Test
    public void test() throws IOException {
        preparePartition();

        NSmartContext.NModelContext mdCtx = getModelContext();
        Assert.assertNotNull(mdCtx);

        NModelMaster modelMaster = new NModelMaster(mdCtx);

        // propose initial cube
        NDataModel dataModel = modelMaster.proposeInitialModel();
        {
            Assert.assertNotNull(dataModel);
            Assert.assertTrue(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(1, dataModel.getAllMeasures().size());
        }

        dataModel = modelMaster.proposeJoins(dataModel);
        {
            JoinTableDesc[] joins = dataModel.getJoinTables();
            Assert.assertNotNull(joins);
            Assert.assertEquals(1, joins.length);
            Assert.assertEquals(NDataModel.TableKind.LOOKUP, joins[0].getKind());
            Assert.assertEquals("left", joins[0].getJoin().getType());
            Assert.assertEquals("DEFAULT.KYLIN_CAL_DT", joins[0].getTable());
            Assert.assertEquals("KYLIN_CAL_DT", joins[0].getAlias());
            Assert.assertArrayEquals(new String[] { "KYLIN_CAL_DT.CAL_DT" }, joins[0].getJoin().getPrimaryKey());
            Assert.assertArrayEquals(new String[] { "KYLIN_SALES.PART_DT" }, joins[0].getJoin().getForeignKey());
        }

        dataModel = modelMaster.proposePartition(dataModel);
        {
            PartitionDesc partition = dataModel.getPartitionDesc();
            Assert.assertNotNull(partition);
            Assert.assertTrue(partition.isPartitioned());
            Assert.assertEquals("KYLIN_SALES.PART_DT", partition.getPartitionDateColumn());
        }

        // propose again, should return same result
        NDataModel dm1 = modelMaster.proposeJoins(dataModel);
        Assert.assertEquals(dm1, dataModel);

        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertFalse(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(3, dataModel.getAllMeasures().size());
        }

        // propose again, should return same result
        NDataModel dm2 = modelMaster.proposeScope(dataModel);
        Assert.assertEquals(dm2, dataModel);
    }

    private NSmartContext.NModelContext getModelContext() throws IOException {
        String[] sqls = new String[] { //
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
        return ctx.getModelContexts().get(0);
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