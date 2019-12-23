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
package io.kyligence.kap.query;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import lombok.var;

public class RealizationChooserTest extends NLocalWithSparkSessionTest {

    private String project = "newten";

    @Test
    public void test_exactlyMatchModel_isBetterThan_PartialMatchModel() throws Exception {
        // 1. create small inner-join model
        String sql = "select CAL_DT, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' group by CAL_DT ";
        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        smartMaster.runAll();
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        Assert.assertFalse(smartMaster.getRecommendedModels().isEmpty());
        String dataflow = smartMaster.getRecommendedModels().get(0).getId();
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow);
        addLayout(df, 1000L);

        // 2. create a big inner-join model
        String sql1 = "select CAL_DT, count(*) from test_kylin_fact "
                + "inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "where CAL_DT='2012-01-10' group by CAL_DT ";
        NSmartMaster smartMaster1 = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, new String[] { sql1 });
        smartMaster1.runAll();
        Assert.assertFalse(smartMaster1.getRecommendedModels().isEmpty());
        NDataModel dataModel1 = smartMaster1.getRecommendedModels().get(0);
        String dataflow1 = dataModel1.getId();
        val df1 = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow1);
        addLayout(df1, 980L);

        // 3. config inner partial match inner join, then the small join should hit the small model.
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.match-partial-inner-join-model", "true");
        Assert.assertFalse(RealizationChooser.matchJoins(dataModel1, context).isEmpty());
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals(context.storageContext.getCandidate().getCuboidLayout().getModel().getId(), dataflow);

    }

    private void addLayout(NDataflow dataflow, long rowcount) {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        var indexPlan = indePlanManager.getIndexPlanByModelAlias(dataflow.getModelAlias());
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();

            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(0, 1));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(0, 1, 100000));
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newAggIndex.getId() + 2);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(1, 0, 100000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2));
            cuboids.add(newAggIndex);

            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater);

        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout1 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 10001L);
        layout1.setRows(rowcount);
        NDataLayout layout2 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 10002L);
        layout2.setRows(rowcount);
        dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(dataflowUpdate);
    }

}
