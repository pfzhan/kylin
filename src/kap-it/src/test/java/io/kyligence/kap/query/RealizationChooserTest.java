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

import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
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
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import lombok.var;

import java.sql.SQLException;
import java.util.List;

public class RealizationChooserTest extends NLocalWithSparkSessionTest {

    private String project = "newten";

    @Test
    public void test_exactlyMatchModel_isBetterThan_PartialMatchModel() throws Exception {
        // 1. create small inner-join model
        String sql = "select CAL_DT, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' group by CAL_DT ";
        val proposeContext = new NSmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        NSmartMaster smartMaster = new NSmartMaster(proposeContext);
        smartMaster.runWithContext();
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
        val proposeContext2 = new NSmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql1 });
        NSmartMaster smartMaster1 = new NSmartMaster(proposeContext2);
        smartMaster1.runWithContext();
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

    @Test
    public void test_sortByCandidatesId_when_candidatesCostAreTheSame() {
        // can be answered by both [nnmodel_basic] & [nmodel_basic_inner]
        String sql = "select count(*) from TEST_ACCOUNT group by ACCOUNT_ID";
        val proposeContext = new NSmartContext(KylinConfig.getInstanceFromEnv(), "default", new String[] { sql });
        NSmartMaster smartMaster = new NSmartMaster(proposeContext);
        smartMaster.runWithContext();
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals("nmodel_basic_inner", context.realization.getModel().getAlias());
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

    @Test
    public void testHeterogeneousSegment() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2012-01-02");
        val segmentRange2 = Pair.newPair("2012-01-02", "2012-01-03");
        val segmentRange3 = Pair.newPair("2012-01-03", "2012-01-04");
        val segmentRange4 = Pair.newPair("2012-01-04", "2012-01-05");
        val segmentRange5 = Pair.newPair("2012-01-05", "2012-01-06");
        val layout_20000000001 = 20000000001L;
        val layout_20001 = 20001L;
        val layout_10001 = 10001L;
        val layout_1 = 1L;

        val sql = "select cal_dt, sum(price) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id ";

        val no_filter = sql + "group by cal_dt";
        try {
            assertPrunedSegmentsRange(project, no_filter, dfId, null, -1L);
        } catch (NoRealizationFoundException ex) {
            // segments do not have common layout
        }

        val sql1_date = sql + "where cal_dt = DATE '2012-01-01' or (cal_dt >= DATE '2012-01-02' and cal_dt < DATE '2012-01-04') group by cal_dt";
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertPrunedSegmentsRange(project, sql1_date, dfId, expectedRanges, layout_20001);

        val sql1_date_string = sql + "where cal_dt = '2012-01-01' or (cal_dt >= '2012-01-02' and cal_dt < '2012-01-04') group by cal_dt";
        assertPrunedSegmentsRange(project, sql1_date_string, dfId, expectedRanges, layout_20001);

        val sql2_date = sql + "where cal_dt >= DATE '2012-01-03' and cal_dt < DATE '2012-01-10' group by cal_dt";
        expectedRanges.clear();
        expectedRanges.add(segmentRange3);
        expectedRanges.add(segmentRange4);
        expectedRanges.add(segmentRange5);
        assertPrunedSegmentsRange(project, sql2_date, dfId, expectedRanges, layout_10001);

        val sql2_date_string = sql + "where cal_dt >= '2012-01-03' and cal_dt < '2012-01-10' group by cal_dt";
        assertPrunedSegmentsRange(project, sql2_date_string, dfId, expectedRanges, layout_10001);

        val sql3_no_layout = "select trans_id from test_kylin_fact " +
                "inner join test_account on test_kylin_fact.seller_id = test_account.account_id " +
                "where cal_dt > '2012-01-03' and cal_dt < '2012-01-05'";
        try {
            assertPrunedSegmentsRange(project, sql3_no_layout, dfId, expectedRanges, -1L);
        } catch (NoRealizationFoundException ex) {
            // pruned segments do not have capable layout to answer
        }

        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        val sql4_table_index = "select trans_id from test_kylin_fact " +
                "inner join test_account on test_kylin_fact.seller_id = test_account.account_id " +
                "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03'";
        assertPrunedSegmentsRange(project, sql4_table_index, dfId, expectedRanges, layout_20000000001);

        val sql5 = "select trans_id, sum(price) " +
                "from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id " +
                "where cal_dt > '2012-01-03' and cal_dt < '2012-01-06' group by trans_id";
        try {
            assertPrunedSegmentsRange(project, sql5, dfId, expectedRanges, -1L);
        } catch (NoRealizationFoundException ex) {
            // pruned segments do not have capable layout to answer
        }
    }

    private void assertPrunedSegmentsRange(String project, String sql, String dfId, List<Pair<String, String>> expectedRanges, long expectedLayoutId) throws SQLException {
        val proposeContext = new NSmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        NSmartMaster smartMaster = new NSmartMaster(proposeContext);
        smartMaster.runWithContext();
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);

        val prunedSegments = context.storageContext.getPrunedSegments();
        val candidate = context.storageContext.getCandidate();
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        Assert.assertEquals(expectedLayoutId, candidate.getCuboidLayout().getId());

        val model = NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);
        }
    }
}
