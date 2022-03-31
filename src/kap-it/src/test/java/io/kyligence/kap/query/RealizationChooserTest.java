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

import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_MODEL_NOT_FOUND;

import com.google.common.collect.Lists;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.realization.HybridRealization;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerBuilder;
import io.kyligence.kap.util.AccelerationContextUtil;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.junit.Assert;
import org.junit.Test;


public class RealizationChooserTest extends NLocalWithSparkSessionTest {

    private String project = "newten";

    @Test
    public void test_exactlyMatchModel_isBetterThan_PartialMatchModel() throws Exception {
        // 1. create small inner-join model
        String sql = "select CAL_DT, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' group by CAL_DT ";
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        Assert.assertFalse(smartMaster.context.getProposedModels().isEmpty());
        String dataflow = smartMaster.context.getProposedModels().get(0).getId();
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow);
        addLayout(df, 1000L);

        // 2. create a big inner-join model
        String sql1 = "select CAL_DT, count(*) from test_kylin_fact "
                + "inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "where CAL_DT='2012-01-10' group by CAL_DT ";
        val proposeContext2 = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql1 });
        SmartMaster smartMaster1 = new SmartMaster(proposeContext2);
        smartMaster1.runUtWithContext(null);
        proposeContext2.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext2);
        Assert.assertFalse(smartMaster1.context.getProposedModels().isEmpty());
        NDataModel dataModel1 = smartMaster1.context.getProposedModels().get(0);
        String dataflow1 = dataModel1.getId();
        val df1 = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow1);
        addLayout(df1, 980L);

        // 3. config inner partial match inner join, then the small join should hit the small model.
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.match-partial-inner-join-model", "true");
        Assert.assertFalse(RealizationChooser.matchJoins(dataModel1, context).isEmpty());
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals(context.storageContext.getCandidate().getLayoutEntity().getModel().getId(), dataflow);

    }

    @Test
    public void test_sortByCandidatesId_when_candidatesCostAreTheSame() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject())
                .getTableDesc("DEFAULT.TEST_ACCOUNT").setLastSnapshotPath(
                        "default/table_snapshot/DEFAULT.TEST_ACCOUNT/d6ba492b-13bf-444d-b6e3-71bfa903344d");

        // can be answered by both [nnmodel_basic] & [nmodel_basic_inner]
        String sql = "select count(*) from TEST_ACCOUNT group by ACCOUNT_ID";
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), "default", new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
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
    public void testNonEquiJoinMatch() {
        //   TEST_KYLIN_FACT
        //              \
        //             TEST_ACCOUNT  on equi-join
        //                  \
        //              SELLER_ACCOUNT  on non-equi-join
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        // 1. create small inner-join model
        String sql = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON SELLER_ACCOUNT.ACCOUNT_BUYER_LEVEL = TEST_ORDER.BUYER_ID "
                + "AND SELLER_ACCOUNT.ACCOUNT_COUNTRY>=TEST_ORDER.TEST_EXTENDED_COLUMN "
                + "AND SELLER_ACCOUNT.ACCOUNT_COUNTRY<TEST_ORDER.TEST_TIME_ENC";
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);

        Assert.assertEquals(smartMaster.getContext().getModelContexts().size(), 1);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);

        Assert.assertEquals(context.joins.get(1).getFKSide().getTableIdentity(), "DEFAULT.TEST_ACCOUNT");
    }

    @Test
    public void testHybridStreaming() {
        String project = "streaming_test";
        String sql = "select count(*), sum(LO_LINENUMBER) from SSB_STREAMING";
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        IRealization realization = context.realization;
        Assert.assertTrue(realization instanceof HybridRealization);
        HybridRealization hybridRealization = (HybridRealization)realization;
        Assert.assertTrue(hybridRealization.getBatchRealization() instanceof NDataflow);
        Assert.assertTrue(hybridRealization.getStreamingRealization() instanceof NDataflow);
        Assert.assertTrue(hybridRealization.getStreamingRealization().isStreaming());
        Assert.assertFalse(realization.getAllColumnDescs().isEmpty());
        Assert.assertFalse(realization.getAllDimensions().isEmpty());
        Assert.assertTrue(realization.getDateRangeStart() < realization.getDateRangeEnd());
        Assert.assertEquals(realization.getStorageType(), IStorageAware.ID_NDATA_STORAGE);
        Assert.assertNotNull(realization.getUuid());
        Assert.assertFalse(realization.isStreaming());
        Assert.assertEquals(realization.getProject(), project);
        FunctionDesc functionDesc = hybridRealization.getBatchRealization().getMeasures().get(1).getFunction();
        Assert.assertNotNull(hybridRealization.findAggrFunc(functionDesc));
        Assert.assertTrue(hybridRealization.hasPrecalculatedFields());
        Assert.assertEquals("model_streaming", context.realization.getModel().getAlias());
        Assert.assertFalse(context.storageContext.isBatchCandidateEmpty());
        Assert.assertFalse(context.storageContext.isStreamCandidateEmpty());

        hybridRealization.setUuid("123");
        hybridRealization.setProject(project);
        Assert.assertEquals(hybridRealization.getUuid(), "123");

        HybridRealization hybridTest = new HybridRealization(hybridRealization.getBatchRealization(), null, "");
        Assert.assertNull(hybridTest.getBatchRealization());

        hybridTest = new HybridRealization(null, null, "");
        Assert.assertNull(hybridTest.getBatchRealization());
        Assert.assertNull(hybridTest.getStreamingRealization());

        hybridTest = new HybridRealization(null, hybridRealization.getStreamingRealization(), "");
        Assert.assertNull(hybridTest.getStreamingRealization());

        Assert.assertFalse(hybridRealization.isCapable(context.getSQLDigest(), Lists.newArrayList()).capable);

    }

    @Test
    public void testHybridStreamingMatchLookUp() throws Exception {
        String project = "streaming_test";
        String sql = "select * from SSB.CUSTOMER";
        NTableMetadataManager.getInstance(getTestConfig(), project)
                .getTableDesc("SSB.CUSTOMER").setLastSnapshotPath(
                "default/table_snapshot/SSB.CUSTOMER/cf3eddab-4686-721c-ee1f-a502ed95163e");

        AbstractQueryRunner queryRunner1 = new QueryRunnerBuilder(project, getTestConfig(), new String[] { sql }).build();
        queryRunner1.execute();
        Map<String, Collection<OLAPContext>> olapContexts = queryRunner1.getOlapContexts();

        OLAPContext context = olapContexts.get(sql).iterator().next();
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());

        RealizationChooser.attemptSelectCandidate(context);
        IRealization realization = context.realization;

        Assert.assertTrue(realization instanceof HybridRealization);
        HybridRealization hybridRealization = (HybridRealization)realization;
        Assert.assertEquals(hybridRealization.getUuid(), "14e00a6f-d910-14b6-ee67-e0a5775012c4");
    }

    @Test
    public void testHybridNoRealization() {
        String project = "streaming_test";
        String sql = "select LO_SHIPMODE from SSB_STREAMING group by LO_SHIPMODE";
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        try {
            RealizationChooser.attemptSelectCandidate(context);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(), STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), String.format(Locale.ROOT, MsgPicker.getMsg().getNO_STREAMING_MODEL_FOUND()));
            Assert.assertTrue(context.storageContext.isBatchCandidateEmpty());
            Assert.assertTrue(context.storageContext.isStreamCandidateEmpty());
        }
        Assert.assertNull(context.realization);
    }

    @Test
    public void testHybridSegmentOverRange() {
        String project = "streaming_test";
        String sql = "select count(*) from SSB_STREAMING where LO_PARTITIONCOLUMN > '2021-07-01 00:00:00'";
        KylinConfig.getInstanceFromEnv().setProperty("kylin.smart.conf.memory-tuning", "false");
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals("model_streaming", context.realization.getModel().getAlias());
        Assert.assertTrue(context.storageContext.isEmptyLayout());

        String sql2 = "select count(*) from SSB_STREAMING where LO_PARTITIONCOLUMN >= '2021-05-28 15:25:00'";
        KylinConfig.getInstanceFromEnv().setProperty("kylin.smart.conf.memory-tuning", "false");
        val proposeContext2 = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql2 });
        SmartMaster smartMaster2 = new SmartMaster(proposeContext2);
        smartMaster2.runUtWithContext(null);
        proposeContext2.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext2);
        OLAPContext context2 = Lists
                .newArrayList(smartMaster2.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context2.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context2);
        Assert.assertEquals("model_streaming", context2.realization.getModel().getAlias());
        Assert.assertTrue(context2.storageContext.isBatchCandidateEmpty());
        Assert.assertFalse(context2.storageContext.isStreamCandidateEmpty());
        Assert.assertEquals(context2.storageContext.getStreamingCandidate(), context2.storageContext.getCandidate());
    }

    @Test
    public void testHybridNoStreamingRealization() {
        String project = "streaming_test";
        String sql1 = "select min(LO_SHIPMODE) from SSB_STREAMING where LO_PARTITIONCOLUMN < '2021-05-01 00:00:00'";
        String sql2 = "select min(LO_SHIPMODE) from SSB_STREAMING where LO_PARTITIONCOLUMN > '2021-05-01 00:00:00'";
        KylinConfig.getInstanceFromEnv().setProperty("kylin.smart.conf.memory-tuning", "false");
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql1 });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        try {
            RealizationChooser.attemptSelectCandidate(context);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(), STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), String.format(Locale.ROOT, MsgPicker.getMsg().getNO_STREAMING_MODEL_FOUND()));
        }
        Assert.assertTrue(context.storageContext.isStreamCandidateEmpty());

        val proposeContext2 = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql2 });
        SmartMaster smartMaster2 = new SmartMaster(proposeContext2);
        smartMaster2.runUtWithContext(null);
        proposeContext2.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext2);
        OLAPContext context2 = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context2.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        try {
            RealizationChooser.attemptSelectCandidate(context2);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(), STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), String.format(Locale.ROOT, MsgPicker.getMsg().getNO_STREAMING_MODEL_FOUND()));
        }
        Assert.assertTrue(context2.storageContext.isBatchCandidateEmpty());
    }

    @Test
    public void testHybridDimensionAsMetrics() {
        String project = "streaming_test";
        String sql = "select min(LO_CUSTKEY) from SSB_STREAMING";
        KylinConfig.getInstanceFromEnv().setProperty("kylin.smart.conf.memory-tuning", "false");
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals(30001L, context.storageContext.getStreamingLayoutId().longValue());
        Assert.assertEquals(30001L, context.storageContext.getLayoutId().longValue());
    }
}
