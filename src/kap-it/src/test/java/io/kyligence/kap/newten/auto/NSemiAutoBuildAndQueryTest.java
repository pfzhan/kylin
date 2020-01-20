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

package io.kyligence.kap.newten.auto;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.KylinTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.tool.garbage.IndexCleaner;
import lombok.val;

public class NSemiAutoBuildAndQueryTest extends SemiAutoTestBase {
    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kap.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kap.smart.conf.auto-modeling.non-equi-join.enabled", "TRUE");
        overwriteSystemProp("kylin.garbage.customized-strategy-enabled", "false");
        super.setup();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "default";
    }


    @Test
    public void testTwoModelLayout() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/multi_model") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(2, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().get(0).getNextLayoutRecommendationItemId());
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().get(1).getNextLayoutRecommendationItemId());
    }

    @Test
    public void testOptimize() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_layout_1") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getIndexes().get(0).getLayouts().size());
        executeTestScenario(new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_layout_2"));
        Assert.assertEquals(2, indexPlanManager.listAllIndexPlans().get(0).getIndexes().get(0).getLayouts().size());
        proposeWithSmartMaster(getProject(),
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_layout_3"));
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().get(0).getDimensionRecommendations().size());
        val nextDimensionId = optManager.listAllOptimizeRecommendations().get(0).getNextDimensionRecommendationItemId();
        executeTestScenario(new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_layout_4"));
        Assert.assertEquals(nextDimensionId,
                optManager.listAllOptimizeRecommendations().get(0).getNextDimensionRecommendationItemId());
    }

    @Test
    public void testOptimize_included() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_1"),
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_2") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
    }

    @Test
    public void testOptimize_includedShortLong() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_1") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
        executeTestScenario(new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_2"));//
        Assert.assertEquals(2, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
        new IndexCleaner().cleanup(getProject());
        verifyAll();
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
    }

    @Test
    public void testOptimize_includedLongShort() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_2") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
        executeTestScenario(new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/include_1"));//
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
    }

    @Test
    public void testOptimize_sameDimension() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/same_dimension") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(2, indexPlanManager.listAllIndexPlans().get(0).getAllLayouts().size());
    }

    @Test
    public void testOptimize_aggAndTable() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_agg_and_table") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        Assert.assertEquals(2, indexPlanManager.listAllIndexPlans().get(0).getWhitelistLayouts().size());
    }

    @Test
    public void testOptimize_allDimensionType() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/dimension_types") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(1, modelManager.listAllModels().size());
        val allDimension = modelManager.listAllModels().get(0).getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).map(NDataModel.NamedColumn::getId)
                .collect(Collectors.toSet());
        val cols = modelManager.listAllModels().get(0).getEffectiveCols().entrySet().stream()
                .filter(e -> allDimension.contains(e.getKey())).map(Map.Entry::getValue).collect(Collectors.toSet());
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("char"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("varchar"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("int"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("timestamp"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("boolean"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("tinyint"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("smallint"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("bigint"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("double"))));
        Assert.assertTrue(cols.stream().anyMatch(col -> col.getType().equals(DataType.getType("decimal"))));
    }

    private void prepareShardByColumn() {
        val tableExtManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val factTable = tableExtManager.copyForWrite(tableExtManager.getOrCreateTableExt("DEFAULT.TEST_KYLIN_FACT"));
        val columnStatus = factTable.getAllColumnStats().stream()
                .collect(Collectors.toMap(TableExtDesc.ColumnStats::getColumnName, columnStats -> columnStats));
        val column = columnStatus.getOrDefault("LSTG_FORMAT_NAME", new TableExtDesc.ColumnStats());
        if (!columnStatus.containsKey("LSTG_FORMAT_NAME")) {
            column.setColumnName("LSTG_FORMAT_NAME");
            column.setTableExtDesc(factTable);
        }
        column.setCardinality(Long.MAX_VALUE);
        columnStatus.put("LSTG_FORMAT_NAME", column);
        factTable.setColumnStats(Lists.newArrayList(columnStatus.values()));
        tableExtManager.mergeAndUpdateTableExt(tableExtManager.getOrCreateTableExt("DEFAULT.TEST_KYLIN_FACT"),
                factTable);
    }

    @Test
    public void testOptimize_shardBy() throws Exception {
        prepareShardByColumn();
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/shard_by") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//

        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        val indexPlan = indexPlanManager.listAllIndexPlans().get(0);
        Assert.assertEquals(3, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllLayouts().stream()
                .filter(l -> !l.getIndex().isTableIndex() && l.getShardByColumns().size() == 1).count());
        Assert.assertEquals(1, indexPlan.getAllLayouts().stream()
                .filter(l -> l.getIndex().isTableIndex() && l.getShardByColumns().size() == 1).count());
        Assert.assertEquals(1, indexPlan.getAllLayouts().stream()
                .filter(l -> l.getIndex().isTableIndex() && l.getShardByColumns().size() == 0).count());
    }

    @Test
    public void testOptimize_measures() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/measures") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        val measures = modelManager.listAllModels().get(0).getAllMeasures();
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_SUM)));
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_PERCENTILE)));
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_COUNT)));
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_MIN)));
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_MAX)));
        Assert.assertTrue(measures.stream()
                .anyMatch(measure -> measure.getFunction().getExpression().equals(FunctionDesc.FUNC_COUNT_DISTINCT)));
    }

    @Test
    public void testOptimize_cc() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/cc") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        assertCC();

    }

    @Test
    public void testOptimize_cc2() throws Exception {
        testOptimize_cc();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModelIds().forEach(id -> {
            dataflowManager.dropDataflow(id);
            indexPlanManager.dropIndexPlan(id);
            modelManager.updateDataModel(id, copyForWrite -> {
                val ccs = copyForWrite.getComputedColumnDescs().stream().map(ComputedColumnDesc::getFullName)
                        .collect(Collectors.toSet());
                copyForWrite.setComputedColumnDescs(Lists.newArrayList());
                copyForWrite.getAllNamedColumns().stream().filter(n -> ccs.contains(n.getAliasDotColumn()))
                        .forEach(n -> n.setStatus(NDataModel.ColumnStatus.TOMB));
                copyForWrite.getAllMeasures().stream().filter(m -> m.getFunction().getParameters().stream().anyMatch(
                        p -> p.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN) && ccs.contains(p.getValue())))
                        .forEach(m -> m.setTomb(true));

            });
            val indexPlan = new IndexPlan();
            indexPlan.setUuid(id);
            indexPlanManager.createIndexPlan(indexPlan);
            dataflowManager.createDataflow(indexPlan, "ADMIN");
        });
        executeTestScenario(
                /* CompareLevel = SAME */
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/cc"));//
        assertCC();
    }

    @Test
    @Ignore("#17042")
    public void testOptimize_cc3() throws Exception {
        testOptimize_cc();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModelIds().forEach(id -> {
            dataflowManager.dropDataflow(id);
            indexPlanManager.dropIndexPlan(id);
            val indexPlan = new IndexPlan();
            indexPlan.setUuid(id);
            indexPlanManager.createIndexPlan(indexPlan);
            dataflowManager.createDataflow(indexPlan, "ADMIN");
        });
        executeTestScenario(
                /* CompareLevel = SAME */
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/cc_2"));//
        val model = modelManager.listAllModels().get(0);
        Assert.assertEquals(2, model.getComputedColumnDescs().size());
        val ccs = model.getComputedColumnDescs().stream().map(ComputedColumnDesc::getFullName)
                .collect(Collectors.toSet());
        val ccIds = model.getAllNamedColumns().stream().filter(n -> ccs.contains(n.getAliasDotColumn()))
                .map(NDataModel.NamedColumn::getId).collect(Collectors.toSet());
        Assert.assertTrue(indexPlanManager.getIndexPlan(model.getId()).getAllLayouts().stream()
                .noneMatch(l -> Sets.intersection(ccIds, Sets.newHashSet(l.getColOrder())).isEmpty()));

    }

    private void assertCC() {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        val ccs = modelManager.listAllModels().get(0).getComputedColumnDescs();
        Assert.assertEquals(1, ccs.size());
        val cc = ccs.get(0);
        val sql1 = String.format("select seller_id ,sum(%s) as GMV, count(1) as TRANS_CNT from test_kylin_fact\n"
                + " group by LSTG_FORMAT_NAME ,seller_id", cc.getColumnName());
        val sql2 = String.format("select seller_id ,sum(%s) as GMV, count(1) as TRANS_CNT from test_kylin_fact\n"
                + " group by LSTG_FORMAT_NAME ,seller_id", cc.getExpression());
        val res1 = NExecAndComp.queryFromCube(getProject(),
                KylinTestBase.changeJoinType(sql1, NExecAndComp.CompareLevel.SAME.name()));
        val res2 = NExecAndComp.queryFromCube(getProject(),
                KylinTestBase.changeJoinType(sql2, NExecAndComp.CompareLevel.SAME.name()));
        Assert.assertTrue(NExecAndComp.compareResults(NExecAndComp.normRows(res1.toJavaRDD().collect()),
                NExecAndComp.normRows(res2.toJavaRDD().collect()), NExecAndComp.CompareLevel.SAME));
    }

    @Test
    public void testOptimize_sameIndexLayouts() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/add_layouts_same_index") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(
                /* CompareLevel = SAME */
                testScenarios);//
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        val indexPlan = indexPlanManager.listAllIndexPlans().get(0);
        Assert.assertEquals(1, indexPlan.getAllIndexes().size());
        Assert.assertEquals(2, indexPlan.getAllIndexes().get(0).getLayouts().size());
    }

    @Test
    @Ignore("see #17095")
    public void testOptimize_lookup() throws Exception {
        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Assert.assertEquals(0, optManager.listAllOptimizeRecommendations().size());
        val testScenarios = new TestScenario[] {
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/look_up_1"),
                new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/look_up_2") };
        prepareModels(getProject(), testScenarios[0]);
        executeTestScenario(testScenarios[0]);
        executeTestScenario(testScenarios[1]);
        Assert.assertEquals(1, optManager.listAllOptimizeRecommendations().size());
        val indexPlan = indexPlanManager.listAllIndexPlans().get(0);
        Assert.assertEquals(2, indexPlan.getAllIndexes().size());
        Assert.assertEquals(1, indexPlan.getAllIndexes().get(0).getLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllIndexes().get(1).getLayouts().size());
        indexPlanManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> copyForWrite.setIndexes(
                copyForWrite.getIndexes().stream().filter(IndexEntity::isTableIndex).collect(Collectors.toList())));
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        try {
            NExecAndComp.queryFromCube(getProject(),
                    KylinTestBase.changeJoinType(testScenarios[1].queries.get(0).getSecond(), "DEFAULT"));
            Assert.fail("query not failed");
        } catch (RuntimeException e) {
            Assert.assertTrue((e.getCause().getCause() instanceof NoRealizationFoundException));
        }
        NExecAndComp.queryFromCube(getProject(),
                KylinTestBase.changeJoinType(testScenarios[0].queries.get(0).getSecond(), "DEFAULT"));
        NExecAndComp.queryFromCube(getProject(),
                KylinTestBase.changeJoinType("select ACCOUNT_ID from TEST_ACCOUNT", "DEFAULT"));

    }
}
