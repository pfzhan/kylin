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
package io.kyligence.kap.metadata.recommendation;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.var;

public class OptimizeRecommendationManagerTest extends NLocalFileMetadataTestCase {
    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;
    private OptimizeRecommendationManager recommendationManager;
    private String projectDefault = "default";
    private String modelTest = "model_test";
    private String ownerTest = "owner_test";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), projectDefault);
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        dataflowManager = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        dataflowManager.listAllDataflows().forEach(dataflow -> dataflowManager.dropDataflow(dataflow.getId()));
        indexPlanManager.listAllIndexPlans().forEach(indexPlan -> indexPlanManager.dropIndexPlan(indexPlan));
        modelManager.listAllModels().forEach(model -> modelManager.dropModel(model));
        recommendationManager = OptimizeRecommendationManager.getInstance(getTestConfig(), projectDefault);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    private final String modelDir = "src/test/resources/ut_meta/optimize/metadata/model/";
    private final String indexDir = "src/test/resources/ut_meta/optimize/metadata/index_plan/";
    private final String id = "25f8bbb7-cddc-4837-873d-f80f994d8a2d";
    private final String baseModelFile = modelDir + "base_model.json";
    private final String baseIndexFile = indexDir + "base_index_plan.json";
    private final String optimizedModelFile = modelDir + "optimized_model.json";
    private final String optimizedIndexPlanFile = indexDir + "optimized_index_plan.json";
    private final String optimizedIndexPlanTwiceFile = indexDir + "optimized_twice_index_plan.json";
    private final String selfSameCCNameExprModelFile = modelDir + "self_same_name_and_same_expr_model.json";
    private final String selfSameCCNameExprIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String selfSameCCExprModelFile = modelDir + "self_same_expr_model.json";
    private final String selfSameCCExprIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String selfSameCCNameModelFile = modelDir + "self_same_name_model.json";
    private final String selfSameCCNameIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String otherSameCCNameExprModelFile = modelDir + "other_same_name_and_same_expr_model.json";
    private final String otherSameCCNameExprIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String otherSameCCExprModelFile = modelDir + "other_same_expr_model.json";
    private final String otherSameCCExprIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String otherSameCCNameModelFile = modelDir + "other_same_name_model.json";
    private final String otherSameCCNameIndexPlanFile = indexDir + "self_same_name_and_same_expr_index_plan.json";
    private final String selfSameMeasureExprModelFile = modelDir + "measure_same_expr_model.json";
    private final String selfSameMeasureExprIndexPlanFile = indexDir + "measure_same_expr_index_plan.json";
    private final String selfSameMeasureNameModelFile = modelDir + "measure_same_name_model.json";
    private final String selfSameMeasureNameIndexPlanFile = indexDir + "measure_same_expr_index_plan.json";
    private final String removeCCBaseModelFile = modelDir + "remove_cc_base_model.json";
    private final String removeCCOptimizedModelFile = modelDir + "remove_cc_optimized_model.json";
    private final String removeCCOptimizedIndexPlanFile = indexDir + "remove_cc_optimized_index_plan.json";
    private final String duplicateColumnBaseModel = modelDir + "duplicate_column_base_model.json";
    private final String duplicateColumnOptimizedModel = modelDir + "duplicate_column_optimized_model.json";

    private void prepare(String baseModelFile, String baseIndexFile, String optimizedModelFile,
            String optimizedIndexPlanFile) throws IOException {
        val originModel = JsonUtil.readValue(new File(baseModelFile), NDataModel.class);
        modelManager.createDataModelDesc(originModel, ownerTest);
        val originIndexPlan = JsonUtil.readValue(new File(baseIndexFile), IndexPlan.class);
        originIndexPlan.setUuid(id);
        originIndexPlan.setProject(projectDefault);
        indexPlanManager.createIndexPlan(originIndexPlan);
        val optimized = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(optimized, optimizedModelFile);
        val indexPlanOptimized = indexPlanManager.getIndexPlan(id).copy();
        updateIndexPlanByFile(indexPlanOptimized, optimizedIndexPlanFile);
        recommendationManager.optimize(optimized, indexPlanOptimized);
    }

    private void prepare(String optimizedModelFile, String optimizedIndexPlanFile) throws IOException {
        prepare(baseModelFile, baseIndexFile, optimizedModelFile, optimizedIndexPlanFile);
    }

    private void prepare() throws IOException {
        prepare(optimizedModelFile, optimizedIndexPlanFile);
    }

    private void updateModelByFile(NDataModel model, String fileName) throws IOException {
        val optimizedFromFile = JsonUtil.readValue(new File(fileName), NDataModel.class);
        model.setAllNamedColumns(optimizedFromFile.getAllNamedColumns());
        model.setComputedColumnDescs(optimizedFromFile.getComputedColumnDescs());
        model.setAllMeasures(optimizedFromFile.getAllMeasures());
    }

    private void updateIndexPlanByFile(IndexPlan indexPlan, String fileName) throws IOException {
        val optimizedFromFile = JsonUtil.readValue(new File(fileName), IndexPlan.class);
        indexPlan.setIndexes(optimizedFromFile.getIndexes());
    }

    @Test
    public void testOptimize() throws IOException {
        prepare();
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertNotNull(recommendation);
        Assert.assertEquals(2, recommendation.getCcRecommendations().size());
        Assert.assertEquals(3, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals("bigint", recommendation.getDimensionRecommendations().get(0).getDataType());
        Assert.assertEquals(6, recommendation.getMeasureRecommendations().size());
        Assert.assertEquals(3, recommendation.getIndexRecommendations().size());
        Assert.assertTrue(recommendation.getIndexRecommendations().stream().filter(item -> !item.isAggIndex())
                .allMatch(item -> item.getEntity().getLayouts().size() == 1));

    }

    @Test
    public void testDrop() throws IOException {
        testOptimize();
        recommendationManager.dropOptimizeRecommendation(id);
        Assert.assertNull(recommendationManager.getOptimizeRecommendation(id));

    }

    @Test
    public void testOptimizeTwice() throws IOException {
        testOptimize();
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val indexPlanOptimized = indexPlanManager.getIndexPlan(id).copy();
        updateIndexPlanByFile(indexPlanOptimized, optimizedIndexPlanTwiceFile);
        val appliedModel = recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                recommendation);
        recommendationManager.optimize(appliedModel, indexPlanOptimized);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(4, recommendation.getIndexRecommendations().size());
    }

    @Test
    public void testClearAll() throws IOException {
        testOptimize();
        recommendationManager.cleanAll(id);
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(0, recommendation.getCcRecommendations().size());
        Assert.assertEquals(0, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(0, recommendation.getMeasureRecommendations().size());
        Assert.assertEquals(0, recommendation.getIndexRecommendations().size());
    }

    @Test
    public void testRemoveIndex() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(5, recommendation.getIndexRecommendations().size());
        Assert.assertEquals(2, recommendation.getIndexRecommendations().stream().filter(item -> !item.isAdd()).count());
    }

    private void testRemoveExists(NDataModelManager.NDataModelUpdater updater) {
        modelManager.updateDataModel(id, updater);
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(3, recommendation.getIndexRecommendations().size());
        recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                indexPlanManager.copy(indexPlanManager.getIndexPlan(id)), recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(2, recommendation.getIndexRecommendations().size());
        Assert.assertTrue(
                recommendation.getIndexRecommendations().stream().noneMatch(IndexRecommendationItem::isAggIndex));
    }

    @Test
    public void testApply_RemoveDimension() throws IOException {
        prepare();
        testRemoveExists(
                copyForWrite -> copyForWrite.getAllNamedColumns().get(0).setStatus(NDataModel.ColumnStatus.EXIST));
    }

    @Test
    public void testApply_RemoveCC() throws IOException {
        prepare(removeCCBaseModelFile, baseIndexFile, removeCCOptimizedModelFile, removeCCOptimizedIndexPlanFile);
        testRemoveExists(copyForWrite -> {
            copyForWrite.setComputedColumnDescs(Lists.newArrayList());
            copyForWrite.getAllNamedColumns().get(16).setStatus(NDataModel.ColumnStatus.TOMB);
        });
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(0, recommendation.getCcRecommendations().size());
        Assert.assertEquals(1, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(3, recommendation.getMeasureRecommendations().size());
    }

    @Test
    public void testApply_RemoveMeasure() throws IOException {
        prepare();
        testRemoveExists(copyForWrite -> copyForWrite.getAllMeasures().stream()
                .filter(measure -> measure.getId() > 100000).forEach(measure -> measure.setTomb(true)));
    }

    @Test
    public void testApply() throws IOException {
        prepare();
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val appliedModel = recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                recommendation);
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(1).getStatus());
        Assert.assertEquals(10000001, appliedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", appliedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(10000002, appliedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", appliedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(17).getStatus());
        val indexPlan = indexPlanManager.getIndexPlan(id).copy();
        val indexId = indexPlan.getNextAggregationIndexId();
        val appliedIndexPlan = recommendationManager.apply(modelManager.copyForWrite(modelManager.getDataModelDesc(id)),
                indexPlanManager.getIndexPlan(id).copy(), recommendation);
        Assert.assertTrue(
                appliedIndexPlan.getAllIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == indexId));

    }

    @Test
    public void testApplyRemove() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L, 20000000001L, 20000000002L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        val indexPlanOrigin = indexPlanManager.copy(indexPlanManager.getIndexPlan(id));
        Assert.assertEquals(13, indexPlanOrigin.getAllIndexes().size());
        Assert.assertEquals(2, indexPlanOrigin.getIndexes().size());
        val applyRemovedIndexPlan = recommendationManager.applyRemove(indexPlanOrigin,
                recommendationManager.getOptimizeRecommendation(id));
        Assert.assertEquals(11, applyRemovedIndexPlan.getAllIndexes().size());
        Assert.assertEquals(1, applyRemovedIndexPlan.getIndexes().size());
        Assert.assertTrue(applyRemovedIndexPlan.getRuleBasedIndex().getLayoutBlackList().contains(1L));
    }

    @Test
    public void testApply_ExistsDimensionRemoved() throws IOException {
        prepare();
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        originInit.getAllNamedColumns().get(1).setStatus(NDataModel.ColumnStatus.DIMENSION);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val appliedModel = recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertTrue(
                recommendation.getDimensionRecommendations().stream().noneMatch(item -> item.getItemId() == 0));
        Assert.assertEquals(10000001, appliedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", appliedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(10000002, appliedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", appliedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(17).getStatus());
        val indexPlan = indexPlanManager.getIndexPlan(id).copy();
        val indexId = indexPlan.getNextAggregationIndexId();
        val appliedIndexPlan = recommendationManager.apply(appliedModel, indexPlanManager.getIndexPlan(id).copy(),
                recommendation);
        Assert.assertTrue(
                appliedIndexPlan.getAllIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == indexId));
    }

    @Test
    public void testApply_CCSelfConflictSameNameSameExpr() throws IOException {
        prepare(optimizedModelFile, selfSameCCNameExprIndexPlanFile);
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameCCNameExprModelFile);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(16, recommendation.getDimensionRecommendations().get(1).getColumn().getId());
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getDimensions().contains(16));
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(16)));
    }

    @Test
    public void testApply_CCSelfConflictSameExpr() throws IOException {
        prepare(optimizedModelFile, selfSameCCExprIndexPlanFile);
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameCCExprModelFile);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(1, recommendation.getCcRecommendations().size());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_X1 * 2",
                recommendation.getCcRecommendations().get(0).getCc().getExpression());
        Assert.assertEquals(16, recommendation.getDimensionRecommendations().get(1).getColumn().getId());
        Assert.assertEquals("CC_AUTO_1", recommendation.getDimensionRecommendations().get(1).getColumn().getName());
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getDimensions().contains(16));
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(16)));
    }

    @Test
    public void testApply_CCSelfConflictSameName() throws IOException {
        prepare(optimizedModelFile, selfSameCCNameIndexPlanFile);
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameCCNameModelFile);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(2, recommendation.getCcRecommendations().size());
        Assert.assertEquals("CC_AUTO_3", recommendation.getCcRecommendations().get(0).getCc().getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_3 * 2",
                recommendation.getCcRecommendations().get(1).getCc().getExpression());
        Assert.assertEquals(10000001, recommendation.getDimensionRecommendations().get(1).getColumn().getId());
        Assert.assertEquals("CC_AUTO_1", recommendation.getDimensionRecommendations().get(1).getColumn().getName());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_3",
                recommendation.getDimensionRecommendations().get(1).getColumn().getAliasDotColumn());
        Assert.assertEquals(10000002, recommendation.getDimensionRecommendations().get(2).getColumn().getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2",
                recommendation.getDimensionRecommendations().get(2).getColumn().getName());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_2",
                recommendation.getDimensionRecommendations().get(2).getColumn().getAliasDotColumn());

    }

    @Test
    public void testApply_SameCCNameAndExpr() throws IOException {
        prepare();
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val otherModel = JsonUtil.readValue(new File(otherSameCCNameExprModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        val appliedModel = recommendationManager.apply(originInit, recommendation);
        Assert.assertEquals(10000001, appliedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", appliedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(10000002, appliedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", appliedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(17).getStatus());
    }

    @Test
    public void testApply_CCConflictSameExpr() throws IOException {
        prepare();
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val otherModel = JsonUtil.readValue(new File(otherSameCCExprModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        val appliedModel = recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(2, recommendation.getCcRecommendations().size());
        Assert.assertEquals("CC_OTHER_3", recommendation.getCcRecommendations().get(0).getCc().getColumnName());
        Assert.assertEquals("CC_OTHER_4", recommendation.getCcRecommendations().get(1).getCc().getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_OTHER_3 * 2",
                recommendation.getCcRecommendations().get(1).getCc().getExpression());
        Assert.assertEquals(10000001, appliedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", appliedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(10000002, appliedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", appliedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(17).getStatus());
    }

    @Test
    public void testApply_CCConflictSameName() throws IOException {
        prepare();
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        val otherModel = JsonUtil.readValue(new File(otherSameCCNameModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        val appliedModel = recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(2, recommendation.getCcRecommendations().size());
        Assert.assertEquals("CC_AUTO_3", recommendation.getCcRecommendations().get(0).getCc().getColumnName());
        Assert.assertEquals("CC_AUTO_4", recommendation.getCcRecommendations().get(1).getCc().getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_3 * 2",
                recommendation.getCcRecommendations().get(1).getCc().getExpression());
        Assert.assertEquals(10000001, appliedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", appliedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(10000002, appliedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", appliedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, appliedModel.getAllNamedColumns().get(17).getStatus());
    }

    @Test
    public void testApply_MeasureConflictSameExpr() throws IOException {
        prepare(optimizedModelFile, selfSameMeasureExprIndexPlanFile);
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameMeasureExprModelFile);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(5, recommendation.getMeasureRecommendations().size());
        Assert.assertTrue(recommendation.getMeasureRecommendations().stream().noneMatch(
                measure -> measure.getMeasure().getFunction().getExpression().equals("TOP_N") && measure.getMeasure()
                        .getFunction().getParameters().stream().anyMatch(p -> p.getValue().contains("CC_AUTO_1"))));
        Assert.assertEquals("TEST_KYLIN_FACT.CC_PRICE_ITEM", recommendation.getMeasureRecommendations().get(3)
                .getMeasure().getFunction().getParameters().get(0).getValue());
        Assert.assertEquals("TEST_KYLIN_FACT.CC_PRICE_ITEM * 2",
                recommendation.getCcRecommendations().get(0).getCc().getExpression());
        Assert.assertEquals(2, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(10000002, recommendation.getDimensionRecommendations().get(1).getColumn().getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2",
                recommendation.getDimensionRecommendations().get(1).getColumn().getName());
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getDimensions().contains(16));
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(16)));
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getMeasures().contains(100002));
        Assert.assertTrue(recommendation.getIndexRecommendations().get(0).getEntity().getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(100002)));
    }

    @Test
    public void testApply_MeasureConflictSameName() throws IOException {
        prepare(optimizedModelFile, selfSameMeasureNameIndexPlanFile);
        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameMeasureNameModelFile);
        modelManager.updateDataModelDesc(originInit);
        originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        var recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(6, recommendation.getMeasureRecommendations().size());
        Assert.assertEquals("COUNT_SELLER", recommendation.getMeasureRecommendations().get(2).getMeasure().getName());
        recommendationManager.apply(originInit, recommendation);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(6, recommendation.getMeasureRecommendations().size());
        Assert.assertEquals("COUNT_SELLER_1", recommendation.getMeasureRecommendations().get(2).getMeasure().getName());
    }

    @Test
    public void testVerifyAll() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        val verifier = new OptimizeRecommendationVerifier(getTestConfig(), projectDefault, id);
        verifier.verifyAll();

        val updatedModel = modelManager.getDataModelDesc(id);
        val updateIndexPlan = indexPlanManager.getIndexPlan(id);

        Assert.assertEquals(0, recommendationManager.getRecommendationCount(id));
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(1).getStatus());
        Assert.assertEquals(16, updatedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", updatedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(17, updatedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", updatedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(17).getStatus());
        Assert.assertEquals(3, updateIndexPlan.getIndexes().size());
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getDimensions().contains(16));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(16)));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getMeasures().contains(100002));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(100002)));
        Assert.assertEquals(13, updateIndexPlan.getAllIndexes().size());
        Assert.assertTrue(updateIndexPlan.getIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == 150000));
        Assert.assertEquals(1, updateIndexPlan.getIndexes().get(0).getLayouts().size());
        Assert.assertEquals(150002L, updateIndexPlan.getIndexes().get(0).getLayouts().get(0).getId());
        Assert.assertTrue(updateIndexPlan.getRuleBasedIndex().getLayoutBlackList().contains(1L));
    }

    @Test
    public void testVerify_passAll() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val passCCs = recommendation.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failCCs = Sets.<Long> newHashSet();
        val passDimensions = recommendation.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = recommendation.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failMeasures = Sets.<Long> newHashSet();

        val passIndexes = recommendation.getIndexRecommendations().stream().map(IndexRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failIndexes = Sets.<Long> newHashSet();

        var originIndexPlan = indexPlanManager.getIndexPlan(id).copy();

        Assert.assertEquals(2, originIndexPlan.getIndexes().size());
        Assert.assertTrue(originIndexPlan.getIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == 150000));
        long nextIndexId = originIndexPlan.getNextAggregationIndexId();

        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);

        verifier.verify();

        val updatedModel = modelManager.getDataModelDesc(id);
        val updateIndexPlan = indexPlanManager.getIndexPlan(id);
        val updateRecommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(1).getStatus());
        Assert.assertEquals(16, updatedModel.getAllNamedColumns().get(16).getId());
        Assert.assertEquals("CC_AUTO_1", updatedModel.getAllNamedColumns().get(16).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(16).getStatus());
        Assert.assertEquals(17, updatedModel.getAllNamedColumns().get(17).getId());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", updatedModel.getAllNamedColumns().get(17).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, updatedModel.getAllNamedColumns().get(17).getStatus());
        Assert.assertEquals(3, updateIndexPlan.getIndexes().size());
        Assert.assertEquals(nextIndexId, updateIndexPlan.getIndexes().get(2).getId());
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getDimensions().contains(16));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(16)));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getMeasures().contains(100002));
        Assert.assertTrue(updateIndexPlan.getIndexes().get(2).getLayouts().stream()
                .allMatch(layout -> layout.getColOrder().contains(100002)));
        Assert.assertTrue(updateIndexPlan.getIndexes().stream().filter(IndexEntity::isTableIndex)
                .allMatch(indexEntity -> indexEntity.getLayouts().size() == 4
                        && indexEntity.getLastLayout().getId() == 20000000004L));
        Assert.assertEquals(13, updateIndexPlan.getAllIndexes().size());
        Assert.assertTrue(updateIndexPlan.getIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == 150000));
        Assert.assertEquals(1, updateIndexPlan.getIndexes().get(0).getLayouts().size());
        Assert.assertEquals(150002L, updateIndexPlan.getIndexes().get(0).getLayouts().get(0).getId());
        Assert.assertTrue(updateIndexPlan.getRuleBasedIndex().getLayoutBlackList().contains(1L));
        Assert.assertEquals(0, updateRecommendation.getCcRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getDimensionRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getMeasureRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getIndexRecommendations().size());
    }

    private OptimizeRecommendationVerifier create(Set<Long> passCCs, Set<Long> failCCs, Set<Long> passDimensions,
            Set<Long> failDimensions, Set<Long> passMeasures, Set<Long> failMeasures, Set<Long> passIndexes,
            Set<Long> failIndexes) {

        val verifier = new OptimizeRecommendationVerifier(getTestConfig(), projectDefault, id);

        verifier.setPassCCItems(passCCs);
        verifier.setFailCCItems(failCCs);
        verifier.setPassDimensionItems(passDimensions);
        verifier.setFailDimensionItems(failDimensions);
        verifier.setPassMeasureItems(passMeasures);
        verifier.setFailMeasureItems(failMeasures);
        verifier.setPassIndexItems(passIndexes);
        verifier.setFailIndexItems(failIndexes);
        return verifier;
    }

    @Test
    public void testVerify_failAll() throws IOException {
        prepare();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val failCCs = recommendation.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val passCCs = Sets.<Long> newHashSet();
        val failDimensions = recommendation.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val passDimensions = Sets.<Long> newHashSet();
        val failMeasures = recommendation.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val passMeasures = Sets.<Long> newHashSet();

        val failIndexes = recommendation.getIndexRecommendations().stream().map(IndexRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val passIndexes = Sets.<Long> newHashSet();

        var originIndexPlan = indexPlanManager.getIndexPlan(id).copy();

        Assert.assertEquals(2, originIndexPlan.getIndexes().size());

        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);

        verifier.verify();

        val updatedModel = modelManager.getDataModelDesc(id);
        val updateIndexPlan = indexPlanManager.getIndexPlan(id);
        val updateRecommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(16, updatedModel.getAllNamedColumns().size());
        Assert.assertEquals(0, updatedModel.getComputedColumnDescs().size());
        Assert.assertEquals(2, updatedModel.getAllMeasures().size());
        Assert.assertEquals(2, updateIndexPlan.getIndexes().size());
        Assert.assertEquals(0, updateRecommendation.getCcRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getDimensionRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getMeasureRecommendations().size());
        Assert.assertEquals(0, updateRecommendation.getIndexRecommendations().size());
    }

    @Test
    public void testVerify_failAndDelete() throws IOException {
        prepare();
        var passCCs = Sets.<Long> newHashSet();
        var failCCs = Sets.<Long> newHashSet(0L);
        val failDimensions = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failIndexes = Sets.<Long> newHashSet();
        val passIndexes = Sets.<Long> newHashSet();

        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);

        verifier.verify();

        val updatedModel = modelManager.getDataModelDesc(id);
        val updateIndexPlan = indexPlanManager.getIndexPlan(id);
        val updateRecommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(16, updatedModel.getAllNamedColumns().size());
        Assert.assertEquals(0, updatedModel.getComputedColumnDescs().size());
        Assert.assertEquals(2, updatedModel.getAllMeasures().size());
        Assert.assertEquals(2, updateIndexPlan.getIndexes().size());
        Assert.assertEquals(0, updateRecommendation.getCcRecommendations().size());
        Assert.assertEquals(1, updateRecommendation.getDimensionRecommendations().size());
        Assert.assertTrue(updateRecommendation.getDimensionRecommendations().stream()
                .noneMatch(item -> item.getColumn().getAliasDotColumn().contains("CC_AUTO")));
        Assert.assertEquals(3, updateRecommendation.getMeasureRecommendations().size());
        Assert.assertTrue(updateRecommendation.getMeasureRecommendations().stream()
                .noneMatch(item -> item.getMeasure().getFunction().getParameters().stream().anyMatch(p -> {
                    if (p.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN)) {
                        return p.getValue().contains("CC_AUTO");
                    }
                    return false;
                })));
        Assert.assertEquals(2, updateRecommendation.getIndexRecommendations().size());
    }

    @Test
    public void testVerify_failPassConflict() throws IOException {
        prepare();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        var passCCs = recommendation.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        var failCCs = Sets.<Long> newHashSet(1L);
        passCCs = Sets.difference(passCCs, failCCs);
        val passDimensions = recommendation.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = recommendation.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failMeasures = Sets.<Long> newHashSet();
        val passIndexes = recommendation.getIndexRecommendations().stream().map(IndexRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failIndexes = Sets.<Long> newHashSet();

        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);

        thrown.expect(DependencyLostException.class);
        thrown.expectMessage(
                "dimension lost dependency: column TEST_KYLIN_FACT_CC_AUTO_2 not exists in all columns, you may need pass it first");
        verifier.verify();

    }

    @Test
    public void testVerify_passCCLostDependency() throws IOException {
        prepare();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        var passCCs = Sets.<Long> newHashSet(1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = recommendation.getDimensionRecommendations().stream()
                .map(DimensionRecommendationItem::getItemId).collect(Collectors.toSet());
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = recommendation.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet();
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage("cc lost dependency: cc TEST_KYLIN_FACT.CC_AUTO_1 not exists, you may need pass it first");
        verifier.verify();

    }

    @Test
    public void testVerify_passDimensionLostDependency() throws IOException {
        prepare();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        var passCCs = Sets.<Long> newHashSet();
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(1L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = recommendation.getMeasureRecommendations().stream().map(MeasureRecommendationItem::getItemId)
                .collect(Collectors.toSet());
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet();
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage(
                "dimension lost dependency: column CC_AUTO_1 not exists in all columns, you may need pass it first");
        verifier.verify();

    }

    @Test
    public void testVerify_passMeasureLostDependency() throws IOException {
        prepare();
        var passCCs = Sets.<Long> newHashSet();
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet();
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet(3L);
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet();
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage(
                "measure lost dependency: column TEST_KYLIN_FACT.CC_AUTO_1 not exists in all columns, you may need pass it first");
        verifier.verify();

    }

    @Test
    public void testVerify_passIndexLostDimensionDependency() throws IOException {
        prepare();
        var passCCs = Sets.<Long> newHashSet();
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet();
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage("index lost dependency: dimension not exists, you may need pass it first");
        verifier.verify();

    }

    @Test
    public void testVerify_passCCAndIndexLostDimensionDependency() throws IOException {
        prepare();
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet();
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage("agg index lost dependency: dimension not exists, you may need pass it first");
        verifier.verify();
    }

    @Test
    public void testVerify_passCCAndIndexLostMeasureDependency() throws IOException {
        prepare();
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(0L, 1L, 2L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(DependencyLostException.class);
        thrown.expectMessage("index lost dependency: measure not exists, you may need pass it first");
        verifier.verify();
    }

    @Test
    public void testVerify_renameCCOtherConflictSameName() throws IOException {
        prepare();
        val otherModel = JsonUtil.readValue(new File(otherSameCCNameModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        recommendationManager.updateOptimizeRecommendation(id, recommendation -> recommendation.getCcRecommendations()
                .forEach(ccRecommendationItem -> ccRecommendationItem.setAutoChangeName(false)));
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(0L, 1L, 2L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(PassConflictException.class);
        thrown.expectMessage("pass cc CC_AUTO_1 name conflict");
        verifier.verify();
    }

    @Test
    public void testVerify_renameCCOtherConflictSameExpr() throws IOException {
        prepare();
        val otherModel = JsonUtil.readValue(new File(otherSameCCExprModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        recommendationManager.updateOptimizeRecommendation(id, recommendation -> recommendation.getCcRecommendations()
                .forEach(ccRecommendationItem -> ccRecommendationItem.setAutoChangeName(false)));
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(0L, 1L, 2L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(PassConflictException.class);
        thrown.expectMessage(
                "pass cc CC_AUTO_1 TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT expression conflict");
        verifier.verify();
    }

    @Test
    public void testVerify_renameCCSelfConflictSameName() throws IOException {
        prepare();

        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameCCNameModelFile);
        modelManager.updateDataModelDesc(originInit);

        recommendationManager.updateOptimizeRecommendation(id, recommendation -> recommendation.getCcRecommendations()
                .forEach(ccRecommendationItem -> ccRecommendationItem.setAutoChangeName(false)));
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(0L, 1L, 2L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(PassConflictException.class);
        thrown.expectMessage("cc CC_AUTO_1 name has already used in model");
        verifier.verify();
    }

    @Test
    public void testVerify_renameCCSelfConflictSameExpr() throws IOException {
        prepare();

        var originInit = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(originInit, selfSameCCExprModelFile);
        modelManager.updateDataModelDesc(originInit);

        recommendationManager.updateOptimizeRecommendation(id, recommendation -> recommendation.getCcRecommendations()
                .forEach(ccRecommendationItem -> ccRecommendationItem.setAutoChangeName(false)));
        var passCCs = Sets.<Long> newHashSet(0L, 1L);
        var failCCs = Sets.<Long> newHashSet();
        val passDimensions = Sets.<Long> newHashSet(0L, 1L, 2L);
        val failDimensions = Sets.<Long> newHashSet();
        val passMeasures = Sets.<Long> newHashSet();
        val failMeasures = Sets.<Long> newHashSet();
        var passIndexes = Sets.<Long> newHashSet(0L);
        var failIndexes = Sets.<Long> newHashSet();
        val verifier = create(passCCs, failCCs, passDimensions, failDimensions, passMeasures, failMeasures, passIndexes,
                failIndexes);
        thrown.expect(PassConflictException.class);
        thrown.expectMessage("cc CC_AUTO_1 expression has already defined in model");
        verifier.verify();
    }

    @Test
    public void testDuplicateColumn() throws IOException {
        prepare(duplicateColumnBaseModel, baseIndexFile, duplicateColumnOptimizedModel, optimizedIndexPlanFile);
        var recommendation = recommendationManager.getOptimizeRecommendation(id);

        var model = recommendationManager.applyModel(id);
        var indexPlan = recommendationManager.applyIndexPlan(id);

        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", model.getAllNamedColumns().get(2).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.EXIST, model.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals("TEST_KYLIN_FACT_CC_AUTO_2", model.getAllNamedColumns().get(3).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.EXIST, model.getAllNamedColumns().get(3).getStatus());

        model.getAllNamedColumns().get(2).setStatus(NDataModel.ColumnStatus.DIMENSION);
        recommendationManager.optimize(model, indexPlan);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals("TEST_KYLIN_FACT_LSTG_SITE_ID",
                recommendation.getDimensionRecommendations().get(3).getColumn().getName());
        model = recommendationManager.applyModel(id);
        Assert.assertEquals("TEST_KYLIN_FACT_LSTG_SITE_ID", model.getAllNamedColumns().get(2).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, model.getAllNamedColumns().get(2).getStatus());
        var passCCs = Sets.newHashSet(0L, 1L);
        var passDimensions = Sets.newHashSet(3L);

        var verifier = create(passCCs, null, passDimensions, null, null, null, null, null);
        verifier.verify();

        model = modelManager.getDataModelDesc(id);
        Assert.assertEquals("TEST_KYLIN_FACT_LSTG_SITE_ID", model.getAllNamedColumns().get(2).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, model.getAllNamedColumns().get(2).getStatus());

        model = recommendationManager.applyModel(id);
        indexPlan = recommendationManager.applyIndexPlan(id);
        model.getAllNamedColumns().get(3).setStatus(NDataModel.ColumnStatus.DIMENSION);
        recommendationManager.optimize(model, indexPlan);
        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(4L, recommendation.getDimensionRecommendations().get(3).getItemId());
        Assert.assertEquals("TEST_KYLIN_FACT_ITEM_COUNT",
                recommendation.getDimensionRecommendations().get(3).getColumn().getName());

        passDimensions = Sets.newHashSet(4L);
        verifier = create(null, null, passDimensions, null, null, null, null, null);
        verifier.verify();
        model = modelManager.getDataModelDesc(id);
        Assert.assertEquals("TEST_KYLIN_FACT_ITEM_COUNT", model.getAllNamedColumns().get(3).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, model.getAllNamedColumns().get(3).getStatus());

    }

}
