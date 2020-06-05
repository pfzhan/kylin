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

package io.kyligence.kap.metadata.recommendation.v2;

import static org.apache.kylin.common.util.JsonUtil.readValue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.cube.model.SelectRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecommendationManager;
import lombok.val;
import lombok.var;

public class OptimizeRecommendationManagerV2Test extends NLocalFileMetadataTestCase {

    protected NDataModelManager modelManager;
    protected NIndexPlanManager indexPlanManager;
    protected NDataflowManager dataflowManager;
    protected OptimizeRecommendationManager recommendationManager;
    protected OptimizeRecommendationManagerV2 recommendationManagerV2;
    protected RawRecommendationManager rawRecommendationManager = Mockito.mock(RawRecommendationManager.class);
    protected String projectDefault = "default";
    protected String modelTest = "model_test";
    protected String ownerTest = "owner_test";

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
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.put(RawRecommendationManager.class, new ConcurrentHashMap<>());
        managersByPrjCache.get(RawRecommendationManager.class).put(projectDefault, rawRecommendationManager);
        Mockito.doAnswer(answer -> mockRawRecItems.get(answer.getArgument(0))).when(rawRecommendationManager)
                .queryById(Mockito.anyInt());
        recommendationManagerV2 = OptimizeRecommendationManagerV2.getInstance(getTestConfig(), projectDefault);

        prepare();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    protected String modelDir = "src/test/resources/ut_meta/optimize/metadataV2/model/";
    protected String indexDir = "src/test/resources/ut_meta/optimize/metadataV2/index_plan/";
    protected final String id = "25f8bbb7-cddc-4837-873d-f80f994d8a2d";
    protected String baseModelFile = modelDir + "base_model.json";
    protected String baseIndexFile = indexDir + "base_index_plan.json";
    protected String optimizedModelFile = modelDir + "optimized_model.json";
    protected String optimizedIndexPlanFile = indexDir + "optimized_index_plan.json";

    protected Map<Integer, RawRecItem> mockRawRecItems;

    private void prepare(String baseModelFile, String baseIndexFile, String optimizedModelFile,
            String optimizedIndexPlanFile, Consumer<NDataModel> modelConsumer,
            NIndexPlanManager.NIndexPlanUpdater indexPlanUpdater) throws IOException {
        val originModel = readValue(new File(baseModelFile), NDataModel.class);
        if (modelConsumer != null) {
            modelConsumer.accept(originModel);
        }
        modelManager.createDataModelDesc(originModel, ownerTest);
        val originIndexPlan = readValue(new File(baseIndexFile), IndexPlan.class);
        originIndexPlan.setUuid(id);
        originIndexPlan.setProject(projectDefault);

        indexPlanManager.createIndexPlan(originIndexPlan);
        if (indexPlanUpdater != null) {
            indexPlanManager.updateIndexPlan(id, indexPlanUpdater);
        }

        NDataflow df = dataflowManager.createDataflow(originIndexPlan, ownerTest);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        dataflowManager.updateDataflow(update);

        val optimized = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(optimized, optimizedModelFile);
        val indexPlanOptimized = indexPlanManager.getIndexPlan(id).copy();
        updateIndexPlanByFile(indexPlanOptimized, optimizedIndexPlanFile);
        recommendationManager.optimize(optimized, indexPlanOptimized);

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        mockRawRecItems = recommendationManagerV2.convertFromV1(recommendation).stream()
                .collect(Collectors.toMap(RawRecItem::getId, Function.identity()));
        recommendationManager.cleanAll(id);

        OptimizeRecommendationV2 recommendationV2 = new OptimizeRecommendationV2();
        recommendationV2.setUuid(id);
        recommendationV2
                .setRawIds(mockRawRecItems.values().stream().filter(v -> v.getType() == RawRecItem.RawRecType.LAYOUT)
                        .map(RawRecItem::getId).sorted().collect(Collectors.toList()));
        recommendationManagerV2.updateOptimizeRecommendationV2(recommendationV2);

    }

    private void prepare(String baseModelFile, String baseIndexFile, String optimizedModelFile,
            String optimizedIndexPlanFile) throws IOException {
        prepare(baseModelFile, baseIndexFile, optimizedModelFile, optimizedIndexPlanFile, null, null);
    }

    private void prepare(String optimizedModelFile, String optimizedIndexPlanFile) throws IOException {
        prepare(baseModelFile, baseIndexFile, optimizedModelFile, optimizedIndexPlanFile);
    }

    private void prepare() throws IOException {
        prepare(optimizedModelFile, optimizedIndexPlanFile);
        assertListSize(1, 2, 2, 12);
    }

    private void updateModelByFile(NDataModel model, String fileName) throws IOException {
        val optimizedFromFile = readValue(new File(fileName), NDataModel.class);
        model.setAllNamedColumns(optimizedFromFile.getAllNamedColumns());
        model.setComputedColumnDescs(optimizedFromFile.getComputedColumnDescs());
        model.setAllMeasures(optimizedFromFile.getAllMeasures());
    }

    private void updateIndexPlanByFile(IndexPlan indexPlan, String fileName) throws IOException {
        val optimizedFromFile = readValue(new File(fileName), IndexPlan.class);
        indexPlan.setIndexes(optimizedFromFile.getIndexes());
    }

    @Test
    public void testInit() throws IOException {
        Assert.assertNotNull(mockRawRecItems);
        OptimizeRecommendationV2 recommendationV2 = recommendationManagerV2.getOptimizeRecommendationV2(id);
        Assert.assertNotNull(recommendationV2);
        Assert.assertEquals(18, recommendationV2.getColumnRefs().size());
        Assert.assertEquals("decimal(30,4)",
                RecommendationUtil.getDimensionDataType(recommendationV2.getEffectiveDimensionRawRecItems().get(3)));
        Assert.assertTrue(recommendationV2.getEffectiveLayoutRawRecItems().values().stream()
                .filter(item -> !RecommendationUtil.isAgg(item))
                .allMatch(item -> RecommendationUtil.getLayout(item) != null));
        Assert.assertTrue(recommendationV2.getEffectiveCCRawRecItems().values().stream()
                .allMatch(item -> item.getCreateTime() > 0));
        Assert.assertTrue(recommendationV2.getEffectiveDimensionRawRecItems().values().stream()
                .allMatch(item -> item.getCreateTime() > 0));
        Assert.assertTrue(recommendationV2.getEffectiveMeasureRawRecItems().values().stream()
                .allMatch(item -> item.getCreateTime() > 0));
        Assert.assertTrue(recommendationV2.getEffectiveMeasureRawRecItems().values().stream()
                .allMatch(item -> item.getCreateTime() > 0));
    }

    @Test
    public void testInit_RemoveCC() throws IOException {
        modelManager.updateDataModel(id, copyForWrite -> {
            copyForWrite.setComputedColumnDescs(Lists.newArrayList());
            copyForWrite.getAllNamedColumns().get(16).setStatus(NDataModel.ColumnStatus.TOMB);
        });

        var recommendation = recommendationManagerV2.getOptimizeRecommendationV2(id);
        assertListSize(1, 1, 2, 10);
        Assert.assertTrue(recommendation.getEffectiveLayoutRawRecItems().values().stream()
                .noneMatch(rawRecItem -> Arrays.stream(rawRecItem.getDependIDs()).anyMatch(id -> id == 16)));
    }

    @Test
    public void testInit_ExistCC() throws IOException {
        val rawRecItem = recommendationManagerV2.getOptimizeRecommendationV2(id).getEffectiveCCRawRecItems().get(1);
        modelManager.updateDataModel(id, copyForWrite -> {
            val cc = RecommendationUtil.getCC(rawRecItem);
            cc.setColumnName("CC_AUTO_C3");
            copyForWrite.getComputedColumnDescs().add(cc);
            val column = new NDataModel.NamedColumn();
            column.setId(
                    copyForWrite.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().orElse(-1)
                            + 1);
            column.setName(cc.getFullName().replace(".", "_"));
            column.setAliasDotColumn(cc.getFullName());
            copyForWrite.getAllNamedColumns().add(column);
        });
        assertListSize(0, 2, 2, 12);
    }

    @Test
    public void testInit_RemoveDimension() throws IOException {
        modelManager.updateDataModel(id,
                copyForWrite -> copyForWrite.getAllNamedColumns().get(12).setStatus(NDataModel.ColumnStatus.EXIST));
        assertListSize(1, 2, 2, 10);
        var recommendation = recommendationManagerV2.getOptimizeRecommendationV2(id);
        Assert.assertTrue(recommendation.getEffectiveLayoutRawRecItems().values().stream()
                .noneMatch(rawRecItem -> Arrays.stream(rawRecItem.getDependIDs()).anyMatch(id -> id == 12)));
    }

    @Test
    public void testInit_ExistDimension() throws IOException {
        modelManager.updateDataModel(id,
                copyForWrite -> copyForWrite.getAllNamedColumns().get(16).setStatus(NDataModel.ColumnStatus.DIMENSION));
        assertListSize(1, 1, 2, 12);
    }

    @Test
    public void testInit_RemoveMeasure() throws IOException {
        modelManager.updateDataModel(id, copyForWrite -> copyForWrite.getAllMeasures().get(2).setTomb(true));
        assertListSize(1, 2, 2, 11);
        Assert.assertTrue(recommendationManagerV2.getOptimizeRecommendationV2(id).getEffectiveLayoutRawRecItems()
                .values().stream().noneMatch(rawRecItem -> RecommendationUtil.getLayout(rawRecItem).getColOrder()
                        .stream().anyMatch(i -> i == 100002)));
    }

    @Test
    public void testInit_ExistMeasure() throws IOException {
        RawRecItem rawRecItem = recommendationManagerV2.getOptimizeRecommendationV2(id).getEffectiveMeasureRawRecItems()
                .get(5);
        Assert.assertNotNull(rawRecItem);
        modelManager.updateDataModel(id, copyForWrite -> {
            NDataModel.Measure measure = RecommendationUtil.getMeasure(rawRecItem);
            measure.setId(
                    copyForWrite.getAllMeasures().stream().mapToInt(NDataModel.Measure::getId).max().orElse(100001)
                            + 1);
            copyForWrite.getAllMeasures().add(measure);
        });
        assertListSize(1, 2, 1, 12);
    }

    protected void assertListSize(int ccItemsSize, int dimItemSize, int measureItemSize, int layoutItemSize) {
        val recommendation = recommendationManagerV2.getOptimizeRecommendationV2(id);
        Assert.assertEquals(ccItemsSize, recommendation.getEffectiveCCRawRecItems().size());
        Assert.assertEquals(dimItemSize, recommendation.getEffectiveDimensionRawRecItems().size());
        Assert.assertEquals(measureItemSize, recommendation.getEffectiveMeasureRawRecItems().size());
        Assert.assertEquals(layoutItemSize, recommendation.getEffectiveLayoutRawRecItems().size());
    }

    @Test
    public void testInit_ExistAggLayout() {
        indexPlanManager.updateIndexPlan(id, copyForWrite -> {
            IndexEntity indexEntity = new IndexEntity();
            indexEntity.setId(copyForWrite.getNextAggregationIndexId());
            indexEntity.setNextLayoutOffset(2);
            LayoutEntity layoutEntity = new LayoutEntity();
            layoutEntity.setAuto(true);
            layoutEntity.setId(indexEntity.getId() + 1);
            layoutEntity.setColOrder(Lists.newArrayList(0, 4, 5, 100000, 100002));
            indexEntity.setLayouts(Lists.newArrayList(layoutEntity));
            indexEntity.setDimensions(Lists.newArrayList(0, 4, 5));
            indexEntity.setMeasures(Lists.newArrayList(100000, 100002));
            val indexes = copyForWrite.getIndexes();
            indexes.add(indexEntity);
            copyForWrite.setIndexes(indexes);
        });
        assertListSize(1, 2, 2, 11);
    }

    @Test
    public void testInit_ExistTableLayout() {
        indexPlanManager.updateIndexPlan(id, copyForWrite -> {
            IndexEntity indexEntity = new IndexEntity();
            indexEntity.setId(copyForWrite.getNextTableIndexId());
            indexEntity.setNextLayoutOffset(2);
            LayoutEntity layoutEntity = new LayoutEntity();
            layoutEntity.setAuto(true);
            layoutEntity.setId(indexEntity.getId() + 1);
            layoutEntity.setColOrder(Lists.newArrayList(0, 1, 2, 3, 4));
            indexEntity.setLayouts(Lists.newArrayList(layoutEntity));
            indexEntity.setDimensions(Lists.newArrayList(0, 1, 2, 3, 4));
            val indexes = copyForWrite.getIndexes();
            indexes.add(indexEntity);
            copyForWrite.setIndexes(indexes);
        });
        assertListSize(1, 2, 2, 11);
    }

    @Test
    public void testInit_ExistAggGroupLayout() throws IOException {
        indexPlanManager.updateIndexPlan(id, copyForWrite -> {
            val ruleBasedIndex = copyForWrite.getRuleBasedIndex();
            val updatedAgg = new NRuleBasedIndex();
            updatedAgg.setAggregationGroups(ruleBasedIndex.getAggregationGroups());
            updatedAgg.setGlobalDimCap(ruleBasedIndex.getGlobalDimCap());
            updatedAgg.setDimensions(ruleBasedIndex.getDimensions());
            updatedAgg.setLastModifiedTime(System.currentTimeMillis());
            val dimensions = new HashSet<Integer>(updatedAgg.getDimensions());
            dimensions.addAll(Lists.newArrayList(0, 4, 5));
            val measures = new HashSet<Integer>(updatedAgg.getMeasures());
            measures.addAll(Lists.newArrayList(100000, 100002));
            val group = new NAggregationGroup();
            group.setIncludes(new Integer[] { 0, 4, 5 });
            group.setMeasures(new Integer[] { 100000, 100002 });
            val rule = new SelectRule();
            group.setSelectRule(rule);
            rule.setMandatoryDims(new Integer[] { 0, 4, 5 });
            rule.setHierarchyDims(new Integer[][] {});
            rule.setJointDims(new Integer[][] {});
            updatedAgg.getAggregationGroups().add(group);
            updatedAgg.setDimensions(dimensions.stream().sorted().collect(Collectors.toList()));
            updatedAgg.setMeasures(measures.stream().sorted().collect(Collectors.toList()));
            copyForWrite.setRuleBasedIndex(updatedAgg);
        });
        assertListSize(1, 2, 2, 11);
    }
}
