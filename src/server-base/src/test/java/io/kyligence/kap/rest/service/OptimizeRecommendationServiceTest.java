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

package io.kyligence.kap.rest.service;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.garbage.FrequencyMap;
import io.kyligence.kap.metadata.cube.garbage.GarbageLayoutType;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationVerifier;
import io.kyligence.kap.metadata.recommendation.PassConflictException;
import io.kyligence.kap.metadata.recommendation.RecommendationItem;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
import io.kyligence.kap.rest.response.LayoutRecommendationResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import lombok.val;
import lombok.var;

public class OptimizeRecommendationServiceTest extends NLocalFileMetadataTestCase {
    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;
    private OptimizeRecommendationManager recommendationManager;
    private String projectDefault = "default";
    private String ownerTest = "owner_test";

    private final String modelDir = "../core-metadata/src/test/resources/ut_meta/optimize/metadata/model/";
    private final String indexDir = "../core-metadata/src/test/resources/ut_meta/optimize/metadata/index_plan/";
    private final String id = "25f8bbb7-cddc-4837-873d-f80f994d8a2d";
    private final String baseModelFile = modelDir + "base_model.json";
    private final String baseIndexFile = indexDir + "base_index_plan.json";
    private final String optimizedModelFile = modelDir + "optimized_model.json";
    private final String optimizedIndexPlanFile = indexDir + "optimized_index_plan.json";
    private final String optimizedIndexPlanTwiceFile = indexDir + "optimized_twice_index_plan.json";
    private final String optimizedModelTwiceFile = modelDir + "optimized_model_twice.json";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private OptimizeRecommendationService service = Mockito.spy(new OptimizeRecommendationService());

    @Before
    public void setup() {
        createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), projectDefault);
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        dataflowManager = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        recommendationManager = OptimizeRecommendationManager.getInstance(getTestConfig(), projectDefault);
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    private void prepare() throws IOException {
        dataflowManager.listAllDataflows().forEach(dataflow -> dataflowManager.dropDataflow(dataflow.getId()));
        indexPlanManager.listAllIndexPlans().forEach(indexPlan -> indexPlanManager.dropIndexPlan(indexPlan));
        modelManager.listAllModels().forEach(model -> modelManager.dropModel(model));

        val originModel = JsonUtil.readValue(new File(baseModelFile), NDataModel.class);
        modelManager.createDataModelDesc(originModel, ownerTest);
        val originIndexPlan = JsonUtil.readValue(new File(baseIndexFile), IndexPlan.class);
        originIndexPlan.setUuid(id);
        originIndexPlan.setProject(projectDefault);
        indexPlanManager.createIndexPlan(originIndexPlan);

        dataflowManager.createDataflow(indexPlanManager.getIndexPlan(id).copy(), "ADMIN");
        dataflowManager.updateDataflow(id, copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.ONLINE));

        val optimized = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(optimized, optimizedModelFile);
        val indexPlanOptimized = indexPlanManager.getIndexPlan(id).copy();
        updateIndexPlanByFile(indexPlanOptimized, optimizedIndexPlanFile);
        recommendationManager.optimize(optimized, indexPlanOptimized);
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

    private void prepareDataflow() {
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(zoneId).toLocalDate();
        long currentDate = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();

        dataflowManager.updateDataflow(id, copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(TimeUtil.minusDays(currentDate, 7), 1);
                            put(TimeUtil.minusDays(currentDate, 8), 2);
                            put(TimeUtil.minusDays(currentDate, 31), 100);
                        }
                    }));
                }
            });
        });
    }

    @Test
    public void testGetRecommendationByModel() throws IOException {
        prepare();
        val removeLayoutsId = createRemoveLayoutIds(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);
        prepareDataflow();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val response = service.getRecommendationByModel(projectDefault, id);

        Assert.assertEquals(response.getCcRecommendations().size(), recommendation.getCcRecommendations().size());
        Assert.assertEquals(response.getDimensionRecommendations().size(),
                recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(response.getMeasureRecommendations().size(),
                recommendation.getMeasureRecommendations().size());
        Assert.assertEquals(response.getIndexRecommendations().size(),
                recommendation.getLayoutRecommendations().size());

        val aggIndexRecommResponse = response.getIndexRecommendations().stream().filter(r -> r.getType().isAgg())
                .filter(aggIndexItem -> aggIndexItem.getId() == 150001L).findFirst().orElse(null);
        assert aggIndexRecommResponse != null;
        Assert.assertTrue(aggIndexRecommResponse.getType().isRemove());
        Assert.assertEquals(0, aggIndexRecommResponse.getUsage());
        Assert.assertEquals(0, aggIndexRecommResponse.getDataSize());
        val detailResponse = service.getLayoutRecommendationContent(projectDefault, id, "",
                aggIndexRecommResponse.getItemId(), 0, aggIndexRecommResponse.getColumnsAndMeasuresSize());
        Assert.assertEquals(4, detailResponse.getColumnsAndMeasures().size());
        Assert.assertTrue(detailResponse.getColumnsAndMeasures().contains("TEST_KYLIN_FACT.TRANS_ID"));
        Assert.assertTrue(detailResponse.getColumnsAndMeasures().contains("TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL"));
        Assert.assertTrue(detailResponse.getColumnsAndMeasures().contains("SUM_CONSTANT"));
        Assert.assertTrue(detailResponse.getColumnsAndMeasures().contains("COUNT_ALL"));

        val aggLayoutToBeRemovedRes = response.getIndexRecommendations().stream()
                .filter(aggIndexItem -> aggIndexItem.getId() == 1L).findFirst().orElse(null);
        Assert.assertEquals(3, aggLayoutToBeRemovedRes.getUsage());

        val tableIndexLayout = recommendation.getLayoutRecommendations().stream().filter(item -> !item.isAggIndex())
                .collect(Collectors.toList()).get(0).getLayout();
        val tableIndexRecommResponse = response.getIndexRecommendations().stream().filter(r -> r.getType().isTable())
                .findFirst().orElse(null);
        assert tableIndexRecommResponse != null;
        val tableIndexContent = service.getLayoutRecommendationContent(projectDefault, id, "",
                tableIndexRecommResponse.getItemId(), 0, tableIndexRecommResponse.getColumnsAndMeasuresSize());
        Assert.assertEquals(tableIndexLayout.getId(), tableIndexRecommResponse.getId());
        Assert.assertEquals(3, tableIndexContent.getColumnsAndMeasures().size());
        Assert.assertTrue(tableIndexContent.getColumnsAndMeasures().contains("TEST_KYLIN_FACT.LSTG_SITE_ID"));
        Assert.assertTrue(tableIndexContent.getColumnsAndMeasures().contains("TEST_KYLIN_FACT.TRANS_ID"));
        Assert.assertTrue(tableIndexContent.getColumnsAndMeasures().contains("TEST_KYLIN_FACT.LEAF_CATEG_ID"));
    }

    @Test
    public void testGetRecommendationByModel_modification() throws IOException {
        prepare();
        val removeLayoutsId = createRemoveLayoutIds(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        val response = service.getRecommendationByModel(projectDefault, id);

        Assert.assertEquals(3, getAggIndexRecommendations(response).size());
        getAggIndexRecommendations(response).forEach(aggIndexRecommResponse -> {
            if (1L == aggIndexRecommResponse.getId()) {
                Assert.assertTrue(aggIndexRecommResponse.getType().isRemove());
            } else if (150001L == aggIndexRecommResponse.getId()) {
                Assert.assertTrue(aggIndexRecommResponse.getType().isRemove());
            } else if (1000001L == aggIndexRecommResponse.getId()) {
                Assert.assertTrue(aggIndexRecommResponse.getType().isAdd());
            } else {
                Assert.fail();
            }
        });
    }

    @Test
    public void testGetRecommendationByModel_sources() throws IOException {
        prepare();
        val removeLayoutsId = createRemoveLayoutIds(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        var response = service.getRecommendationByModel(projectDefault, id, Lists.newArrayList("imported"));

        Assert.assertEquals(0, response.getIndexRecommendations().size());
        Assert.assertTrue(response.getIndexRecommendations().stream().allMatch(r -> r.getSource().equals("imported")));

        response = service.getRecommendationByModel(projectDefault, id, Lists.newArrayList("query_history"));

        Assert.assertEquals(3, response.getIndexRecommendations().size());
        Assert.assertTrue(
                response.getIndexRecommendations().stream().allMatch(r -> r.getSource().equals("query_history")));

    }

    @Test
    public void testGetAggIndexRecomContent() throws IOException {
        prepare();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val aggIndexRecom = recommendation.getLayoutRecommendations().stream()
                .filter(LayoutRecommendationItem::isAggIndex).findFirst().orElse(null);
        var aggIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "",
                aggIndexRecom.getItemId(), 0, 10);
        Assert.assertEquals(4, aggIndexRecommContent.getSize());
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", aggIndexRecommContent.getColumnsAndMeasures().get(0));
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_1", aggIndexRecommContent.getColumnsAndMeasures().get(1));
        Assert.assertEquals("SUM_CONSTANT", aggIndexRecommContent.getColumnsAndMeasures().get(2));
        Assert.assertEquals("COUNT_DISTINCT_SELLER", aggIndexRecommContent.getColumnsAndMeasures().get(3));

        // test fuzzy filer
        aggIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "trans_id ",
                aggIndexRecom.getItemId(), 0, 10);
        Assert.assertEquals(1, aggIndexRecommContent.getSize());
        Assert.assertEquals(1, aggIndexRecommContent.getColumnsAndMeasures().size());
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", aggIndexRecommContent.getColumnsAndMeasures().get(0));

        // test paging
        aggIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, null,
                aggIndexRecom.getItemId(), 0, 2);
        Assert.assertEquals(4, aggIndexRecommContent.getSize());
        Assert.assertEquals(2, aggIndexRecommContent.getColumnsAndMeasures().size());

        aggIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "test_kylin_fact",
                aggIndexRecom.getItemId(), 0, 1);
        Assert.assertEquals(2, aggIndexRecommContent.getSize());
        Assert.assertEquals(1, aggIndexRecommContent.getColumnsAndMeasures().size());
    }

    @Test
    public void testGetRecommendationWithStorageSize() throws IOException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val recommendation = new OptimizeRecommendation();
        recommendation.setUuid(modelId);
        recommendation.setProject(projectDefault);
        val layoutRecommendation = new LayoutRecommendationItem();
        layoutRecommendation.setAggIndex(true);
        layoutRecommendation.setItemId(0);
        layoutRecommendation.setRecommendationType(RecommendationType.REMOVAL);
        layoutRecommendation.setAdd(false);

        val layoutEntity = indexPlanManager.getIndexPlan(modelId).getIndexEntity(0L).getLayouts().get(0);
        layoutRecommendation.setLayout(JsonUtil.deepCopyQuietly(layoutEntity, LayoutEntity.class));
        recommendation.setLayoutRecommendations(Lists.newArrayList(layoutRecommendation));

        recommendationManager.save(recommendation);
        val response = service.getRecommendationByModel(projectDefault, modelId);
        Assert.assertEquals(1,
                response.getIndexRecommendations().stream()
                        .filter(r -> r.getType().equals(LayoutRecommendationResponse.Type.ADD_AGG)
                                || r.getType().equals(LayoutRecommendationResponse.Type.REMOVE_AGG))
                        .count());

        val aggIndexResponse = response.getIndexRecommendations().stream()
                .filter(r -> r.getType().equals(LayoutRecommendationResponse.Type.ADD_AGG)
                        || r.getType().equals(LayoutRecommendationResponse.Type.REMOVE_AGG))
                .findFirst().get();
        Assert.assertEquals(252928L, aggIndexResponse.getDataSize());

    }

    @Test
    public void testGetTableIndexRecomContent() throws IOException {
        prepare();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val tableIndexRecom = recommendation.getLayoutRecommendations().stream()
                .filter(indexItem -> !indexItem.isAggIndex()).findFirst().orElse(null);
        var tableIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "",
                tableIndexRecom.getItemId(), 0, 10);
        Assert.assertEquals(3, tableIndexRecommContent.getSize());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_SITE_ID", tableIndexRecommContent.getColumnsAndMeasures().get(0));
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", tableIndexRecommContent.getColumnsAndMeasures().get(1));
        Assert.assertEquals("TEST_KYLIN_FACT.LEAF_CATEG_ID", tableIndexRecommContent.getColumnsAndMeasures().get(2));

        // test fuzzy filter
        tableIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "lstg ",
                tableIndexRecom.getItemId(), 0, 10);
        Assert.assertEquals(1, tableIndexRecommContent.getSize());
        Assert.assertEquals(1, tableIndexRecommContent.getColumnsAndMeasures().size());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_SITE_ID", tableIndexRecommContent.getColumnsAndMeasures().get(0));

        // test paging
        tableIndexRecommContent = service.getLayoutRecommendationContent(projectDefault, id, "T.L ",
                tableIndexRecom.getItemId(), 0, 1);
        Assert.assertEquals(2, tableIndexRecommContent.getSize());
        Assert.assertEquals(1, tableIndexRecommContent.getColumnsAndMeasures().size());
    }

    @Test
    public void testBrokenModelApplyRecommendations() {
        val project = "broken_test";

        // broken model
        val modelId = "3f8941de-d01c-42b8-91b5-44646390864b";
        val request = new ApplyRecommendationsRequest();
        request.setProject(project);
        request.setModelId(modelId);

        try {
            service.applyRecommendations(request, project);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof IllegalArgumentException);
            Assert.assertEquals(
                    "model [3f8941de-d01c-42b8-91b5-44646390864b] is broken, cannot apply any optimize recommendations",
                    ex.getMessage());
        }
    }

    private Map<Long, GarbageLayoutType> createRemoveLayoutIds(Long... ids) {
        var removeLayoutsId = Maps.<Long, GarbageLayoutType> newHashMap();
        for (Long layoutId : ids) {
            removeLayoutsId.put(layoutId, GarbageLayoutType.LOW_FREQUENCY);
        }
        return removeLayoutsId;
    }

    @Test
    public void testApplyRecommendations() throws IOException {
        prepare();
        val removeLayoutsId = createRemoveLayoutIds(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        var recommendation = recommendationManager.getOptimizeRecommendation(id);

        val request = new ApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelId(id);
        request.setCcRecommendations(recommendation.getCcRecommendations());
        request.getCcRecommendations().get(0).getCc().setColumnName("CC_changed");
        request.setDimensionRecommendations(recommendation.getDimensionRecommendations());
        request.setMeasureRecommendations(recommendation.getMeasureRecommendations());
        request.getMeasureRecommendations().get(0).getMeasure().setName("measure_changed");

        service.applyRecommendations(request, "default");

        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(0, recommendation.getCcRecommendations().size());
        Assert.assertEquals(0, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(0, recommendation.getMeasureRecommendations().size());

        val optimizedModel = NDataModelManager.getInstance(getTestConfig(), "default").getDataModelDesc(id);
        val expectedModel = JsonUtil.readValue(new File(optimizedModelFile), NDataModel.class);
        val dimensions = optimizedModel.getAllNamedColumns().stream().filter(column -> column.isDimension())
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toList());
        val expectedDimensions = expectedModel.getAllNamedColumns().stream().filter(column -> column.isDimension())
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toList());
        Assert.assertEquals(expectedDimensions.size(), dimensions.size());
        Assert.assertTrue(dimensions.contains("TEST_KYLIN_FACT.CC_CHANGED"));
        val optimizedMeasures = optimizedModel.getAllMeasures().stream().map(MeasureDesc::getName)
                .collect(Collectors.toList());
        val expectedMeasures = expectedModel.getAllMeasures().stream().map(MeasureDesc::getName)
                .collect(Collectors.toList());
        Assert.assertEquals(expectedMeasures.size(), optimizedMeasures.size());
        Assert.assertTrue(optimizedMeasures.contains("MEASURE_CHANGED"));

        // apply index recommendations
        val response = service.getRecommendationByModel(projectDefault, id);
        request.setIndexRecommendationItemIds(response.getIndexRecommendations().stream()
                .map(LayoutRecommendationResponse::getItemId).collect(Collectors.toList()));
        request.setCcRecommendations(Lists.newArrayList());
        request.setMeasureRecommendations(Lists.newArrayList());
        request.setDimensionRecommendations(Lists.newArrayList());

        service.applyRecommendations(request, "default");
        val optimizedIndexPlan = indexPlanManager.getIndexPlan(id);
        val optimizedIndex = optimizedIndexPlan.getIndexes();

        Assert.assertEquals(3, optimizedIndex.size());
        optimizedIndex.forEach(indexEntity -> {
            switch (String.valueOf(indexEntity.getId())) {
            case "10000000":
                Assert.assertEquals(4, indexEntity.getDimensions().size() + indexEntity.getMeasures().size());
                Assert.assertEquals(1, indexEntity.getLayouts().size());
                Assert.assertEquals(Lists.newArrayList(0, 16, 100001, 100003),
                        Lists.newArrayList(indexEntity.getLayouts().get(0).getColOrder()));
                break;
            case "150000":
                Assert.assertEquals(4, indexEntity.getDimensions().size() + indexEntity.getMeasures().size());
                Assert.assertEquals(1, indexEntity.getLayouts().size());
                val layoutIds = indexEntity.getLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());
                Assert.assertTrue(layoutIds.contains(150002L));
                break;
            case "20000000000":
                Assert.assertEquals(4, indexEntity.getLayouts().size());
                break;
            default:
                break;
            }
        });
    }

    @Test
    public void testRemoveRecommendations() throws IOException {
        prepare();
        var recommendation = recommendationManager.getOptimizeRecommendation(id);

        val request = new RemoveRecommendationsRequest();
        request.setProject(projectDefault);
        request.setModelId(id);
        request.setCcItemIds(recommendation.getCcRecommendations().stream().map(CCRecommendationItem::getItemId)
                .collect(Collectors.toList()));
        request.setMeasureItemIds(Lists.newArrayList(0L));
        request.setIndexItemIds(recommendation.getLayoutRecommendations().stream().map(RecommendationItem::getItemId)
                .collect(Collectors.toList()));

        service.removeRecommendations(request, request.getProject());

        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(0, recommendation.getCcRecommendations().size());
        Assert.assertEquals(0, recommendation.getLayoutRecommendations().size());
        Assert.assertEquals(1, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(2, recommendation.getMeasureRecommendations().size());
    }

    @Test
    public void testGetRecommendationsStatsByProject() throws IOException {
        prepare();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val response = service.getRecommendationsStatsByProject("default");
        Assert.assertEquals(recommendation.getRecommendationsCount(), response.getTotalRecommendations());
        Assert.assertEquals(1, response.getModels().size());
        Assert.assertEquals("origin", response.getModels().get(0).getAlias());
        Assert.assertEquals(recommendation.getRecommendationsCount(),
                response.getModels().get(0).getRecommendationsSize());

        val response2 = service.getRecommendationsStatsByProject("match");
        Assert.assertEquals(0, response2.getTotalRecommendations());
        Assert.assertEquals(2, response2.getModels().size());
        Assert.assertEquals(0, response2.getModels().get(0).getRecommendationsSize());

        try {
            service.getRecommendationsStatsByProject("not_exist_project");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof IllegalArgumentException);
            Assert.assertEquals("project [not_exist_project] does not exist", ex.getMessage());
        }
    }

    @Test
    public void testBatchApplyRecommendationsWithNoModelNames() throws IOException {
        prepare();

        service.batchApplyRecommendations("default", null);
        Assert.assertEquals(0, recommendationManager.getRecommendationCount(id));
    }

    @Test
    public void testBatchApplyRecommendations() throws IOException {
        prepare();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        service.batchApplyRecommendations("default", Lists.newArrayList("not_exist_model1"));
        Assert.assertEquals(recommendation.getRecommendationsCount(), recommendationManager.getRecommendationCount(id));

        service.batchApplyRecommendations("default", Lists.newArrayList("not_exist_model", "origin"));
        Assert.assertEquals(0, recommendationManager.getRecommendationCount(id));
    }

    @Test
    public void testBatchApplyRecommendationWithBrokenModel() throws IOException {
        val project = "broken_test";

        // broken model
        val modelId = "3f8941de-d01c-42b8-91b5-44646390864b";

        val recommendation = new OptimizeRecommendation();
        recommendation.setUuid(modelId);

        recommendation.setMeasureRecommendations(
                Lists.newArrayList(new MeasureRecommendationItem(), new MeasureRecommendationItem()));
        recommendation.setDimensionRecommendations(Lists.newArrayList(new DimensionRecommendationItem()));
        val layoutRecommendation1 = new LayoutRecommendationItem();
        val layoutEntity = new LayoutEntity();
        layoutEntity.setId(10001L);
        layoutRecommendation1.setLayout(layoutEntity);
        layoutRecommendation1.setAggIndex(true);

        val layoutRecommendation2 = new LayoutRecommendationItem();
        layoutRecommendation2.setLayout(layoutEntity);
        layoutRecommendation2.setAggIndex(true);
        recommendation.setLayoutRecommendations(Lists.newArrayList(layoutRecommendation1, layoutRecommendation2));

        val recommendationManager = OptimizeRecommendationManager.getInstance(getTestConfig(), project);
        recommendationManager.save(recommendation);

        service.batchApplyRecommendations(project, null);

        Assert.assertEquals(recommendation.getRecommendationsCount(),
                recommendationManager.getRecommendationCount(modelId));
    }

    private List<LayoutRecommendationResponse> getAggIndexRecommendations(OptRecommendationResponse response) {
        return response.getIndexRecommendations().stream().filter(r -> r.getType().isAgg())
                .collect(Collectors.toList());
    }

    private void prepareTwice() throws IOException {
        prepare();
        new OptimizeRecommendationVerifier(getTestConfig(), projectDefault, id).verifyAll();
        val indexPlanOptimized = indexPlanManager.getIndexPlan(id).copy();
        updateIndexPlanByFile(indexPlanOptimized, optimizedIndexPlanTwiceFile);
        val appliedModel = modelManager.copyForWrite(modelManager.getDataModelDesc(id));
        updateModelByFile(appliedModel, optimizedModelTwiceFile);
        recommendationManager.optimize(appliedModel, indexPlanOptimized);
    }

    @Test
    public void testApplyRecommendationCCConflict() throws IOException {
        prepareTwice();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val request = new ApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelId(id);
        request.setCcRecommendations(recommendation.getCcRecommendations());
        request.getCcRecommendations().get(0).getCc().setColumnName("cc_AUTO_1");
        request.setDimensionRecommendations(recommendation.getDimensionRecommendations());
        request.setMeasureRecommendations(recommendation.getMeasureRecommendations());

        thrown.expect(PassConflictException.class);
        thrown.expectMessage("cc cc_AUTO_1 name has already used in model");
        service.applyRecommendations(request, "default");
    }

    @Test
    public void testApplyRecommendationDimensionConflict() throws IOException {
        prepareTwice();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val request = new ApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelId(id);
        request.setCcRecommendations(recommendation.getCcRecommendations());
        request.setDimensionRecommendations(recommendation.getDimensionRecommendations());
        request.getDimensionRecommendations().get(0).getColumn().setName("cc_AUTO_1");
        request.setMeasureRecommendations(recommendation.getMeasureRecommendations());

        thrown.expect(PassConflictException.class);
        thrown.expectMessage("dimension all named column cc_AUTO_1 has already used in model");
        service.applyRecommendations(request, "default");
    }

    @Test
    public void testApplyRecommendationMeasureConflict() throws IOException {
        prepareTwice();
        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val request = new ApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelId(id);
        request.setCcRecommendations(recommendation.getCcRecommendations());
        request.setDimensionRecommendations(recommendation.getDimensionRecommendations());
        request.setMeasureRecommendations(recommendation.getMeasureRecommendations());
        request.getMeasureRecommendations().get(0).getMeasure().setName("max_ITEM");

        thrown.expect(PassConflictException.class);
        thrown.expectMessage("measure name max_ITEM has already used in model");
        service.applyRecommendations(request, "default");
    }
}
