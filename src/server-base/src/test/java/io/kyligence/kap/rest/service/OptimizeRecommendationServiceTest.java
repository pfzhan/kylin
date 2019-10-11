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
import java.util.stream.Collectors;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.IndexRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
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

    @Test
    public void testGetRecommendationByModel() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        // case of MODIFICATION type
        recommendationManager.updateOptimizeRecommendation(id, optRecomm -> {
            val aggIndexRecom = optRecomm.getIndexRecommendations().stream().filter(indexItem -> indexItem.getEntity().getId() == 150000L).findAny().orElse(null);
            val updatedIndexRecom = JsonUtil.deepCopyQuietly(aggIndexRecom, IndexRecommendationItem.class);
            updatedIndexRecom.setItemId(optRecomm.getIndexRecommendations().size());
            updatedIndexRecom.setAdd(true);
            updatedIndexRecom.setRecommendationType(RecommendationType.ADDITION);
            val layoutEntity = new LayoutEntity();
            layoutEntity.setAuto(true);
            layoutEntity.setId(150003L);
            layoutEntity.setColOrder(Lists.newArrayList(12, 0, 100001, 100000));
            updatedIndexRecom.getEntity().setLayouts(Lists.newArrayList(layoutEntity));
            optRecomm.getIndexRecommendations().add(updatedIndexRecom);
        });

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val response = service.getRecommendationByModel(projectDefault, id);

        Assert.assertEquals(response.getCcRecommendations().size(), recommendation.getCcRecommendations().size());
        Assert.assertEquals(response.getDimensionRecommendations().size(),
                recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(response.getMeasureRecommendations().size(),
                recommendation.getMeasureRecommendations().size());
        Assert.assertEquals(
                response.getAggIndexRecommendations().size() + response.getTableIndexRecommendations().size(),
                recommendation.getIndexRecommendations().size() - 1);
        Assert.assertEquals(3, response.getAggIndexRecommendations().size());
        Assert.assertEquals(1, response.getTableIndexRecommendations().size());

        val aggIndexRecommResponse = response.getAggIndexRecommendations().stream().filter(aggIndexItem -> aggIndexItem.getId() == 150000L).findFirst().orElse(null);
        val aggIndexContent = aggIndexRecommResponse.getContent();
        Assert.assertEquals(RecommendationType.MODIFICATION, aggIndexRecommResponse.getRecommendationType());
        Assert.assertEquals(0, aggIndexRecommResponse.getQueryHitCount());
        Assert.assertEquals(0, aggIndexRecommResponse.getStorageSize());
        Assert.assertEquals(4, aggIndexContent.size());
        Assert.assertTrue(aggIndexContent.contains("TEST_KYLIN_FACT.TRANS_ID"));
        Assert.assertTrue(aggIndexContent.contains("TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL"));
        Assert.assertTrue(aggIndexContent.contains("SUM_CONSTANT"));
        Assert.assertTrue(aggIndexContent.contains("COUNT_ALL"));

        val tableIndexLayout = recommendation.getIndexRecommendations().stream().filter(item -> !item.isAggIndex())
                .collect(Collectors.toList()).get(0).getEntity().getLayouts().get(0);
        val tableIndexRecommResponse = response.getTableIndexRecommendations().get(0);
        val tableIndexContent = tableIndexRecommResponse.getColumns();
        Assert.assertEquals(tableIndexLayout.getId(), tableIndexRecommResponse.getId());
        Assert.assertEquals(3, tableIndexContent.size());
        Assert.assertTrue(tableIndexContent.contains("TEST_KYLIN_FACT.LSTG_SITE_ID"));
        Assert.assertTrue(tableIndexContent.contains("TEST_KYLIN_FACT.TRANS_ID"));
        Assert.assertTrue(tableIndexContent.contains("TEST_KYLIN_FACT.LEAF_CATEG_ID"));
    }

    @Test
    public void testGetAggIndexRecomContent() throws IOException {
        prepare();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val aggIndexRecom = recommendation.getIndexRecommendations().stream().filter(IndexRecommendationItem::isAggIndex).findFirst().orElse(null);
        var aggIndexRecommContent = service.getAggIndexRecomContent(projectDefault, id, "", aggIndexRecom.getEntity().getId(), 0, 10);
        Assert.assertEquals(4, aggIndexRecommContent.getSize());
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", aggIndexRecommContent.getContent().get(0));
        Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_1", aggIndexRecommContent.getContent().get(1));
        Assert.assertEquals("SUM_CONSTANT", aggIndexRecommContent.getContent().get(2));
        Assert.assertEquals("COUNT_DISTINCT_SELLER", aggIndexRecommContent.getContent().get(3));

        // test fuzzy filer
        aggIndexRecommContent = service.getAggIndexRecomContent(projectDefault, id, "trans_id ", aggIndexRecom.getEntity().getId(), 0, 10);
        Assert.assertEquals(1, aggIndexRecommContent.getSize());
        Assert.assertEquals(1, aggIndexRecommContent.getContent().size());
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", aggIndexRecommContent.getContent().get(0));

        // test paging
        aggIndexRecommContent = service.getAggIndexRecomContent(projectDefault, id, null, aggIndexRecom.getEntity().getId(), 0, 2);
        Assert.assertEquals(4, aggIndexRecommContent.getSize());
        Assert.assertEquals(2, aggIndexRecommContent.getContent().size());

        aggIndexRecommContent = service.getAggIndexRecomContent(projectDefault, id, "test_kylin_fact", aggIndexRecom.getEntity().getId(), 0, 1);
        Assert.assertEquals(2, aggIndexRecommContent.getSize());
        Assert.assertEquals(1, aggIndexRecommContent.getContent().size());
    }

    @Test
    public void testGetRecommendationWithStorageSize() throws IOException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val recommendation = new OptimizeRecommendation();
        recommendation.setUuid(modelId);
        recommendation.setProject(projectDefault);
        val indexRecommendation = new IndexRecommendationItem();
        indexRecommendation.setAggIndex(true);
        indexRecommendation.setItemId(0);
        indexRecommendation.setRecommendationType(RecommendationType.REMOVAL);
        indexRecommendation.setAdd(false);

        val indexEntity = indexPlanManager.getIndexPlan(modelId).getIndexEntity(0L);
        indexRecommendation.setEntity(JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class));
        recommendation.setIndexRecommendations(Lists.newArrayList(indexRecommendation));

        recommendationManager.save(recommendation);
        val response = service.getRecommendationByModel(projectDefault, modelId);
        Assert.assertEquals(1, response.getAggIndexRecommendations().size());

        val aggIndexResponse = response.getAggIndexRecommendations().get(0);
        Assert.assertEquals(252928L, aggIndexResponse.getStorageSize());

    }

    @Test
    public void testGetTableIndexRecomContent() throws IOException {
        prepare();

        val recommendation = recommendationManager.getOptimizeRecommendation(id);
        val tableIndexRecom = recommendation.getIndexRecommendations().stream().filter(indexItem -> !indexItem.isAggIndex()).findFirst().orElse(null);
        var tableIndexRecommContent = service.getTableIndexRecomContent(projectDefault, id, "", tableIndexRecom.getEntity().getLayouts().get(0).getId(), 0, 10);
        Assert.assertEquals(3, tableIndexRecommContent.getSize());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_SITE_ID", tableIndexRecommContent.getColumns().get(0));
        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", tableIndexRecommContent.getColumns().get(1));
        Assert.assertEquals("TEST_KYLIN_FACT.LEAF_CATEG_ID", tableIndexRecommContent.getColumns().get(2));

        // test fuzzy filter
        tableIndexRecommContent = service.getTableIndexRecomContent(projectDefault, id, "lstg ", tableIndexRecom.getEntity().getLayouts().get(0).getId(), 0, 10);
        Assert.assertEquals(1, tableIndexRecommContent.getSize());
        Assert.assertEquals(1, tableIndexRecommContent.getColumns().size());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_SITE_ID", tableIndexRecommContent.getColumns().get(0));

        // test paging
        tableIndexRecommContent = service.getTableIndexRecomContent(projectDefault, id, "T.L ", tableIndexRecom.getEntity().getLayouts().get(0).getId(), 0, 1);
        Assert.assertEquals(2, tableIndexRecommContent.getSize());
        Assert.assertEquals(1, tableIndexRecommContent.getColumns().size());
    }

    @Test
    public void testApplyRecommendations() throws IOException {
        prepare();
        val removeLayoutsId = Sets.<Long> newHashSet(1L, 150001L);
        recommendationManager.removeLayouts(id, removeLayoutsId);

        // case of MODIFICATION type
        recommendationManager.updateOptimizeRecommendation(id, optRecomm -> {
            val aggIndexRecom = optRecomm.getIndexRecommendations().stream().filter(indexItem -> indexItem.getEntity().getId() == 150000L).findAny().orElse(null);
            val updatedIndexRecom = JsonUtil.deepCopyQuietly(aggIndexRecom, IndexRecommendationItem.class);
            updatedIndexRecom.setItemId(optRecomm.getIndexRecommendations().size());
            updatedIndexRecom.setAdd(true);
            updatedIndexRecom.setRecommendationType(RecommendationType.ADDITION);
            val layoutEntity = new LayoutEntity();
            layoutEntity.setAuto(true);
            layoutEntity.setId(150003L);
            layoutEntity.setColOrder(Lists.newArrayList(12, 0, 100001, 100000));
            updatedIndexRecom.getEntity().setLayouts(Lists.newArrayList(layoutEntity));
            optRecomm.getIndexRecommendations().add(updatedIndexRecom);
        });

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
        val optimizedMeasures = optimizedModel.getAllMeasures().stream().map(MeasureDesc::getName).collect(Collectors.toList());
        val expectedMeasures = expectedModel.getAllMeasures().stream().map(MeasureDesc::getName).collect(Collectors.toList());
        Assert.assertEquals(expectedMeasures.size(), optimizedMeasures.size());
        Assert.assertTrue(optimizedMeasures.contains("MEASURE_CHANGED"));

        // apply index recommendations
        val response = service.getRecommendationByModel(projectDefault, id);
        request.setAggIndexRecommendations(response.getAggIndexRecommendations());
        request.setTableIndexRecommendations(response.getTableIndexRecommendations());
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
                    Assert.assertEquals(Lists.newArrayList(0, 16, 100001, 100002), Lists.newArrayList(indexEntity.getLayouts().get(0).getColOrder()));
                    break;
                case "150000":
                    Assert.assertEquals(4, indexEntity.getDimensions().size() + indexEntity.getMeasures().size());
                    Assert.assertEquals(2, indexEntity.getLayouts().size());
                    val layoutIds = indexEntity.getLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());
                    Assert.assertTrue(layoutIds.contains(150002L));
                    Assert.assertTrue(layoutIds.contains(150003L));
                    break;
                case "20000000000":
                    Assert.assertEquals(3, indexEntity.getLayouts().size());
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
        request.setAggIndexItemIds(recommendation.getIndexRecommendations().stream()
                .map(IndexRecommendationItem::getItemId).collect(Collectors.toList()));

        service.removeRecommendations(request, request.getProject());

        recommendation = recommendationManager.getOptimizeRecommendation(id);
        Assert.assertEquals(0, recommendation.getCcRecommendations().size());
        Assert.assertEquals(0, recommendation.getIndexRecommendations().size());
        Assert.assertEquals(1, recommendation.getDimensionRecommendations().size());
        Assert.assertEquals(2, recommendation.getMeasureRecommendations().size());
    }
}
