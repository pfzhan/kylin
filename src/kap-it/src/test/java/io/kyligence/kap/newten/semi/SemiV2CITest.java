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

package io.kyligence.kap.newten.semi;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.ModelSuggestionResponse;
import io.kyligence.kap.rest.response.OptRecDepResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;

public class SemiV2CITest extends SemiAutoTestBase {
    private static final long QUERY_TIME = 1595520000000L;

    private JdbcRawRecStore jdbcRawRecStore;
    private RawRecService rawRecommendation;
    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Before
    public void setup() throws Exception {
        super.setup();
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        rawRecommendation = new RawRecService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        prepareACL();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Test
    public void testCCAsDimensionWithoutRename() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);

        // assert origin model
        List<AbstractContext.NModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // generate raw recommendations for origin model
        QueryHistory qh1 = new QueryHistory();
        qh1.setSql("select price+1, sum(price+1) from test_kylin_fact group by price+1");
        qh1.setQueryTime(QUERY_TIME);
        qh1.setId(1);
        rawRecommendation.generateRawRecommendations(getProject(), Lists.newArrayList(qh1));

        // assert before apply recommendations
        NDataModel modelBeforeApplyRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeApplyRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApplyRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeApplyRecItems.getComputedColumnDescs().isEmpty());
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(4, rawRecItems.size());

        // get layout recommendation and change state to RECOMMENDED
        RawRecItem layoutRecItem = rawRecItems.stream().filter(RawRecItem::isAddLayoutRec).findFirst().orElse(null);
        Assert.assertNotNull(layoutRecItem);
        changeRecItemState(Lists.newArrayList(layoutRecItem), RawRecItem.RawRecState.RECOMMENDED);

        // validateSelectedRecItems
        OptRecDetailResponse optRecDetailResponse = optRecService.validateSelectedRecItems(getProject(), modelID,
                Lists.newArrayList(layoutRecItem.getId()), Lists.newArrayList());
        Assert.assertEquals(1, optRecDetailResponse.getDimensionItems().size());
        Assert.assertEquals(2, optRecDetailResponse.getMeasureItems().size());
        Assert.assertEquals(1, optRecDetailResponse.getCcItems().size());
        final OptRecDepResponse optCCRecDepResponse = optRecDetailResponse.getCcItems().get(0);
        Assert.assertEquals("\"TEST_KYLIN_FACT\".\"PRICE\" + 1", optCCRecDepResponse.getContent());
        final OptRecDepResponse optDimRecDepResponse = optRecDetailResponse.getDimensionItems().get(0);
        Assert.assertEquals(optCCRecDepResponse.getName().replace(ComputedColumnUtil.CC_NAME_PREFIX, "DIMENSION_AUTO_"),
                optDimRecDepResponse.getName());

        // mock optRecRequest() and apply recommendations
        OptRecRequest recRequest = mockOptRecRequest(modelID, optRecDetailResponse);
        UnitOfWork.doInTransactionWithRetry(() -> {
            optRecService.approve(getProject(), recRequest);
            return 0;
        }, "");

        // assert after apply recommendations
        NDataModel modelAfterApplyRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelAfterApplyRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelAfterApplyRecItems.getAllMeasures().size());
        Assert.assertEquals(1, modelAfterApplyRecItems.getComputedColumnDescs().size());
    }

    @Test
    public void testSuggestModel() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        val smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price, sum(price+1) from test_kylin_fact group by price" });
        NSmartMaster smartMaster = new NSmartMaster(smartContext);
        smartMaster.runUtWithContext(smartUtHook);

        // assert origin model
        List<AbstractContext.NModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeOptimization = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelBeforeOptimization.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeOptimization.getAllMeasures().size());
        Assert.assertEquals(1, modelBeforeOptimization.getComputedColumnDescs().size());
        IndexPlan indexPlanBeforeOptimization = modelContexts.get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlanBeforeOptimization.getAllLayouts().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // suggest model and verify result
        List<String> sqlList = ImmutableList.of(
                "select price, item_count, sum(price+1) from \"DEFAULT\".test_kylin_fact inner join edw.test_cal_dt "
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt group by price, item_count",
                "select lstg_format_name, item_count, sum(price+1), sum(price+2) "
                        + "from test_kylin_fact group by lstg_format_name, item_count");
        ModelSuggestionResponse suggestionResp = modelService.suggestModel(getProject(), sqlList, true);
        List<ModelSuggestionResponse.NRecommendedModelResponse> reusedModels = suggestionResp.getReusedModels();
        Assert.assertEquals(1, reusedModels.size());
        ModelSuggestionResponse.NRecommendedModelResponse recommendedModelResponse = reusedModels.get(0);
        List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
        Assert.assertEquals(1, indexes.size());
        LayoutRecDetailResponse layoutRecResp1 = indexes.get(0);
        Assert.assertEquals(1, layoutRecResp1.getSqlList().size());
        Assert.assertTrue(layoutRecResp1.getSqlList().get(0).equalsIgnoreCase(sqlList.get(1)));
        Assert.assertEquals(2, layoutRecResp1.getDimensions().size());
        Assert.assertEquals(3, layoutRecResp1.getMeasures().size());
        Assert.assertEquals(1, layoutRecResp1.getComputedColumns().size());
        List<ModelSuggestionResponse.NRecommendedModelResponse> newModels = suggestionResp.getNewModels();
        Assert.assertEquals(1, newModels.size());
        ModelSuggestionResponse.NRecommendedModelResponse newModelResponse = newModels.get(0);
        List<LayoutRecDetailResponse> newModelIndexes = newModelResponse.getIndexes();
        Assert.assertEquals(1, newModelIndexes.size());
        LayoutRecDetailResponse layoutRecResp2 = newModelIndexes.get(0);
        Assert.assertEquals(1, layoutRecResp2.getSqlList().size());
        Assert.assertTrue(layoutRecResp2.getSqlList().get(0).equalsIgnoreCase(sqlList.get(0)));
        Assert.assertEquals(2, layoutRecResp2.getDimensions().size());
        Assert.assertEquals(2, layoutRecResp2.getMeasures().size());
        Assert.assertEquals(1, layoutRecResp2.getComputedColumns().size());

        // Mock modelRequest and save
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        // assert model optimization result
        NDataModel reusedModelAfter = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(2, reusedModelAfter.getComputedColumnDescs().size());
        Assert.assertEquals(14, reusedModelAfter.getAllNamedColumns().size());
        Assert.assertEquals(3, reusedModelAfter.getAllMeasures().size());
        Assert.assertEquals(3, reusedModelAfter.getAllNamedColumns().stream()//
                .filter(NDataModel.NamedColumn::isDimension).count());
        IndexPlan indexPlanAfter = indexPlanManager.getIndexPlan(modelID);
        Assert.assertEquals(2, indexPlanAfter.getAllLayouts().size());
        String newModelID = newModelResponse.getIndexPlan().getUuid();
        NDataModel newModel = modelManager.getDataModelDesc(newModelID);
        Assert.assertEquals(1, newModel.getComputedColumnDescs().size());
        Assert.assertEquals(113, newModel.getAllNamedColumns().size());
        Assert.assertEquals(2, newModel.getAllMeasures().size());
        Assert.assertEquals(2, newModel.getAllNamedColumns().stream()//
                .filter(NDataModel.NamedColumn::isDimension).count());
        IndexPlan newIndexPlan = indexPlanManager.getIndexPlan(newModelID);
        Assert.assertEquals(1, newIndexPlan.getAllLayouts().size());
    }

    private List<ModelRequest> mockModelRequest(
            List<ModelSuggestionResponse.NRecommendedModelResponse> modelResponses) {
        List<ModelRequest> modelRequestList = Lists.newArrayList();
        modelResponses.forEach(model -> {
            ModelRequest modelRequest = new ModelRequest();
            modelRequest.setUuid(model.getUuid());
            modelRequest.setJoinTables(model.getJoinTables());
            modelRequest.setJoinsGraph(model.getJoinsGraph());
            modelRequest.setFactTableRefs(model.getFactTableRefs());
            modelRequest.setAllTableRefs(model.getAllTableRefs());
            modelRequest.setLookupTableRefs(model.getLookupTableRefs());
            modelRequest.setTableNameMap(model.getTableNameMap());
            modelRequest.setRootFactTableName(model.getRootFactTableName());
            modelRequest.setRootFactTableAlias(model.getRootFactTableAlias());
            modelRequest.setRootFactTableRef(model.getRootFactTableRef());

            modelRequest.setIndexPlan(model.getIndexPlan());
            modelRequest.setAllNamedColumns(model.getAllNamedColumns());
            modelRequest.setAllMeasures(model.getAllMeasures());
            modelRequest.setComputedColumnDescs(model.getComputedColumnDescs());
            modelRequest.setRecItems(model.getIndexes());

            modelRequest.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                    .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList()));
            modelRequest.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                    .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
            modelRequest.setSimplifiedJoinTableDescs(
                    SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(model.getJoinTables()));
            modelRequest.setAlias(model.getAlias());
            modelRequest.setManagementType(model.getManagementType());
            modelRequestList.add(modelRequest);
        });
        return modelRequestList;
    }

    private void changeRecItemState(List<RawRecItem> recItems, RawRecItem.RawRecState state) {
        recItems.forEach(recItem -> recItem.setState(state));
        jdbcRawRecStore.update(recItems);
    }

    private OptRecRequest mockOptRecRequest(String modelID, OptRecDetailResponse optRecDetailResponse) {
        Map<Integer, String> userDefinedNameMap = Maps.newHashMap();
        optRecDetailResponse.getCcItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            String name = item.getName();
            userDefinedNameMap.put(itemId, name.substring(name.indexOf('.') + 1));
        });
        optRecDetailResponse.getDimensionItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            String name = item.getName();
            userDefinedNameMap.put(itemId, name.substring(name.indexOf('.') + 1));
        });
        optRecDetailResponse.getMeasureItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            userDefinedNameMap.put(itemId, item.getName());
        });
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setProject(getProject());
        recRequest.setModelId(modelID);
        recRequest.setRecItemsToAddLayout(optRecDetailResponse.getRecItemsToAddLayout());
        recRequest.setRecItemsToRemoveLayout(optRecDetailResponse.getRecItemsToRemoveLayout());
        recRequest.setNames(userDefinedNameMap);
        return recRequest;
    }
}
