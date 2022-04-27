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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.OpenRecApproveResponse.RecToIndexResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;

public class OptRecServiceTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setup() throws Exception {
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        prepareACL();
        QueryHistoryTaskScheduler.getInstance(getProject()).init();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        QueryHistoryTaskScheduler.shutdownByProject(getProject());
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    public OptRecServiceTest() {
        super("../yinglong-smart-service/src/test/resources/ut_rec_v2/opt_service",
                new String[] { "db89adb4-3aad-4f2a-ac2e-72ea0a30420b" });
    }

    @Test
    public void testGetOptRecRequestExcludeRecIfDeleteDependLayout() throws IOException {
        prepareAllLayoutRecs();
        OptRecLayoutsResponse recResp1 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(28, recResp1.getLayouts().size());

        // delete the depend layout
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        Set<Long> deletedLayouts = Sets.newHashSet();
        deletedLayouts.add(130001L);
        indexMgr.updateIndexPlan(getDefaultUUID(), copyForWrite -> {
            copyForWrite.removeLayouts(deletedLayouts, true, true);
        });
        OptRecLayoutsResponse recResp2 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(27, recResp2.getLayouts().size());
    }

    @Test
    public void testGetOptRecRequest() throws IOException {
        // test get all
        prepareAllLayoutRecs();
        OptRecLayoutsResponse recResp1 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(28, recResp1.getLayouts().size());
        assertOptRecDetailResponse(recResp1);

        // set topN to 50, get all and assert
        changeRecTopN(50);
        OptRecLayoutsResponse recResp2 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(35, recResp2.getLayouts().size());
        assertOptRecDetailResponse(recResp2);

        // test empty recTypeList
        OptRecLayoutsResponse recResp = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList(), null, false, "", 0, 10);
        Assert.assertEquals(10, recResp.getLayouts().size());
        assertOptRecDetailResponse(recResp);

        // only get add_table_index
        OptRecLayoutsResponse recResp3 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp3.getLayouts().size());
        assertOptRecDetailResponse(recResp3);

        // test limit
        OptRecLayoutsResponse recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX", "ADD_TABLE_INDEX"), null, false, "", 0, 30);
        Assert.assertEquals(27, recResp4.getLayouts().size());
        assertOptRecDetailResponse(recResp4);
        recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "", 0, 20);
        Assert.assertEquals(20, recResp4.getLayouts().size());
        assertOptRecDetailResponse(recResp4);

        // test offset
        OptRecLayoutsResponse recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 1, 10);
        Assert.assertTrue(recResp5.getLayouts().isEmpty());
        assertOptRecDetailResponse(recResp5);
        recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp5.getLayouts().size());
        assertOptRecDetailResponse(recResp5);

        // test orderBy
        OptRecLayoutsResponse recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(recResp6.getLayouts().size() - 1).getId());
        assertOptRecDetailResponse(recResp6);
        recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, true, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(0).getId());
        assertOptRecDetailResponse(recResp6);
    }

    private void assertOptRecDetailResponse(OptRecLayoutsResponse recResp) {
        recResp.getLayouts().forEach(layout -> {
            OptRecDetailResponse optRecDetailResponse = optRecService.getSingleOptRecDetail(getProject(),
                    getDefaultUUID(), layout.getId(), layout.isAdd());
            Assert.assertEquals(optRecDetailResponse, layout.getRecDetailResponse());
        });
    }

    private void prepareAllLayoutRecs() throws IOException {
        prepare(Lists.newArrayList(2, 3, 6, 10, 24, 59, 60, 61, 62, 76, 77, 78, 79, 80, 82, 83, 84, 85, 87, 88, 89, 91,
                92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104));
    }

    @Test
    public void testApproveAll() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        optRecService.batchApprove(getProject(), "all");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(56, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(21, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveAllRemovalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        optRecService.batchApprove(getProject(), "REMOVE_INDEX");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(7, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(1, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveAllAdditionalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        changeRecTopN(50);
        optRecService.batchApprove(getProject(), "ADD_INDEX");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(36, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveOneModel() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toLowerCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecService.batchApprove(getProject(), modelIds, "all");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(27, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    @Test
    public void testApproveOneModelWithUpperCase() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toUpperCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecService.batchApprove(getProject(), modelIds, "all");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(27, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    @Test
    public void testApproveIllegalLayoutRecommendation() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        prepareAbnormalLayoutRecommendation();

        RawRecItem rawRecItem = jdbcRawRecStore.queryAll().stream() //
                .filter(recItem -> recItem.getId() == 6) //
                .findAny().orElse(null);
        Assert.assertNotNull(rawRecItem);
        List<Integer> dependIds = Lists.newArrayList();
        for (int dependID : rawRecItem.getDependIDs()) {
            dependIds.add(dependID);
        }
        Assert.assertEquals(Lists.newArrayList(4, 16, 14, 100000, -1, -1), dependIds);

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toLowerCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecService.batchApprove(getProject(), modelIds, "all");

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(57, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(26, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    private void prepareAbnormalLayoutRecommendation() {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.forEach(recItem -> {
            // prepare an abnormal RawRecItem
            if (recItem.getId() == 6) {
                final LayoutRecItemV2 recEntity = (LayoutRecItemV2) recItem.getRecEntity();
                recEntity.getLayout().setColOrder(Lists.newArrayList(4, 16, 14, 100000, -1, -1));
                recItem.setDependIDs(new int[] { 4, 16, 14, 100000, -1, -1 });
            }
        });
        jdbcRawRecStore.batchAddOrUpdate(rawRecItems);
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }
}
