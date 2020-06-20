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

import static org.apache.kylin.common.util.JsonUtil.readValue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationManagerV2Test;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.LayoutRecommendationResponse;
import io.kyligence.kap.rest.response.OptRecDepResponse;
import lombok.val;
import lombok.var;

public class OptRecServiceTest extends OptimizeRecommendationManagerV2Test {

    @InjectMocks
    private OptRecService service = Mockito.spy(new OptRecService());

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    protected String otherSameCCExprModelFile;
    protected String otherSameCCNameModelFile;
    protected final String factAlias = "TEST_KYLIN_FACT";

    @Before
    public void setUp() throws Exception {
        modelDir = "../core-metadata/src/test/resources/ut_meta/optimize/metadataV2/model/";
        indexDir = "../core-metadata/src/test/resources/ut_meta/optimize/metadataV2/index_plan/";
        baseModelFile = modelDir + "base_model.json";
        baseIndexFile = indexDir + "base_index_plan.json";
        optimizedModelFile = modelDir + "optimized_model.json";
        optimizedIndexPlanFile = indexDir + "optimized_index_plan.json";
        otherSameCCExprModelFile = modelDir + "other_same_expr_model.json";
        otherSameCCNameModelFile = modelDir + "other_same_name_model.json";
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(service, "aclEvaluate", aclEvaluate);
        super.setUp();
        transferProjectToSemiAutoMode();
    }

    private void transferProjectToSemiAutoMode() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(projectDefault, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.query.metadata.expose-computed-column", "true");
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testGetOptRecLayoutsResponse() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        val addLayoutsResponse = response.getLayouts().stream().filter(l -> l.getType().isAdd())
                .sorted(Comparator.comparingLong(LayoutRecommendationResponse::getItemId)).collect(Collectors.toList());
        Assert.assertEquals(12, addLayoutsResponse.size());
        Assert.assertTrue(addLayoutsResponse.stream().allMatch(
                l -> l.getLastModifyTime() > 0 && l.getCreateTime() > 0 && l.getColumnsAndMeasuresSize() > 0));
    }

    @Test
    public void testGetSingleOptRecDetail_AggLayout() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        val addLayoutsResponse = response.getLayouts().stream().filter(l -> l.getType().isAdd())
                .sorted(Comparator.comparingLong(LayoutRecommendationResponse::getItemId)).collect(Collectors.toList());
        Assert.assertEquals(12, addLayoutsResponse.size());
        val detailResponse = service.getSingleOptRecDetail(projectDefault, id, findFromOriginLayout(1000001L));
        Assert.assertEquals(2, detailResponse.getColumnItems().size());
        Assert.assertEquals(1, detailResponse.getColumnItems().stream().filter(OptRecDepResponse::isAdd).count());
        Assert.assertEquals(2, detailResponse.getDimensionItems().size());
        Assert.assertEquals(1, detailResponse.getDimensionItems().stream().filter(OptRecDepResponse::isAdd).count());
        Assert.assertEquals(2, detailResponse.getMeasureItems().size());
        Assert.assertEquals(1, detailResponse.getMeasureItems().stream().filter(OptRecDepResponse::isAdd).count());

    }

    @Test
    public void testGetSingleOptRecDetail_TableLayout() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        val addLayoutsResponse = response.getLayouts().stream().filter(l -> l.getType().isAdd())
                .sorted(Comparator.comparingLong(LayoutRecommendationResponse::getItemId)).collect(Collectors.toList());
        Assert.assertEquals(12, addLayoutsResponse.size());
        val detailResponse = service.getSingleOptRecDetail(projectDefault, id, findFromOriginLayout(20003000001L));
        Assert.assertEquals(5, detailResponse.getColumnItems().size());
        Assert.assertEquals(1, detailResponse.getColumnItems().stream().filter(OptRecDepResponse::isAdd).count());
        Assert.assertEquals(5, detailResponse.getDimensionItems().size());
        Assert.assertEquals(0, detailResponse.getMeasureItems().size());
    }

    @Test
    public void testGetAllOptRecDetail() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        val addLayoutsResponse = response.getLayouts().stream().filter(l -> l.getType().isAdd())
                .sorted(Comparator.comparingLong(LayoutRecommendationResponse::getItemId)).collect(Collectors.toList());
        val detailResponse = service.getOptRecDetail(projectDefault, id,
                addLayoutsResponse.stream().map(l -> (int) l.getItemId()).collect(Collectors.toList()));
        Assert.assertEquals(1, detailResponse.getColumnItems().size());
        Assert.assertEquals(3, detailResponse.getDimensionItems().size());
        Assert.assertTrue(detailResponse.getDimensionItems().stream()
                .allMatch(r -> r.getName().equals("TEST_KYLIN_FACT_CC_AUTO_1")
                        || r.getName().equals("TEST_KYLIN_FACT_CC_AUTO_2")
                        || r.getName().equals("TEST_KYLIN_FACT_ITEM_COUNT")));
        Assert.assertEquals(2, detailResponse.getMeasureItems().size());
        Assert.assertTrue(detailResponse.getMeasureItems().stream()
                .allMatch(r -> r.getName().equals("MEASURE_AUTO_1") || r.getName().equals("MEASURE_AUTO_2")));
        Assert.assertEquals(12, detailResponse.getLayoutItemIds().size());
    }

    @Test
    public void testPassSingle_AggLayout_NewIndex() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(Lists.newArrayList(findFromOriginLayout(1000001L)));
        service.pass(projectDefault, request);
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 1, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size() + 1, afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size() + 1, afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size() + 1, afterMeasures.size());
        List<LayoutEntity> diffLayouts = difference(afterLayouts, beforeLayouts);
        Assert.assertEquals(1, diffLayouts.size());
        LayoutEntity layoutEntity = diffLayouts.iterator().next();
        Assert.assertTrue(layoutEntity.isAuto());
        Assert.assertFalse(layoutEntity.getIndex().isTableIndex());
        Assert.assertEquals(1, layoutEntity.getIndex().getLayouts().size());
        List<ComputedColumnDesc> diffCCs = difference(afterCCs, beforeCCs);
        Assert.assertEquals(1, diffCCs.size());
        ComputedColumnDesc cc = diffCCs.iterator().next();
        String ccName = "CC_AUTO_2";
        Assert.assertEquals(ccName, cc.getColumnName());
        Assert.assertEquals(factAlias + "." + ccName, cc.getFullName());

        List<NDataModel.NamedColumn> diffColumns = difference(afterNamedColumns, beforeNamedColumns);
        Assert.assertEquals(1, diffColumns.size());
        Assert.assertEquals(17, diffColumns.get(0).getId());
        Assert.assertEquals(factAlias + "_" + ccName, diffColumns.get(0).getName());
        Assert.assertEquals(factAlias + "." + ccName, diffColumns.get(0).getAliasDotColumn());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, diffColumns.get(0).getStatus());

        List<NDataModel.Measure> diffMeasures = difference(afterMeasures, beforeMeasures);
        Assert.assertEquals(1, diffMeasures.size());
        Assert.assertEquals(100003, diffMeasures.get(0).getId());
        Assert.assertEquals("MEASURE_AUTO_1", diffMeasures.get(0).getName());
        Assert.assertEquals("COUNT", diffMeasures.get(0).getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID",
                diffMeasures.get(0).getFunction().getParameters().get(0).getValue());
        Assert.assertEquals(11, recommendationManagerV2.getOptimizeRecommendationV2(id).getRawIds().size());
    }

    @Test
    public void testPassSingle_TableLayout_NewIndex() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(Lists.newArrayList(findFromOriginLayout(20003000001L)));
        service.pass(projectDefault, request);
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 1, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size() + 1, afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size() + 1, afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size(), afterMeasures.size());
        List<LayoutEntity> diffLayouts = difference(afterLayouts, beforeLayouts);
        Assert.assertEquals(1, diffLayouts.size());
        LayoutEntity layoutEntity = diffLayouts.iterator().next();
        Assert.assertTrue(layoutEntity.isAuto());
        Assert.assertTrue(layoutEntity.getIndex().isTableIndex());
        Assert.assertEquals(1, layoutEntity.getIndex().getLayouts().size());
        List<ComputedColumnDesc> diffCCs = difference(afterCCs, beforeCCs);
        Assert.assertEquals(1, diffCCs.size());
        ComputedColumnDesc cc = diffCCs.iterator().next();
        String ccName = "CC_AUTO_2";
        Assert.assertEquals(ccName, cc.getColumnName());
        Assert.assertEquals(factAlias + "." + ccName, cc.getFullName());

        List<NDataModel.NamedColumn> diffColumns = difference(afterNamedColumns, beforeNamedColumns);
        diffColumns.sort(Comparator.comparingInt(NDataModel.NamedColumn::getId));
        Assert.assertEquals(2, diffColumns.size());
        Assert.assertEquals(17, diffColumns.get(1).getId());
        Assert.assertEquals(factAlias + "_" + ccName, diffColumns.get(1).getName());
        Assert.assertEquals(factAlias + "." + ccName, diffColumns.get(1).getAliasDotColumn());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, diffColumns.get(1).getStatus());
    }

    private static <T> List<T> difference(List<T> list1, List<T> list2) {
        return new ArrayList<>(Sets.difference(Sets.newHashSet(list1), Sets.newHashSet(list2)));
    }

    @Test
    public void testPassSingle_AggLayout_ExistIndex() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(Lists.newArrayList(findFromOriginLayout(160002L)));
        service.pass(projectDefault, request);
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 1, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size(), afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size(), afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size(), afterMeasures.size());
        List<LayoutEntity> diffLayouts = difference(afterLayouts, beforeLayouts);
        Assert.assertEquals(1, diffLayouts.size());
        LayoutEntity layoutEntity = diffLayouts.iterator().next();
        Assert.assertTrue(layoutEntity.isAuto());
        Assert.assertFalse(layoutEntity.getIndex().isTableIndex());
        Assert.assertEquals(2, layoutEntity.getIndex().getLayouts().size());
        Assert.assertEquals(11, recommendationManagerV2.getOptimizeRecommendationV2(id).getRawIds().size());
    }

    @Test
    public void testPassSingle_AggLayout_AggGroup() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(Lists.newArrayList(findFromOriginLayout(1000002L)));
        service.pass(projectDefault, request);
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 1, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size(), afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size(), afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size(), afterMeasures.size());
        List<LayoutEntity> diffLayouts = difference(afterLayouts, beforeLayouts);
        Assert.assertEquals(1, diffLayouts.size());
        LayoutEntity layoutEntity = diffLayouts.iterator().next();
        Assert.assertTrue(layoutEntity.isAuto());
        Assert.assertFalse(layoutEntity.getIndex().isTableIndex());
        Assert.assertEquals(2, layoutEntity.getIndex().getLayouts().size());
        List<LayoutEntity> layoutEntities = layoutEntity.getIndex().getLayouts().stream()
                .sorted(Comparator.comparingLong(LayoutEntity::getId)).collect(Collectors.toList());
        Assert.assertTrue(layoutEntities.get(0).isManual());
        Assert.assertTrue(layoutEntities.get(1).isAuto());
        Assert.assertEquals(layoutEntities.get(0).getId() + 1, layoutEntities.get(1).getId());
        Assert.assertEquals(11, recommendationManagerV2.getOptimizeRecommendationV2(id).getRawIds().size());
    }

    @Test
    public void testPassSingle_TableLayout_ExistIndex() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(Lists.newArrayList(findFromOriginLayout(20000000003L)));
        service.pass(projectDefault, request);
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 1, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size(), afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size(), afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size(), afterMeasures.size());
        List<LayoutEntity> diffLayouts = difference(afterLayouts, beforeLayouts);
        Assert.assertEquals(1, diffLayouts.size());
        LayoutEntity layoutEntity = diffLayouts.iterator().next();
        Assert.assertTrue(layoutEntity.isAuto());
        Assert.assertTrue(layoutEntity.getIndex().isTableIndex());
        Assert.assertEquals(3, layoutEntity.getIndex().getLayouts().size());
        Assert.assertEquals(11, recommendationManagerV2.getOptimizeRecommendationV2(id).getRawIds().size());
    }

    @Test
    public void testPassAll() {
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        passAll();
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 12, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size() + 1, afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size() + 1, afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size() + 2, afterMeasures.size());
        exactlyAssertCC(afterCCs, beforeCCs);
        exactlyAssertDimension(afterNamedColumns, beforeNamedColumns);
        exactlyAssertMeasure(afterMeasures, beforeMeasures);
        Assert.assertEquals(0, recommendationManagerV2.getOptimizeRecommendationV2(id).getRawIds().size());
        Assert.assertEquals(18,
                mockRawRecItems.values().stream().filter(r -> r.getState() == RawRecItem.RawRecState.APPLIED).count());
    }

    @Test
    public void testPassAll_CC_Conflict() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.getNames().put(1, "CC_AUTO_1");
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getCC_NAME_CONFLICT("CC_AUTO_1"));
        service.pass(projectDefault, request);
    }

    @Test
    public void testPassAll_Measure_Conflict() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.getNames().put(5, "SUM_CONSTANT");
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getMEASURE_CONFLICT("SUM_CONSTANT"));
        service.pass(projectDefault, request);
    }

    @Test
    public void testPassAll_Dimension_Conflict() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.getNames().put(4, "TEST_KYLIN_FACT_TRANS_ID");
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getDIMENSION_CONFLICT("TEST_KYLIN_FACT_TRANS_ID"));
        service.pass(projectDefault, request);
    }

    @Test
    public void testPassAll_ExistCC() {
        super.testInit_ExistCC();
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        passAll();
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 12, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size(), afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size(), afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size() + 2, afterMeasures.size());
        String ccName = "CC_AUTO_C3";
        List<NDataModel.NamedColumn> diffColumns = difference(afterNamedColumns, beforeNamedColumns).stream()
                .sorted(Comparator.comparingInt(NDataModel.NamedColumn::getId)).collect(Collectors.toList());
        Assert.assertEquals(3, diffColumns.size());
        Assert.assertEquals(17, diffColumns.get(2).getId());
        Assert.assertEquals(factAlias + "_" + ccName, diffColumns.get(2).getName());
        Assert.assertEquals(factAlias + "." + ccName, diffColumns.get(2).getAliasDotColumn());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, diffColumns.get(2).getStatus());
        exactlyAssertMeasure(afterMeasures, beforeMeasures);
    }

    @Test
    public void testPassAll_ExistDimension() {
        super.testInit_ExistDimension();
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        passAll();
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 12, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size() + 1, afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size() + 1, afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size() + 2, afterMeasures.size());
        exactlyAssertCC(afterCCs, beforeCCs);
        String ccName = "CC_AUTO_2";

        List<NDataModel.NamedColumn> diffColumns = difference(afterNamedColumns, beforeNamedColumns).stream()
                .sorted(Comparator.comparingInt(NDataModel.NamedColumn::getId)).collect(Collectors.toList());
        Assert.assertEquals(2, diffColumns.size());
        Assert.assertEquals(17, diffColumns.get(1).getId());
        Assert.assertEquals(factAlias + "_" + ccName, diffColumns.get(1).getName());
        Assert.assertEquals(factAlias + "." + ccName, diffColumns.get(1).getAliasDotColumn());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, diffColumns.get(1).getStatus());
        exactlyAssertMeasure(afterMeasures, beforeMeasures);
    }

    @Test
    public void testPassAll_ExistMeasure() {
        super.testInit_ExistMeasure();
        NDataModel beforeModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> beforeCCs = beforeModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> beforeNamedColumns = beforeModel.getAllNamedColumns();
        List<NDataModel.Measure> beforeMeasures = beforeModel.getAllMeasures();
        List<LayoutEntity> beforeLayouts = indexPlanManager.getIndexPlan(id).getAllLayouts();
        passAll();
        List<LayoutEntity> afterLayouts = NIndexPlanManager.getInstance(getTestConfig(), projectDefault)
                .getIndexPlan(id).getAllLayouts();
        NDataModel afterModel = modelManager.getDataModelDesc(id);
        List<ComputedColumnDesc> afterCCs = afterModel.getComputedColumnDescs();
        List<NDataModel.NamedColumn> afterNamedColumns = afterModel.getAllNamedColumns();
        List<NDataModel.Measure> afterMeasures = afterModel.getAllMeasures();
        Assert.assertEquals(beforeLayouts.size() + 12, afterLayouts.size());
        Assert.assertEquals(beforeCCs.size() + 1, afterCCs.size());
        Assert.assertEquals(beforeNamedColumns.size() + 1, afterNamedColumns.size());
        Assert.assertEquals(beforeMeasures.size() + 1, afterMeasures.size());
        exactlyAssertCC(afterCCs, beforeCCs);
        exactlyAssertDimension(afterNamedColumns, beforeNamedColumns);
        List<NDataModel.Measure> diffMeasures = difference(afterMeasures, beforeMeasures).stream()
                .sorted(Comparator.comparingInt(NDataModel.Measure::getId)).collect(Collectors.toList());
        Assert.assertEquals(1, diffMeasures.size());
        Assert.assertEquals(100004, diffMeasures.get(0).getId());
        Assert.assertEquals("MEASURE_AUTO_2", diffMeasures.get(0).getName());
        Assert.assertEquals("MAX", diffMeasures.get(0).getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT",
                diffMeasures.get(0).getFunction().getParameters().get(0).getValue());
    }

    private void passAll() {
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        service.pass(projectDefault, request);
    }

    private void exactlyAssertCC(List<ComputedColumnDesc> afterCCs, List<ComputedColumnDesc> beforeCCs) {
        List<ComputedColumnDesc> diffCCs = difference(afterCCs, beforeCCs);
        Assert.assertEquals(1, diffCCs.size());
        ComputedColumnDesc cc = diffCCs.iterator().next();
        String ccName = "CC_AUTO_2";
        Assert.assertEquals(ccName, cc.getColumnName());
        Assert.assertEquals(factAlias + "." + ccName, cc.getFullName());
    }

    private void exactlyAssertDimension(List<NDataModel.NamedColumn> afterNamedColumns,
            List<NDataModel.NamedColumn> beforeNamedColumns) {
        List<NDataModel.NamedColumn> diffColumns = difference(afterNamedColumns, beforeNamedColumns).stream()
                .sorted(Comparator.comparingInt(NDataModel.NamedColumn::getId)).collect(Collectors.toList());
        diffColumns.sort(Comparator.comparingInt(NDataModel.NamedColumn::getId));
        Assert.assertEquals(3, diffColumns.size());
        Assert.assertEquals(17, diffColumns.get(2).getId());
        String ccName = "CC_AUTO_2";
        Assert.assertEquals(factAlias + "_" + ccName, diffColumns.get(2).getName());
        Assert.assertEquals(factAlias + "." + ccName, diffColumns.get(2).getAliasDotColumn());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, diffColumns.get(2).getStatus());
    }

    private void exactlyAssertMeasure(List<NDataModel.Measure> afterMeasures, List<NDataModel.Measure> beforeMeasures) {
        List<NDataModel.Measure> diffMeasures = difference(afterMeasures, beforeMeasures).stream()
                .sorted(Comparator.comparingInt(NDataModel.Measure::getId)).collect(Collectors.toList());
        Assert.assertEquals(2, diffMeasures.size());
        Assert.assertEquals(100003, diffMeasures.get(0).getId());
        Assert.assertEquals("MEASURE_AUTO_1", diffMeasures.get(0).getName());
        Assert.assertEquals("COUNT", diffMeasures.get(0).getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID",
                diffMeasures.get(0).getFunction().getParameters().get(0).getValue());
        Assert.assertEquals(100004, diffMeasures.get(1).getId());
        Assert.assertEquals("MEASURE_AUTO_2", diffMeasures.get(1).getName());
        Assert.assertEquals("MAX", diffMeasures.get(1).getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT",
                diffMeasures.get(1).getFunction().getParameters().get(0).getValue());
    }

    @Test
    public void testPassAll_CCConflictSameExpr() throws IOException {
        val otherModel = readValue(new File(otherSameCCExprModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.getNames().put(1, "CC_AUTO_2");
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getCC_EXPRESSION_CONFLICT(
                "TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT * 2", "CC_AUTO_2", "CC_OTHER_4"));
        service.pass(projectDefault, request);
    }

    @Test
    public void testPassAll_CCConflictSameName() throws IOException {
        val otherModel = readValue(new File(otherSameCCNameModelFile), NDataModel.class);
        modelManager.createDataModelDesc(otherModel, ownerTest);
        val response = service.getOptRecLayoutsResponse(projectDefault, id);
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.getNames().put(1, "CC_AUTO_2");
        request.setProject(projectDefault);
        request.setIds(response.getLayouts().stream().filter(l -> l.getType().isAdd()).map(l -> (int) l.getItemId())
                .collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getCC_NAME_CONFLICT("CC_AUTO_2"));
        service.pass(projectDefault, request);
    }

    @Test
    public void testCleanAll() {
        service.clean(projectDefault, id);
        assertListSize(0, 0, 0, 0);
    }

    @Test
    public void testDelete() {
        OptRecRequest request = new OptRecRequest();
        request.setModelId(id);
        request.setIds(Lists.newArrayList(11, 12, 13, 14, 15));
        service.delete(projectDefault, request);
        val recommendation = recommendationManagerV2.getOptimizeRecommendationV2(id);
        Assert.assertEquals(7, recommendation.getRawIds().size());
    }

}
