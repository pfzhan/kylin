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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2TestBase;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
import io.kyligence.kap.rest.request.OptRecRequest;

public class OptRecServiceAliasConflictTest extends OptRecV2TestBase {

    OptRecService optRecService = Mockito.spy(new OptRecService());
    ModelService modelService = Mockito.spy(new ModelService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    public OptRecServiceAliasConflictTest() {
        super("../yinglong-smart-service/src/test/resources/ut_rec_v2/cc_name_conflict",
                new String[] { "caa2c0a5-2957-4110-bf2e-d92a7eb7ea97" });
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ModelMetadataInvoker.setDelegate(modelService);
        ReflectionTestUtils.setField(optRecService, "modelMetadataInvoker", new ModelMetadataInvoker());
        ReflectionTestUtils.setField(modelService, "optRecService", optRecService);
    }

    @Test
    public void testCCRecNameConflictWithExistingCC() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(13);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "CC1");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals("The name already exists. Please rename and try again.\n{\"CC1\":[18,-8]}",
                    rootCause.getMessage());
        }
    }

    @Test
    public void testCCRecConflictWithTableColumn() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(13);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "LO_shiPPRIOTITY");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals("The name already exists. Please rename and try again.\n{\"LO_SHIPPRIOTITY\":[-8,9]}",
                    rootCause.getMessage());
        }
    }

    @Test
    public void testCCRecNameNoConflictWithExistingMeasure() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(getDefaultUUID(), copyForWrite -> {
            List<NDataModel.Measure> allMeasures = copyForWrite.getAllMeasures();
            allMeasures.get(0).setName("CC_AUTO__1611234685746_1");
            copyForWrite.getAllNamedColumns().get(10).setName("CC1");
        });
        Assert.assertEquals("CC_AUTO__1611234685746_1",
                modelManager.getDataModelDesc(getDefaultUUID()).getAllMeasures().get(0).getName());

        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "CC_AUTO__1611234685746_1");

        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);
        Assert.assertEquals(4, allLayouts.size());

    }

    @Test
    public void testCCRecNameWithExistingDimension() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16, 23);
        prepare(addLayoutId);
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(getDefaultUUID(), copyForWrite -> {
            NDataModel.NamedColumn column = copyForWrite.getAllNamedColumns().get(10);
            column.setName("CC_AUTO__1611234685746_1");
        });

        Assert.assertEquals("CC_AUTO__1611234685746_1",
                modelManager.getDataModelDesc(getDefaultUUID()).getAllNamedColumns().get(10).getName());

        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "CC_AUTO__1611234685746_1");

        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals("[0, 1, 3, 8, 10, 11, 12, 13, 18, 20]", dimensionList.toString());
        Assert.assertEquals("[100000, 100001, 100002]", measureList.toString());
        Assert.assertEquals(5, allLayouts.size());
    }

    @Test
    public void testNoNameConflictWhenDimensionRecDependOnModelCC() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(17);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-1, "CC1");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals(Lists.newArrayList(10, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000), measureList);
        Assert.assertEquals(1, allLayouts.size());

    }

    @Test
    public void testNoNameConflictBetweenDimensionRecAndModelMeasure() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16, 17);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-3, "count_all");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);
        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);

        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals(5, allLayouts.size());

    }

    @Test
    public void testNoNameConflictBetweenMeasureRecAndModelCC() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-15, "cc1");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);

        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals(4, allLayouts.size());

    }

    @Test
    public void testNameConflictBetweenMeasureRecAndModelMeasure() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16, 17);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-15, "count_all".toUpperCase(Locale.ROOT));
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals(
                    "The name already exists. Please rename and try again.\n" + "{\"COUNT_ALL\":[100000,-15]}",
                    rootCause.getMessage());
        }

    }

    @Test
    public void testNoNameConflictBetweenMeasureRecAndModelColumn() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-15, "V_REVENUE");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        // assert
        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals(4, allLayouts.size());

    }

    @Test
    public void testNoNameNoConflictBetweenCCRecAndDimRecDependedOnCurrentCC() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(22, 23);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-18, "CC2");
        nameMap.put(-19, "CC2");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals(Lists.newArrayList(6, 10, 13, 19), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001), measureList);
        Assert.assertEquals(2, allLayouts.size());
    }

    @Test
    public void testNoNameConflictBetweenCCRecAndDimRec() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(23, 27);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-10, "COMPUTEDCOL005");
        nameMap.put(-25, "computedCol005".toUpperCase(Locale.ROOT));
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        // assert
        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals("[8, 10, 13, 20]", dimensionList.toString());
        Assert.assertEquals("[100000, 100001, 100002]", measureList.toString());
        Assert.assertEquals(2, allLayouts.size());
    }

    @Test
    public void testNoNameConflictBetweenCCAndMeasureRecs() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(22, 23);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-18, "CC2");
        nameMap.put(-21, "CC2");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);
        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals(Lists.newArrayList(6, 10, 13, 19), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001), measureList);
        Assert.assertEquals(2, allLayouts.size());
    }

    @Test
    public void testNoNameConflictBetweenDimAndMeasureRecs() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(22, 23);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-20, "CC2");
        nameMap.put(-21, "cc2".toUpperCase(Locale.ROOT));
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals(Lists.newArrayList(6, 10, 13, 19), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001), measureList);
        Assert.assertEquals(2, allLayouts.size());

    }

    @Test
    public void testCCNameNotConflictWithExistingColumn() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(getDefaultUUID(), copyForWrite -> {
            NDataModel.NamedColumn column = copyForWrite.getAllNamedColumns().get(5);
            column.setName("CC_AUTO__1611234685746_1");
        });

        NDataModel model = getModel();
        List<Integer> existingDimensions = Lists.newArrayList(model.getEffectiveDimensions().keySet());
        existingDimensions.sort(Integer::compareTo);
        Assert.assertEquals("[10, 13]", existingDimensions.toString());
        Assert.assertEquals(1, model.getEffectiveMeasures().size());
        Assert.assertEquals("CC_AUTO__1611234685746_1",
                modelManager.getDataModelDesc(getDefaultUUID()).getAllNamedColumns().get(5).getName());

        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "CC_AUTO__1611234685746_1");

        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));

        Assert.assertEquals("[0, 1, 3, 8, 10, 11, 12, 13, 18]", dimensionList.toString());
        Assert.assertEquals("[100000, 100001, 100002]", measureList.toString());
        Assert.assertEquals(4, allLayouts.size());
    }

    @Test
    public void testDimensionAliasConflict() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);

        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-3, "LO_REVENUE");
        nameMap.put(-4, "LO_REVENUE");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals("The name already exists. Please rename and try again.\n" + "{\"LO_REVENUE\":[-3,-4]}",
                    rootCause.getMessage());
        }
    }

    @Test
    public void testDimensionAliasNoConflictWithMeasure() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);
        prepare(addLayoutId);

        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-3, "LO_REVENUE");
        nameMap.put(-12, "LO_REVENUE");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);
        optRecService.approve(getProject(), recRequest);
        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);

        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals(4, allLayouts.size());
    }

    @Test
    public void testNameConflictBetweenCCRec() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(27);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-8, "computedCol002".toUpperCase(Locale.ROOT));
        nameMap.put(-25, "computedCol002".toUpperCase(Locale.ROOT));
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals(
                    "The name already exists. Please rename and try again.\n" + "{\"COMPUTEDCOL002\":[-8,-25]}",
                    rootCause.getMessage());
        }

    }

    @Test
    public void testNameConflictBetweenMeasureRec() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16, 17);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-15, "measure00");
        nameMap.put(-12, "measure00");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals("The name already exists. Please rename and try again.\n" + "{\"measure00\":[-12,-15]}",
                    rootCause.getMessage());
        }

    }

    @Test
    public void testNameConflictBetweenDimRec() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(24);
        prepare(addLayoutId);
        Map<Integer, String> nameMap = Maps.newHashMap();
        nameMap.put(-9, "dim1");
        nameMap.put(-11, "dim1");
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, nameMap);

        try {
            optRecService.approve(getProject(), recRequest);
            Assert.fail();
        } catch (Exception e) {
            KylinException rootCause = (KylinException) Throwables.getRootCause(e);
            Assert.assertEquals(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION.toErrorCode(), rootCause.getErrorCode());
            Assert.assertEquals("The name already exists. Please rename and try again.\n" + "{\"dim1\":[-9,-11]}",
                    rootCause.getMessage());
        }

    }

    @Test
    public void testApproveWithoutConflict() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(6, 7, 13, 16);

        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        List<Integer> dimensionList = Lists.newArrayList(dataModel.getEffectiveDimensions().keySet());
        List<Integer> measureList = Lists.newArrayList(dataModel.getEffectiveMeasures().keySet().asList());
        dimensionList.sort(Integer::compareTo);
        measureList.sort(Integer::compareTo);
        Assert.assertEquals(Lists.newArrayList(0, 1, 3, 8, 10, 11, 12, 13, 18), dimensionList);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002), measureList);

        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        allLayouts.sort(Comparator.comparing(LayoutEntity::getId));
        Assert.assertEquals(4, allLayouts.size());
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), ImmutableMap.of());
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, Map<Integer, String> nameMap) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), nameMap);
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId,
            Map<Integer, String> nameMap) {
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setModelId(getDefaultUUID());
        recRequest.setProject(getProject());
        recRequest.setRecItemsToAddLayout(addLayoutId);
        recRequest.setRecItemsToRemoveLayout(removeLayoutId);
        recRequest.setNames(nameMap);
        return recRequest;
    }
}
