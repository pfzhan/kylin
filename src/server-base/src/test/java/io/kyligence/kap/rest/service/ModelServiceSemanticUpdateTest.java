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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelServiceSemanticUpdateTest extends LocalFileMetadataTestCase {

    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        modelService.setSemanticUpdater(semanticService);
        indexPlanService.setSemanticUpater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val projectManager = NProjectManager.getInstance(getTestConfig());
        val projectInstance = projectManager.copyForWrite(projectManager.getProject(getProject()));
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstance);

        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelMgr.updateDataModel(MODEL_ID, model -> {
            model.setManagementType(ManagementType.MODEL_BASED);
        });
        modelMgr.updateDataModel("741ca86a-1f13-46da-a59f-95fb68615e3a", model -> {
            model.setManagementType(ManagementType.MODEL_BASED);
        });

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
    }

    private String getProject() {
        return "default";
    }

    @Before
    public void setup() {
        super.setup();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateCC_DontNeedReload() throws Exception {
        ModelRequest request = newSemanticRequest();
        for (ComputedColumnDesc cc : request.getComputedColumnDescs()) {
            if (cc.getColumnName().equalsIgnoreCase("DEAL_AMOUNT")) {
                cc.setComment("comment1");
            }
        }
        modelService.updateDataModelSemantic(request.getProject(), request);

        NDataModel model = getTestModel();
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            if (cc.getColumnName().equalsIgnoreCase("DEAL_AMOUNT")) {
                Assert.assertEquals("comment1", cc.getComment());
            }
        }

        val colIdOfCC = model.getColumnIdByColumnName("TEST_KYLIN_FACT.DEAL_AMOUNT");
        Assert.assertEquals(27, colIdOfCC);
    }

    @Test
    public void testModelUpdateComputedColumn() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        // Add new computed column
        final int colIdOfCC;
        {
            ModelRequest request = newSemanticRequest();
            Assert.assertFalse(request.getComputedColumnNames().contains("TEST_CC_1"));
            ComputedColumnDesc newCC = new ComputedColumnDesc();
            newCC.setColumnName("TEST_CC_1");
            newCC.setExpression("1 + 1");
            newCC.setDatatype("integer");
            newCC.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
            newCC.setTableAlias("TEST_KYLIN_FACT");
            request.getComputedColumnDescs().add(newCC);
            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            Assert.assertTrue(model.getComputedColumnNames().contains("TEST_CC_1"));
            colIdOfCC = model.getColumnIdByColumnName("TEST_KYLIN_FACT.TEST_CC_1");
            Assert.assertNotEquals(-1, colIdOfCC);
        }

        // Add dimension which uses TEST_CC_1, column will be renamed
        {
            ModelRequest request = newSemanticRequest();
            //            Assert.assertEquals(-1, request.getColumnIdByColumnName("TEST_KYLIN_FACT.TEST_CC_1"));
            NamedColumn newDimension = new NamedColumn();
            newDimension.setName("TEST_DIM_WITH_CC");
            newDimension.setAliasDotColumn("TEST_KYLIN_FACT.TEST_CC_1");
            newDimension.setStatus(ColumnStatus.DIMENSION);
            request.getSimplifiedDimensions().add(newDimension);
            modelService.updateDataModelSemantic(request.getProject(), request);

            ModelRequest requestToVerify = newSemanticRequest();
            Assert.assertEquals(colIdOfCC, requestToVerify.getColumnIdByColumnName("TEST_KYLIN_FACT.TEST_CC_1"));
            NamedColumn dimensionToVerify = requestToVerify.getSimplifiedDimensions().stream()
                    .filter(col -> col.getId() == colIdOfCC).findFirst().get();
            Assert.assertNotNull(dimensionToVerify);
            Assert.assertEquals("TEST_DIM_WITH_CC", dimensionToVerify.getName());
            Assert.assertEquals(ColumnStatus.DIMENSION, dimensionToVerify.getStatus());
        }

        // Add measure which uses TEST_CC_1
        final int measureIdOfCC;
        {
            ModelRequest request = newSemanticRequest();
            SimplifiedMeasure newMeasure = new SimplifiedMeasure();
            newMeasure.setName("TEST_MEASURE_WITH_CC");
            newMeasure.setExpression("SUM");
            newMeasure.setReturnType("bigint");
            ParameterResponse param = new ParameterResponse("column", "TEST_KYLIN_FACT.TEST_CC_1");
            newMeasure.setParameterValue(Lists.newArrayList(param));
            request.getSimplifiedMeasures().add(newMeasure);

            SimplifiedMeasure newMeasure2 = new SimplifiedMeasure();
            newMeasure2.setName("TEST_MEASURE_CONSTANT");
            newMeasure2.setExpression("SUM");
            newMeasure2.setReturnType("bigint");
            newMeasure2.setParameterValue(Lists.newArrayList(new ParameterResponse("constant", "1")));
            request.getSimplifiedMeasures().add(newMeasure2);

            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            Measure measure = model.getAllMeasures().stream().filter(m -> m.getName().equals("TEST_MEASURE_WITH_CC"))
                    .findFirst().get();
            Assert.assertNotNull(measure);
            measureIdOfCC = measure.getId();
            Assert.assertTrue(measure.getFunction().isSum());
            Assert.assertEquals("TEST_KYLIN_FACT.TEST_CC_1", measure.getFunction().getParameters().get(0).getValue());
        }

        // Update TEST_CC_1's definition, named column and measure will be updated
        {
            ModelRequest request = newSemanticRequest();
            ComputedColumnDesc ccDesc = request.getComputedColumnDescs().stream()
                    .filter(cc -> cc.getColumnName().equals("TEST_CC_1")).findFirst().get();
            Assert.assertNotNull(ccDesc);
            ccDesc.setExpression("1 + 2");
            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            NamedColumn originalColumn = model.getAllNamedColumns().stream().filter(col -> col.getId() == colIdOfCC)
                    .findFirst().get();
            Assert.assertNotNull(originalColumn);
            Assert.assertEquals("TEST_DIM_WITH_CC", originalColumn.getName());
            Assert.assertEquals(ColumnStatus.DIMENSION, originalColumn.getStatus());

            Measure originalMeasure = model.getAllMeasures().stream().filter(m -> m.getId() == measureIdOfCC)
                    .findFirst().get();
            Assert.assertNotNull(originalMeasure);
            Assert.assertEquals("TEST_MEASURE_WITH_CC", originalMeasure.getName());
            Assert.assertFalse(originalMeasure.isTomb());
        }

        // Remove TEST_CC_1, all related should be moved to tomb
        {
            ModelRequest request = newSemanticRequest();
            Assert.assertTrue(request.getComputedColumnDescs().removeIf(cc -> cc.getColumnName().equals("TEST_CC_1")));
            try {
                modelService.updateDataModelSemantic(request.getProject(), request);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(
                        "Cannot init measure TEST_MEASURE_WITH_CC: Column 'TEST_KYLIN_FACT.TEST_CC_1' not found in any table.",
                        e.getMessage());
            }

            // remove broken measure
            request.getSimplifiedMeasures().removeIf(m -> m.getName().equals("TEST_MEASURE_WITH_CC"));
            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            Assert.assertFalse(model.getAllNamedColumns().stream().filter(c -> c.getId() == colIdOfCC).findFirst().get()
                    .isExist());
            Assert.assertTrue(
                    model.getAllMeasures().stream().filter(m -> m.getId() == measureIdOfCC).findFirst().get().isTomb());
        }
    }

    @Test
    public void testModelUpdateMeasures() throws Exception {
        val request = newSemanticRequest();
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("GMV_AVG");
        newMeasure1.setExpression("AVG");
        newMeasure1.setReturnType("bitmap");
        val param = new ParameterResponse("column", "TEST_KYLIN_FACT.PRICE");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        request.setSimplifiedMeasures(request.getSimplifiedMeasures().stream()
                .filter(m -> m.getId() != 100002 && m.getId() != 100003).collect(Collectors.toList()));
        // add new measure and remove 1002 and 1003
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject())
                .getIndexPlan(getTestModel().getUuid());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, getProject());
        modelService.updateDataModelSemantic(getProject(), request);

        val model = getTestModel();
        Assert.assertEquals("GMV_AVG", model.getEffectiveMeasures().get(100018).getName());
        Assert.assertNull(model.getEffectiveMeasures().get(100002));
        Assert.assertNull(model.getEffectiveMeasures().get(100003));
    }

    @Test
    public void testUpdateMeasure_DuplicateParams() throws Exception {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "The measure definition is the same as the measure 'TRANS_SUM2'. So this measure cannot be created.");
        val request = newSemanticRequest();
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("TRANS_SUM2");
        newMeasure1.setExpression("SUM");
        val param = new ParameterResponse();
        param.setType("column");
        param.setValue("TEST_KYLIN_FACT.PRICE");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        newMeasure1.setReturnType("decimal");
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testUpdateMeasure_ChangeReturnType() throws Exception {
        val request = newSemanticRequest();
        for (SimplifiedMeasure simplifiedMeasure : request.getSimplifiedMeasures()) {
            if (simplifiedMeasure.getReturnType().equals("bitmap")) {
                simplifiedMeasure.setReturnType("hllc(12)");
            }
        }
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject())
                .getIndexPlan(getTestModel().getUuid());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, getProject());
        modelService.updateDataModelSemantic(getProject(), request);
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertNull(model.getEffectiveMeasures().get(100010));
        Assert.assertEquals(1, model.getAllMeasures().stream()
                .filter(m -> m.getFunction().getReturnType().equals("hllc(12)")).count());
    }

    @Test
    public void testModelUpdateMeasureName() throws Exception {
        val request = newSemanticRequest();
        request.getSimplifiedMeasures().get(0).setName("NEW_MEASURE");
        val originId = request.getSimplifiedMeasures().get(0).getId();
        modelService.updateDataModelSemantic(getProject(), request);

        val model = getTestModel();
        Assert.assertEquals("NEW_MEASURE", model.getEffectiveMeasures().get(originId).getName());
    }

    @Test
    public void testRenameTableAlias() throws Exception {
        var request = newSemanticRequest();
        val OLD_ALIAS = "TEST_ORDER";
        val NEW_ALIAS = "NEW_ALIAS";
        val colCount = request.getAllNamedColumns().stream().filter(n -> n.getAliasDotColumn().startsWith("TEST_ORDER"))
                .count();
        request = changeAlias(request, OLD_ALIAS, NEW_ALIAS);
        modelService.updateDataModelSemantic(getProject(), request);

        val model = getTestModel();
        val tombCount = model.getAllNamedColumns().stream().filter(n -> n.getAliasDotColumn().startsWith("TEST_ORDER"))
                .peek(col -> {
                    Assert.assertEquals(ColumnStatus.TOMB, col.getStatus());
                }).count();
        Assert.assertEquals(0, tombCount);
        val otherTombCount = model.getAllNamedColumns().stream()
                .filter(n -> !n.getAliasDotColumn().startsWith("TEST_ORDER")).filter(nc -> !nc.isExist()).count();
        Assert.assertEquals(1, otherTombCount);
        Assert.assertEquals(202, model.getAllNamedColumns().size());
        val executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testRenameTableAliasUsedAsMeasure() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().forEach(modelManager::dropModel);
        val request = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure.json"), ModelRequest.class);
        val newModel = modelService.createModel(request.getProject(), request);

        val updateRequest = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias.json"),
                ModelRequest.class);
        updateRequest.setUuid(newModel.getUuid());
        modelService.updateDataModelSemantic(getProject(), updateRequest);

        var model = modelService.getDataModelManager(getProject()).getDataModelDesc(updateRequest.getUuid());
        Assert.assertThat(
                model.getAllMeasures().stream().filter(m -> !m.isTomb()).sorted(Comparator.comparing(Measure::getId))
                        .map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("MAX1", "COUNT_ALL")));

        // make sure update again is ok
        val updateRequest2 = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias_twice.json"),
                ModelRequest.class);
        updateRequest2.setUuid(newModel.getUuid());
        modelService.updateDataModelSemantic(getProject(), updateRequest2);
        model = modelService.getDataModelManager(getProject()).getDataModelDesc(updateRequest.getUuid());
        Assert.assertThat(
                model.getAllMeasures().stream().filter(m -> !m.isTomb()).sorted(Comparator.comparing(Measure::getId))
                        .map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("MAX1", "COUNT_ALL")));
        Assert.assertEquals(2, model.getAllMeasures().size());
    }

    @Test
    public void testModelUpdateDimensions() throws Exception {
        val request = newSemanticRequest();
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream()
                .filter(c -> c.isDimension() && c.getId() != 25).collect(Collectors.toList()));
        val newCol = new NDataModel.NamedColumn();
        newCol.setName("PRICE2");
        newCol.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        newCol.setStatus(NDataModel.ColumnStatus.DIMENSION);
        request.getSimplifiedDimensions().add(newCol);
        ComputedColumnDesc ccDesc = request.getComputedColumnDescs().stream()
                .filter(cc -> "DEAL_YEAR".equals(cc.getColumnName())).findFirst().orElse(null);
        Assert.assertNotNull(ccDesc);
        NamedColumn ccCol = request.getAllNamedColumns().stream()
                .filter(c -> c.getAliasDotColumn().equals(ccDesc.getFullName())).findFirst().orElse(null);
        Assert.assertNotNull(ccCol);
        Assert.assertTrue(ccCol.getStatus() == ColumnStatus.DIMENSION);
        int ccColId = ccCol.getId();
        request.getComputedColumnDescs().remove(ccDesc);

        val prevId = getTestModel().getAllNamedColumns().stream()
                .filter(n -> n.getAliasDotColumn().equals(newCol.getAliasDotColumn())).findFirst().map(n -> n.getId())
                .orElse(0);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject())
                .getIndexPlan(getTestModel().getUuid());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, getProject());
        modelService.updateDataModelSemantic(getProject(), request);

        val model = getTestModel();
        Assert.assertEquals(newCol.getName(), model.getNameByColumnId(prevId));
        Assert.assertNull(model.getEffectiveDimensions().get(25));
        Assert.assertFalse(model.getComputedColumnNames().contains("DEAL_YEAR"));
        Assert.assertNull(model.getEffectiveDimensions().get(ccColId));
        Assert.assertNull(model.getEffectiveCols().get(ccColId));

        newCol.setName("PRICE3");
        request.getComputedColumnDescs().add(ccDesc);
        modelService.updateDataModelSemantic(getProject(), request);
        val model2 = getTestModel();
        Assert.assertEquals(newCol.getName(), model2.getNameByColumnId(prevId));
        Assert.assertTrue(model2.getComputedColumnNames().contains("DEAL_YEAR"));
        NamedColumn newCcCol = model2.getAllNamedColumns().stream()
                .filter(c -> c.getAliasDotColumn().equals(ccDesc.getFullName())).filter(c -> c.isExist()).findFirst()
                .orElse(null);
        Assert.assertNotNull(newCcCol);
        Assert.assertNotEquals(ccColId, newCcCol.getId());
    }

    @Test
    public void testRemoveColumnExistInTableIndex() throws Exception {
        val request = newSemanticRequest();
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream()
                .filter(c -> c.isDimension() && c.getId() != 25).collect(Collectors.toList()));
        val newCol = new NDataModel.NamedColumn();
        newCol.setName("PRICE2");
        newCol.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        newCol.setStatus(NDataModel.ColumnStatus.DIMENSION);
        request.getSimplifiedDimensions().add(newCol);
        ComputedColumnDesc ccDesc = request.getComputedColumnDescs().stream()
                .filter(cc -> "DEAL_YEAR".equals(cc.getColumnName())).findFirst().orElse(null);
        Assert.assertNotNull(ccDesc);
        NamedColumn ccCol = request.getAllNamedColumns().stream()
                .filter(c -> c.getAliasDotColumn().equals(ccDesc.getFullName())).findFirst().orElse(null);
        Assert.assertNotNull(ccCol);
        request.getComputedColumnDescs().remove(ccDesc);

        thrown.expect(KylinException.class);
        thrown.expectMessage("The dimension BUYER_COUNTRY.NAME,TEST_KYLIN_FACT.DEAL_YEAR is being referenced by "
                + "aggregation group, recommended aggregate index or table index. Please delete this dimension from the above first.");
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testRemoveDimensionExistInAggIndex() throws Exception {
        String modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        val request = newSemanticRequest(modelId);
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream()
                .filter(c -> c.isDimension() && c.getId() != 25).collect(Collectors.toList()));
        NamedColumn dimDesc = request.getSimplifiedDimensions().stream()
                .filter(cc -> "LSTG_FORMAT_NAME".equals(cc.getName())).findFirst().orElse(null);
        Assert.assertNotNull(dimDesc);
        request.getSimplifiedDimensions().remove(dimDesc);
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "The dimension TEST_KYLIN_FACT.LSTG_FORMAT_NAME,BUYER_COUNTRY.NAME is being referenced by aggregation group,"
                        + " recommended aggregate index or table index. Please delete this dimension from the above first.");
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testRemoveMeasureExistInAggIndex() throws Exception {
        String modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        val request = newSemanticRequest(modelId);
        request.getSimplifiedMeasures().remove(1);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The measure GMV_SUM is referenced by indexes. Please try again after "
                + "deleting it from aggregation group or table index.");
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testRemoveCCExistInTableIndexWithAggGroup() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        // ensure model has an agg group
        NAggregationGroup newAggregationGroup = new NAggregationGroup();
        newAggregationGroup.setIncludes(new Integer[] { 0 });
        newAggregationGroup.setMeasures(new Integer[] { 100000 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        newAggregationGroup.setSelectRule(selectRule);
        indexPlanService.updateRuleBasedCuboid(getProject(),
                UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(modelId)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(newAggregationGroup)).build());

        // remove cc TEST_KYLIN_FACT.NEST5
        var request = newSemanticRequest(modelId);
        request.getComputedColumnDescs().removeIf(c -> ("NEST5").equals(c.getColumnName()));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("The dimension TEST_KYLIN_FACT.NEST5 is being referenced by aggregation group, "
                + "recommended aggregate index or table index. Please delete this dimension from the above first.");
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testModifyCCExistInTableIndex() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        var request = newSemanticRequest(modelId);
        val originPlan = indexPlanManager.getIndexPlan(modelId);
        val nest5 = request.getColumnIdByColumnName("TEST_KYLIN_FACT.NEST5");
        val transId = request.getColumnIdByColumnName("TEST_KYLIN_FACT.TRANS_ID");
        val siteName = request.getColumnIdByColumnName("TEST_SITES.SITE_NAME");
        val indexCol = Arrays.asList(nest5, transId, siteName);
        // old indexes
        val oldIndexId = originPlan.getAllLayouts().stream().filter(l -> l.getColOrder().containsAll(indexCol))
                .findFirst().map(LayoutEntity::getId).orElse(-1L);
        // modify expression of cc TEST_KYLIN_FACT.NEST5
        val originCC = request.getComputedColumnDescs().stream().filter(c -> ("NEST5").equals(c.getColumnName()))
                .findFirst().orElse(null);
        originCC.setExpression(originCC.getExpression() + "+1");
        modelService.updateDataModelSemantic(getProject(), request);
        // new indexes
        val newPlan = indexPlanManager.getIndexPlan(modelId);
        val newIndexId = newPlan.getAllLayouts().stream().filter(l -> l.getColOrder().containsAll(indexCol)).findFirst()
                .map(LayoutEntity::getId).orElse(-2L);
        Assert.assertTrue(newIndexId > oldIndexId);
    }

    @Test
    public void testModifyCCExistInAggIndex() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        // create measure
        var request = newSemanticRequest(modelId);
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("NEST5_SUM");
        newMeasure1.setExpression("SUM");
        val param = new ParameterResponse();
        param.setType("column");
        param.setValue("TEST_KYLIN_FACT.NEST5");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        newMeasure1.setReturnType("decimal(38, 0)");
        modelService.updateDataModelSemantic(getProject(), request);
        // get dim and measure id
        request = newSemanticRequest(modelId);
        val transId = request.getColumnIdByColumnName("TEST_KYLIN_FACT.TRANS_ID");
        val nest5SumId = request.getSimplifiedMeasures().stream().filter(m -> "NEST5_SUM".equals(m.getName()))
                .mapToInt(SimplifiedMeasure::getId).findFirst().orElse(-1);
        // create agg group
        NAggregationGroup newAggregationGroup = new NAggregationGroup();
        newAggregationGroup.setIncludes(new Integer[] { transId });
        newAggregationGroup.setMeasures(new Integer[] { nest5SumId });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        newAggregationGroup.setSelectRule(selectRule);
        indexPlanService.updateRuleBasedCuboid(getProject(),
                UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(modelId)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(newAggregationGroup)).build());
        // old indexes
        val indexCol = Arrays.asList(transId, nest5SumId);
        val oldIndexId = indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream()
                .filter(l -> l.getColOrder().containsAll(indexCol)).findFirst().map(LayoutEntity::getId).orElse(-1L);
        // modify expression of cc TEST_KYLIN_FACT.NEST5
        val originCC = request.getComputedColumnDescs().stream().filter(c -> ("NEST5").equals(c.getColumnName()))
                .findFirst().orElse(null);
        originCC.setExpression(originCC.getExpression() + "+1");
        modelService.updateDataModelSemantic(getProject(), request);
        // new indexes
        val newIndexId = indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream()
                .filter(l -> l.getColOrder().containsAll(indexCol)).findFirst().map(LayoutEntity::getId).orElse(-2L);
        Assert.assertTrue(newIndexId > oldIndexId);
    }

    @Test
    public void testModifyCCChangeType() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        // create measure
        var request = newSemanticRequest(modelId);
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("NEST5_SUM");
        newMeasure1.setExpression("SUM");
        val param = new ParameterResponse();
        param.setType("column");
        param.setValue("TEST_KYLIN_FACT.NEST5");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        // return type for SUM(decimal) is "decimal(38, 0)"
        // this will trigger measure return type change
        newMeasure1.setReturnType("any");
        modelService.updateDataModelSemantic(getProject(), request);
        // get dim and measure id
        request = newSemanticRequest(modelId);
        val transId = request.getColumnIdByColumnName("TEST_KYLIN_FACT.TRANS_ID");
        val nest5SumId = request.getSimplifiedMeasures().stream().filter(m -> "NEST5_SUM".equals(m.getName()))
                .mapToInt(SimplifiedMeasure::getId).findFirst().orElse(-1);
        // create agg group
        NAggregationGroup newAggregationGroup = new NAggregationGroup();
        newAggregationGroup.setIncludes(new Integer[] { transId });
        newAggregationGroup.setMeasures(new Integer[] { nest5SumId });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        newAggregationGroup.setSelectRule(selectRule);
        indexPlanService.updateRuleBasedCuboid(getProject(),
                UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(modelId)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(newAggregationGroup)).build());
        // old indexes
        val indexCol = Arrays.asList(transId, nest5SumId);
        val oldLayoutId = indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream()
                .filter(l -> l.getColOrder().containsAll(indexCol)).findFirst().map(LayoutEntity::getId).orElse(-1L);
        // modify expression of cc TEST_KYLIN_FACT.NEST5
        val originCC = request.getComputedColumnDescs().stream().filter(c -> ("NEST5").equals(c.getColumnName()))
                .findFirst().orElse(null);
        originCC.setExpression(originCC.getExpression() + "+1");
        modelService.updateDataModelSemantic(getProject(), request);
        // measure NEST5_SUM is reloaded due to return type change
        val newNest5SumId = nest5SumId + 1;
        val newIndexCol = Arrays.asList(transId, newNest5SumId);
        // new layout id
        val newLayoutId = indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream()
                .filter(l -> l.getColOrder().containsAll(newIndexCol)).findFirst().map(LayoutEntity::getId).orElse(-2L);
        Assert.assertTrue(newLayoutId > oldLayoutId);
    }

    @Test
    public void testModifyCCMeasureInvalid() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        // create measure
        var request = newSemanticRequest(modelId);
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("NEST5_SUM");
        newMeasure1.setExpression("SUM");
        val param = new ParameterResponse();
        param.setType("column");
        param.setValue("TEST_KYLIN_FACT.NEST5");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        // return type for SUM(decimal) is "decimal(38, 0)"
        newMeasure1.setReturnType("decimal(38, 0)");
        modelService.updateDataModelSemantic(getProject(), request);
        // get dim and measure id
        request = newSemanticRequest(modelId);
        val transId = request.getColumnIdByColumnName("TEST_KYLIN_FACT.TRANS_ID");
        val nest5SumId = request.getSimplifiedMeasures().stream().filter(m -> "NEST5_SUM".equals(m.getName()))
                .mapToInt(SimplifiedMeasure::getId).findFirst().orElse(-1);
        // create agg group
        NAggregationGroup newAggregationGroup = new NAggregationGroup();
        newAggregationGroup.setIncludes(new Integer[] { transId });
        newAggregationGroup.setMeasures(new Integer[] { nest5SumId });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        newAggregationGroup.setSelectRule(selectRule);
        indexPlanService.updateRuleBasedCuboid(getProject(),
                UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(modelId)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(newAggregationGroup)).build());
        // old indexes
        val indexCol = Arrays.asList(transId, nest5SumId);
        val oldLayoutId = indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream()
                .filter(l -> l.getColOrder().containsAll(indexCol)).findFirst().map(LayoutEntity::getId).orElse(-1L);
        // modify expression of cc TEST_KYLIN_FACT.NEST5
        val originCC = request.getComputedColumnDescs().stream().filter(c -> ("NEST5").equals(c.getColumnName()))
                .findFirst().orElse(null);
        originCC.setExpression("'now im a varchar'");
        originCC.setInnerExpression("'now im a varchar'");
        originCC.setDatatype("VARCHAR");
        try {
            modelService.updateDataModelSemantic(getProject(), request);
        } catch (KylinException e) {
            Assert.assertEquals("Cannot init measure NEST5_SUM: Invalid column type varchar(4096) for measure SUM", e.getMessage());
        }
    }

    @Test
    public void testModifyCCExistInNestedCC() throws Exception {
        // add nested cc
        final String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var request = newSemanticRequest(modelId);
        ComputedColumnDesc newCC = new ComputedColumnDesc();
        newCC.setColumnName("NEST6");
        newCC.setExpression("TEST_KYLIN_FACT.NEST5+1");
        newCC.setDatatype("decimal(34,0)");
        newCC.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        newCC.setTableAlias("TEST_KYLIN_FACT");
        request.getComputedColumnDescs().add(newCC);
        modelService.updateDataModelSemantic(getProject(), request);
        // expect a KylinException when modify cc used by a nested cc
        NDataModel modelDesc = getTestModel();
        thrown.expect(KylinException.class);
        thrown.expectMessage("Operation failed. There is a nested computed column [TEST_KYLIN_FACT.NEST6] "
                + "in the current model depends on the current computed column [TEST_KYLIN_FACT.NEST5]. "
                + "The expression of nested computed column is [TEST_KYLIN_FACT.NEST5+1].");
        modelService.checkComputedColumn(modelDesc, getProject(), "TEST_KYLIN_FACT.NEST5");
    }

    @Test
    public void testCreateModelWithMultipleMeasures() throws Exception {
        val request = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_multi_measures.json"),
                ModelRequest.class);
        val model = modelService.createModel(request.getProject(), request);
        Assert.assertEquals(3, model.getEffectiveMeasures().size());
        Assert.assertThat(
                model.getEffectiveMeasures().values().stream().map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("SUM_PRICE", "MAX_COUNT", "COUNT_ALL")));
    }

    @Test
    public void testRemoveDimensionsWithCubePlanRule() throws Exception {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The dimension TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP is being referenced by "
                + "aggregation group, recommended aggregate index or table index. Please delete this dimension from the above first.");
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indePlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", cubeBasic -> {
            val rule = new RuleBasedIndex();
            rule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 26));
            rule.setMeasures(Arrays.asList(100001, 100002, 100003));
            cubeBasic.setRuleBasedIndex(rule);
        });
        val request = newSemanticRequest();
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream()
                .filter(c -> c.getId() != 26 && c.isExist()).collect(Collectors.toList()));
        modelService.updateDataModelSemantic(getProject(), request);
    }

    @Test
    public void testChangeJoinType() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_ID, model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("inner");
        });
        val cube = dfMgr.getDataflow(originModel.getUuid()).getIndexPlan();
        val tableIndexCount = cube.getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count();
        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, null, null);
        val executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);

        Assert.assertEquals(tableIndexCount,
                cube.getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count());
    }

    @Test
    public void testChangePartitionDesc() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();
        val cube = dfMgr.getDataflow(originModel.getUuid()).getIndexPlan();
        val tableIndexCount = cube.getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count();
        val ids = cube.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());

        modelMgr.updateDataModel(MODEL_ID, model -> {
            val partitionDesc = model.getPartitionDesc();
            partitionDesc.setCubePartitionType(PartitionDesc.PartitionType.UPDATE_INSERT);
        });
        semanticService.handleSemanticUpdate(getProject(), originModel.getUuid(), originModel, null, null);

        val df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(0, df.getSegments().size());
        Assert.assertEquals(tableIndexCount,
                df.getIndexPlan().getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count());
        Assert.assertEquals(df.getStatus(), RealizationStatusEnum.OFFLINE);
    }

    @Test
    public void testChangeParititionDesc_OneToNull() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();
        val cube = dfMgr.getDataflow(originModel.getUuid()).getIndexPlan();
        val tableIndexCount = cube.getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count();

        modelMgr.updateDataModel(MODEL_ID, model -> {
            model.setPartitionDesc(null);
        });
        semanticService.handleSemanticUpdate(getProject(), originModel.getUuid(), originModel, null, null);

        val executables = getRunningExecutables(getProject(), MODEL_ID);
        Assert.assertEquals(1, executables.size());
        val df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertEquals(tableIndexCount,
                df.getIndexPlan().getAllLayouts().stream().filter(l -> l.getIndex().isTableIndex()).count());
    }

    @Test
    public void testChangePartitionDesc_NullToOne() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelMgr.updateDataModel(MODEL_ID, model -> model.setPartitionDesc(null));

        val originModel = modelMgr.getDataModelDesc(MODEL_ID);

        modelMgr.updateDataModel(MODEL_ID, model -> {
            val partition = new PartitionDesc();
            partition.setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT");
            partition.setPartitionDateFormat("yyyy-MM-dd");
            model.setPartitionDesc(partition);
        });

        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, "1325347200000", "1388505600000");

        val executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(1, executables.size());

        val df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(1, df.getSegments().size());

        val segment = df.getSegments().get(0);
        Assert.assertEquals(1325347200000L, segment.getTSRange().getStart());
        Assert.assertEquals(1388505600000L, segment.getTSRange().getEnd());
    }

    @Test
    public void testChangePartitionDesc_NullToOneWithNoDateRange() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelMgr.updateDataModel(MODEL_ID, model -> model.setPartitionDesc(null));

        val originModel = modelMgr.getDataModelDesc(MODEL_ID);

        modelMgr.updateDataModel(MODEL_ID, model -> {
            val partition = new PartitionDesc();
            partition.setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT");
            partition.setPartitionDateFormat("yyyy-MM-dd");
            model.setPartitionDesc(partition);
        });

        semanticService.handleSemanticUpdate(getProject(), originModel.getUuid(), originModel, null, null);

        val executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(0, executables.size());
        val df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(0, df.getSegments().size());
    }

    @Test
    public void testChangePartitionDesc_ChangePartitionColumn() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();

        modelMgr.updateDataModel(MODEL_ID, model -> {
            val partition = new PartitionDesc();
            partition.setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.TRANS_ID");
            partition.setPartitionDateFormat("yyyy-MM-dd");
            model.setPartitionDesc(partition);
        });

        var df = dfMgr.getDataflow(MODEL_ID);
        Assert.assertEquals(1, df.getSegments().size());

        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, null, null);

        val executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());
        df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(0, df.getSegments().size());
    }

    @Test
    public void testChangePartitionDesc_ChangePartitionColumn_WithDateRange() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();

        modelMgr.updateDataModel(MODEL_ID, model -> {
            val partition = new PartitionDesc();
            partition.setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.TRANS_ID");
            partition.setPartitionDateFormat("yyyy-MM-dd");
            model.setPartitionDesc(partition);
        });

        var df = dfMgr.getDataflow(MODEL_ID);
        Assert.assertEquals(1, df.getSegments().size());

        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, "1325347200000", "1388505600000");

        val executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(1, executables.size());
        df = dfMgr.getDataflow(MODEL_ID);

        Assert.assertEquals(1, df.getSegments().size());
        val segment = df.getSegments().get(0);

        Assert.assertEquals(1325347200000L, segment.getSegRange().getStart());
        Assert.assertEquals(1388505600000L, segment.getSegRange().getEnd());
    }

    @Test
    public void testOnlyAddDimensions() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_ID,
                model -> model.setAllNamedColumns(model.getAllNamedColumns().stream().peek(c -> {
                    if (!c.isExist()) {
                        return;
                    }
                    c.setStatus(NDataModel.ColumnStatus.DIMENSION);
                }).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, null, null);
        val executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testOnlyChangeMeasures() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_ID, model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
            if (m.getId() == 100011) {
                m.setId(100018);
            }
        }).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, null, null);

        var executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(0, executables.size());

        indePlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            val rule = new RuleBasedIndex();
            rule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            rule.setMeasures(Arrays.asList(100000, 100001));
            val aggGroup = new NAggregationGroup();
            aggGroup.setIncludes(new Integer[] { 1, 2, 3, 4, 5, 6 });
            aggGroup.setMeasures(new Integer[] { 100000, 100001 });
            val selectRule = new SelectRule();
            selectRule.mandatoryDims = new Integer[0];
            selectRule.hierarchyDims = new Integer[0][0];
            selectRule.jointDims = new Integer[0][0];
            aggGroup.setSelectRule(selectRule);
            rule.setAggregationGroups(Lists.newArrayList(aggGroup));
            copyForWrite.setRuleBasedIndex(rule);
        });
        semanticService.handleSemanticUpdate(getProject(), MODEL_ID, originModel, null, null);

        executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());

        val cube = indePlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        for (LayoutEntity layout : cube.getWhitelistLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(100011));
            Assert.assertTrue(!layout.getIndex().getMeasures().contains(100011));
        }
    }

    @Test
    public void testOnlyChangeMeasuresWithRule() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getUuid(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.getId() == 100017) {
                        m.setId(100018);
                    }
                }).collect(Collectors.toList())));

        semanticService.handleSemanticUpdate(getProject(), originModel.getUuid(), originModel, null, null);

        val cube = indePlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        for (LayoutEntity layout : cube.getWhitelistLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(100017));
            Assert.assertTrue(!layout.getIndex().getMeasures().contains(100017));
        }
        val newRule = cube.getRuleBasedIndex();
        Assert.assertTrue(!newRule.getMeasures().contains(100017));
    }

    @Test
    public void testAllChanged() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getUuid(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.getId() == 100011) {
                        m.setId(100017);
                    }
                }).collect(Collectors.toList())));
        modelMgr.updateDataModel(originModel.getUuid(), model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("left");
        });
        modelMgr.updateDataModel(originModel.getUuid(),
                model -> model.setAllNamedColumns(model.getAllNamedColumns().stream().peek(c -> {
                    if (!c.isExist()) {
                        return;
                    }
                    c.setStatus(NDataModel.ColumnStatus.DIMENSION);
                    if (c.getId() == 26) {
                        c.setStatus(NDataModel.ColumnStatus.EXIST);
                    }
                }).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate(getProject(), originModel.getUuid(), originModel, null, null);

        val executables = getRunningExecutables(getProject(), null);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);

        val cube = indePlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        for (LayoutEntity layout : cube.getWhitelistLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(100011));
            Assert.assertTrue(!layout.getIndex().getMeasures().contains(100011));
        }
    }

    @Test
    public void testOnlyRuleChanged() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val df = dfMgr.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val originSegLayoutSize = df.getSegments().get(0).getLayoutsMap().size();
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        val cube = df.getIndexPlan();
        val nc1 = NDataLayout.newDataLayout(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseLayouts().get(0).getId());
        val nc2 = NDataLayout.newDataLayout(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseLayouts().get(1).getId());
        val nc3 = NDataLayout.newDataLayout(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseLayouts().get(2).getId());
        update.setToAddOrUpdateLayouts(nc1, nc2, nc3);
        dfMgr.updateDataflow(update);

        val newCube = indexPlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            newRule.setMeasures(Arrays.asList(100001, 100002));
            copyForWrite.setRuleBasedIndex(newRule);
        });
        semanticService.handleIndexPlanUpdateRule(getProject(), df.getModel().getUuid(), cube.getRuleBasedIndex(),
                newCube.getRuleBasedIndex(), false);

        val executables = getRunningExecutables(getProject(), "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);

        val df2 = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(df.getUuid());
        Assert.assertEquals(originSegLayoutSize, df2.getFirstSegment().getLayoutsMap().size());
    }

    @Test
    public void testOnlyRemoveColumns_removeToBeDeletedIndex() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());

        val indexPlan = indexPlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val originModel = getTestInnerModel();

        NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(indexPlan.getUuid(),
                copyForWrite -> {
                    val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts)
                            .flatMap(List::stream).filter(layoutEntity -> 20000020001L == layoutEntity.getId())
                            .collect(Collectors.toSet());
                    copyForWrite.markIndexesToBeDeleted(copyForWrite.getUuid(), toBeDeletedSet);
                    copyForWrite.removeLayouts(Sets.newHashSet(20000020001L), true, true);
                });

        modelManager.updateDataModel(originModel.getUuid(), model -> model.setAllNamedColumns(
                model.getAllNamedColumns().stream().filter(m -> m.getId() != 25).collect(Collectors.toList())));

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NIndexPlanManager.getInstance(getTestConfig(), getProject()).updateIndexPlan(dataflow.getUuid(),
                copyForWrite -> {
                    val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts)
                            .flatMap(List::stream).filter(layoutEntity -> 1000001L == layoutEntity.getId())
                            .collect(Collectors.toSet());
                    copyForWrite.markIndexesToBeDeleted(dataflow.getUuid(), toBeDeletedSet);
                });
        Assert.assertTrue(CollectionUtils.isNotEmpty(
                indexPlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getToBeDeletedIndexes()));
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), k -> {
            val newDim = k.getRuleBasedIndex().getDimensions().stream().filter(x -> x != 25)
                    .collect(Collectors.toList());
            k.getRuleBasedIndex().setDimensions(newDim);
            List<NAggregationGroup> aggs = new ArrayList<>();
            for (val agg : k.getRuleBasedIndex().getAggregationGroups()) {
                val newMeasure = Arrays.stream(agg.getIncludes()).filter(x -> x != 25).toArray(Integer[]::new);
                agg.setMeasures(newMeasure);
            }
            k.getRuleBasedIndex().setAggregationGroups(aggs);
        });
        semanticService.handleSemanticUpdate(getProject(), indexPlan.getUuid(), originModel, null, null);
        Assert.assertTrue(CollectionUtils.isEmpty(
                indexPlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getToBeDeletedIndexes()));
    }

    @Test
    public void testOnlyRemoveMeasures() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());

        val indexPlan = indexPlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val originModel = getTestInnerModel();

        indexPlanManager.updateIndexPlan(indexPlan.getId(), k -> {
            List<NAggregationGroup> aggs = new ArrayList<>();
            for (val agg : indexPlan.getRuleBasedIndex().getAggregationGroups()) {
                val newMeasure = Arrays.stream(agg.getMeasures()).filter(x -> x != 100001 && x != 100002 && x != 100011)
                        .toArray(Integer[]::new);
                agg.setMeasures(newMeasure);
                aggs.add(agg);
            }
            k.getRuleBasedIndex().setAggregationGroups(aggs);
        });

        modelManager.updateDataModel(originModel.getUuid(),
                model -> model.setAllMeasures(model.getAllMeasures().stream()
                        .filter(m -> m.getId() != 100002 && m.getId() != 100001 && m.getId() != 100011)
                        .collect(Collectors.toList())));
        semanticService.handleSemanticUpdate(getProject(), indexPlan.getUuid(), originModel, null, null);

        val executables = getRunningExecutables(getProject(), "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(1, executables.size());

        val newCube = indexPlanManager.getIndexPlan(indexPlan.getUuid());
        Assert.assertNotEquals(indexPlan.getRuleBasedIndex().getLayoutIdMapping().toString(),
                newCube.getRuleBasedIndex().getLayoutIdMapping().toString());
    }

    @Test
    public void testSetBlackListLayout() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val indexPlan = indexPlanManager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        val dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");

        val dfUpdate = new NDataflowUpdate(dataflow.getUuid());
        List<NDataLayout> layouts = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            val layout1 = new NDataLayout();
            layout1.setLayoutId(indexPlan.getRuleBaseLayouts().get(i).getId());
            layout1.setRows(100);
            layout1.setByteSize(100);
            layout1.setSegDetails(dataflow.getSegments().getLatestReadySegment().getSegDetails());
            layouts.add(layout1);
        }
        dfUpdate.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        dataflowManager.updateDataflow(dfUpdate);

        val blacklist2 = Lists.newArrayList(indexPlan.getRuleBaseLayouts().get(1).getId());
        var updatedPlan = semanticService.addRuleBasedIndexBlackListLayouts(indexPlan, blacklist2);
        Assert.assertEquals(updatedPlan.getAllLayouts().size() + 1, indexPlan.getAllLayouts().size());
        val df2 = dataflowManager.getDataflow(dataflow.getId());
        for (Long bId : blacklist2) {
            Assert.assertFalse(df2.getLastSegment().getLayoutsMap().containsKey(bId));
        }

        val blacklist3 = Lists.newArrayList(indexPlan.getRuleBaseLayouts().get(2).getId());
        updatedPlan = semanticService.addRuleBasedIndexBlackListLayouts(indexPlan, blacklist3);
        Assert.assertEquals(updatedPlan.getAllLayouts().size() + 2, indexPlan.getAllLayouts().size());
        val df3 = dataflowManager.getDataflow(dataflow.getId());
        for (Long bId : blacklist3) {
            Assert.assertFalse(df3.getLastSegment().getLayoutsMap().containsKey(bId));
        }

        // add layout to blacklist which is auto and manual, will not remove datalayout from segment
        val blacklist4 = Lists.newArrayList(indexPlan.getRuleBaseLayouts().get(0).getId());
        updatedPlan = semanticService.addRuleBasedIndexBlackListLayouts(indexPlan, blacklist4);
        Assert.assertEquals(updatedPlan.getAllLayouts().size() + 2, indexPlan.getAllLayouts().size());
        val df4 = dataflowManager.getDataflow(dataflow.getId());
        for (Long bId : blacklist4) {
            Assert.assertTrue(df4.getLastSegment().getLayoutsMap().containsKey(bId));
        }
    }

    @Test
    public void testSCD2ModelWithAlias() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "scd2");
        val model = modelMgr.getDataModelDescByAlias("same_scd2_dim_tables");
        val req = newSemanticRequest(model.getId(), "scd2");
        val modelFromReq = modelService.convertToDataModel(req);
        Assert.assertEquals(2, modelFromReq.getJoinTables().size());
        val join = modelFromReq.getJoinTables().get(1).getAlias().equalsIgnoreCase("TEST_SCD2_1") ?
                modelFromReq.getJoinTables().get(1) : modelFromReq.getJoinTables().get(0);
        Assert.assertEquals("TEST_SCD2_1", join.getAlias());
        Assert.assertEquals("\"TEST_KYLIN_FACT\".\"SELLER_ID\" = \"TEST_SCD2_1\".\"BUYER_ID\" " +
                "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" >= \"TEST_SCD2_1\".\"START_DATE\" " +
                "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" < \"TEST_SCD2_1\".\"END_DATE\"",
                join.getJoin().getNonEquiJoinCondition().getExpr());
    }


    private NDataModel getTestInnerModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelMgr.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        return model;
    }

    private NDataModel getTestBasicModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        return model;
    }

    private ModelRequest changeAlias(ModelRequest request, String old, String newAlias) throws IOException {
        val newRequest = JsonUtil.deepCopy(request, ModelRequest.class);
        Function<String, String> replaceTableName = col -> {
            if (col.startsWith(old)) {
                return col.replace(old, newAlias);
            } else {
                return col;
            }
        };
        newRequest.getJoinTables().forEach(join -> {
            if (join.getAlias().equals(old)) {
                join.setAlias(newAlias);
            }
            join.getJoin().setForeignKey(
                    Stream.of(join.getJoin().getForeignKey()).map(replaceTableName).toArray(String[]::new));
            join.getJoin().setPrimaryKey(
                    Stream.of(join.getJoin().getPrimaryKey()).map(replaceTableName).toArray(String[]::new));
        });
        newRequest.setSimplifiedDimensions(request.getAllNamedColumns().stream().filter(NamedColumn::isDimension)
                .peek(nc -> nc.setAliasDotColumn(replaceTableName.apply(nc.getAliasDotColumn())))
                .collect(Collectors.toList()));
        newRequest.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(newRequest.getJoinTables()));
        return newRequest;
    }

    private ModelRequest newSemanticRequest() throws Exception {
        return newSemanticRequest("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    private ModelRequest newSemanticRequest(String modelId) throws Exception {
        return newSemanticRequest(modelId, getProject());
    }

    private ModelRequest newSemanticRequest(String modelId, String project) throws Exception {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelMgr.getDataModelDesc(modelId);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(project);
        request.setUuid(modelId);
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(model.getJoinTables()));

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private NDataModel getTestModel() {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        return model;
    }

    @Test
    public void testIsFilterConditonNotChange() {
        Assert.assertTrue(semanticService.isFilterConditonNotChange(null, null));
        Assert.assertTrue(semanticService.isFilterConditonNotChange("", null));
        Assert.assertTrue(semanticService.isFilterConditonNotChange(null, "    "));
        Assert.assertTrue(semanticService.isFilterConditonNotChange("  ", ""));
        Assert.assertTrue(semanticService.isFilterConditonNotChange("", "         "));
        Assert.assertTrue(semanticService.isFilterConditonNotChange("A=8", " A=8   "));

        Assert.assertFalse(semanticService.isFilterConditonNotChange(null, "null"));
        Assert.assertFalse(semanticService.isFilterConditonNotChange("", "null"));
        Assert.assertFalse(semanticService.isFilterConditonNotChange("A=8", "A=9"));
    }

    @Test
    public void testUpdateDataModelParatitionDesc() {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        var model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertNotNull(model.getPartitionDesc());
        ModelParatitionDescRequest modelParatitionDescRequest = new ModelParatitionDescRequest();
        modelParatitionDescRequest.setStart("0");
        modelParatitionDescRequest.setEnd("1111");
        modelParatitionDescRequest.setPartitionDesc(null);
        PartitionDesc partitionDesc = model.getPartitionDesc();

        var executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());

        modelService.updateDataModelParatitionDesc(getProject(), model.getAlias(), modelParatitionDescRequest);
        model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertNull(model.getPartitionDesc());
        executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
        modelParatitionDescRequest.setPartitionDesc(partitionDesc);

        deleteJobByForce(executables.get(0));
        modelService.updateDataModelParatitionDesc(getProject(), model.getAlias(), modelParatitionDescRequest);
        model = modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(partitionDesc, model.getPartitionDesc());
        executables = getRunningExecutables(getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
    }

}
