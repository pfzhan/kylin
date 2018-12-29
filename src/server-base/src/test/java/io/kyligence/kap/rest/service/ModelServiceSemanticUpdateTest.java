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
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.constant.Constant;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelServiceSemanticUpdateTest extends NLocalFileMetadataTestCase {

    private static final String MODEL_NAME = "nmodel_basic";

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        modelService.setSemanticUpdater(semanticService);
        val projectManager = NProjectManager.getInstance(getTestConfig());
        val projectInstance = projectManager.copyForWrite(projectManager.getProject("default"));
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstance);
    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testModelUpdateComputedColumn() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");

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
            Assert.assertEquals(-1, request.getColumnIdByColumnName("TEST_KYLIN_FACT.TEST_CC_1"));
            NamedColumn newDimension = new NamedColumn();
            newDimension.setName("TEST_DIM_WITH_CC");
            newDimension.setAliasDotColumn("TEST_KYLIN_FACT.TEST_CC_1");
            newDimension.setStatus(ColumnStatus.DIMENSION);
            request.getAllNamedColumns().add(newDimension);
            modelService.updateDataModelSemantic(request.getProject(), request);

            ModelRequest requestToVerify = newSemanticRequest();
            Assert.assertEquals(colIdOfCC, requestToVerify.getColumnIdByColumnName("TEST_KYLIN_FACT.TEST_CC_1"));
            NamedColumn dimensionToVerify = requestToVerify.getAllNamedColumns().stream()
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
            newMeasure.setReturnType("integer");
            ParameterResponse param = new ParameterResponse();
            param.setType("column");
            param.setValue("TEST_KYLIN_FACT.TEST_CC_1");
            newMeasure.setParameterValue(Lists.newArrayList(param));
            request.getSimplifiedMeasures().add(newMeasure);
            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            Measure measure = model.getAllMeasures().stream().filter(m -> m.getName().equals("TEST_MEASURE_WITH_CC"))
                    .findFirst().get();
            Assert.assertNotNull(measure);
            measureIdOfCC = measure.getId();
            Assert.assertTrue(measure.getFunction().isSum());
            Assert.assertEquals("TEST_KYLIN_FACT.TEST_CC_1", measure.getFunction().getParameter().getValue());
        }

        // Update TEST_CC_1's definition, named column and measure will be recreated
        int newColIdOfCC;
        int newMeasureIdOfCC;
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
            Assert.assertEquals(ColumnStatus.TOMB, originalColumn.getStatus());
            NamedColumn newColumn = model.getAllNamedColumns().stream().filter(NamedColumn::isExist)
                    .filter(col -> col.getAliasDotColumn().equals("TEST_KYLIN_FACT.TEST_CC_1")).findFirst().get();
            Assert.assertNotNull(newColumn);
            Assert.assertEquals("TEST_DIM_WITH_CC", newColumn.getName());
            Assert.assertEquals(ColumnStatus.DIMENSION, newColumn.getStatus());
            newColIdOfCC = newColumn.getId();

            Measure originalMeasure = model.getAllMeasures().stream().filter(m -> m.getId() == measureIdOfCC)
                    .findFirst().get();
            Assert.assertNotNull(originalMeasure);
            Assert.assertEquals("TEST_MEASURE_WITH_CC", originalMeasure.getName());
            Assert.assertTrue(originalMeasure.tomb);
            Measure newMeasure = model.getEffectiveMeasures().values().stream()
                    .filter(m -> m.getName().equals("TEST_MEASURE_WITH_CC")).findFirst().get();
            Assert.assertNotNull(newMeasure);
            Assert.assertFalse(newMeasure.tomb);
            newMeasureIdOfCC = newMeasure.getId();
        }

        // Remove TEST_CC_1, all related should be moved to tomb
        {
            ModelRequest request = newSemanticRequest();
            Assert.assertTrue(request.getComputedColumnDescs().removeIf(cc -> cc.getColumnName().equals("TEST_CC_1")));
            try {
                modelService.updateDataModelSemantic(request.getProject(), request);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalStateException);
                Assert.assertEquals(
                        "Cannot init measure TEST_MEASURE_WITH_CC: Column not found by TEST_KYLIN_FACT.TEST_CC_1",
                        e.getMessage());
            }

            // remove broken measure
            request.getSimplifiedMeasures().removeIf(m -> m.getName().equals("TEST_MEASURE_WITH_CC"));
            modelService.updateDataModelSemantic(request.getProject(), request);

            NDataModel model = getTestModel();
            Assert.assertFalse(model.getAllNamedColumns().stream().filter(c -> c.getId() == newColIdOfCC).findFirst()
                    .get().isExist());
            Assert.assertTrue(
                    model.getAllMeasures().stream().filter(m -> m.getId() == newMeasureIdOfCC).findFirst().get().tomb);
        }
    }

    @Test
    public void testModelUpdateMeasures() throws Exception {
        val request = newSemanticRequest();
        val newMeasure1 = new SimplifiedMeasure();
        newMeasure1.setName("GMV_AVG");
        newMeasure1.setExpression("AVG");
        newMeasure1.setReturnType("bitmap");
        val param = new ParameterResponse();
        param.setType("column");
        param.setValue("TEST_KYLIN_FACT.PRICE");
        newMeasure1.setParameterValue(Lists.newArrayList(param));
        request.getSimplifiedMeasures().add(newMeasure1);
        request.setSimplifiedMeasures(request.getSimplifiedMeasures().stream()
                .filter(m -> m.getId() != 1002 && m.getId() != 1003).collect(Collectors.toList()));
        // add new measure and remove 1002 and 1003
        modelService.updateDataModelSemantic("default", request);

        val model = getTestModel();
        Assert.assertEquals("GMV_AVG", model.getEffectiveMeasureMap().get(1017).getName());
        Assert.assertNull(model.getEffectiveMeasureMap().get(1002));
        Assert.assertNull(model.getEffectiveMeasureMap().get(1003));
    }

    @Test
    public void testModelUpdateMeasureName() throws Exception {
        val request = newSemanticRequest();
        request.getSimplifiedMeasures().get(0).setName("NEW_MEASURE");
        val originId = request.getSimplifiedMeasures().get(0).getId();
        modelService.updateDataModelSemantic("default", request);

        val model = getTestModel();
        Assert.assertEquals("NEW_MEASURE", model.getEffectiveMeasureMap().get(originId).getName());
    }

    @Test
    public void testRenameTableAlias() throws Exception {
        var request = newSemanticRequest();
        val OLD_ALIAS = "TEST_ORDER";
        val NEW_ALIAS = "NEW_ALIAS";
        val colCount = request.getAllNamedColumns().stream().filter(n -> n.getAliasDotColumn().startsWith("TEST_ORDER"))
                .count();
        request = changeAlias(request, OLD_ALIAS, NEW_ALIAS);
        modelService.updateDataModelSemantic("default", request);

        val model = getTestModel();
        val tombCount = model.getAllNamedColumns().stream().filter(n -> n.getAliasDotColumn().startsWith("TEST_ORDER"))
                .peek(col -> {
                    Assert.assertEquals(ColumnStatus.TOMB, col.getStatus());
                }).count();
        Assert.assertEquals(colCount, tombCount);
        val otherTombCount = model.getAllNamedColumns().stream()
                .filter(n -> !n.getAliasDotColumn().startsWith("TEST_ORDER")).filter(nc -> !nc.isExist()).count();
        Assert.assertEquals(1, otherTombCount);
        Assert.assertEquals(203, model.getAllNamedColumns().size());
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(2, eventDao.getEvents().size());
    }

    @Test
    public void testRenameTableAliasUsedAsMeasure() throws Exception {
        val request = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure.json"), ModelRequest.class);
        modelService.createModel(request.getProject(), request);

        val updateRequest = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias.json"),
                ModelRequest.class);
        modelService.updateDataModelSemantic("default", updateRequest);

        var model = modelService.getDataModelManager("default").getDataModelDesc(request.getName());
        Assert.assertThat(
                model.getAllMeasures().stream().filter(m -> !m.tomb).sorted(Comparator.comparing(k -> k.id))
                        .map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("COUNT_ALL", "MAX1")));

        // make sure update again is ok
        val updateRequest2 = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias_twice.json"),
                ModelRequest.class);
        modelService.updateDataModelSemantic("default", updateRequest2);
        model = modelService.getDataModelManager("default").getDataModelDesc(request.getName());
        Assert.assertThat(
                model.getAllMeasures().stream().filter(m -> !m.tomb).sorted(Comparator.comparing(k -> k.id))
                        .map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("COUNT_ALL", "MAX1")));
        Assert.assertEquals(4, model.getAllMeasures().size());
    }

    @Test
    public void testModelUpdateDimensions() throws Exception {
        val request = newSemanticRequest();
        request.setAllNamedColumns(request.getAllNamedColumns().stream().filter(c -> c.isDimension() && c.getId() != 25)
                .collect(Collectors.toList()));
        val newCol = new NDataModel.NamedColumn();
        newCol.setName("PRICE2");
        newCol.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        newCol.setStatus(NDataModel.ColumnStatus.DIMENSION);
        request.getAllNamedColumns().add(newCol);
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
        modelService.updateDataModelSemantic("default", request);

        val model = getTestModel();
        Assert.assertEquals(newCol.getName(), model.getNameByColumnId(prevId));
        Assert.assertNull(model.getEffectiveDimenionsMap().get(25));
        Assert.assertFalse(model.getComputedColumnNames().contains("DEAL_YEAR"));
        Assert.assertNull(model.getEffectiveDimenionsMap().get(ccColId));
        Assert.assertNull(model.getEffectiveColsMap().get(ccColId));

        newCol.setName("PRICE3");
        request.getComputedColumnDescs().add(ccDesc);
        modelService.updateDataModelSemantic("default", request);
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
    public void testCreateModelWithMultipleMeasures() throws IOException {
        val request = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_multi_measures.json"),
                ModelRequest.class);
        modelService.createModel(request.getProject(), request);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelManager.getDataModelDesc("new_model");
        Assert.assertEquals(3, model.getEffectiveMeasureMap().size());
        Assert.assertThat(
                model.getEffectiveMeasureMap().values().stream().map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("SUM_PRICE", "MAX_COUNT", "COUNT_ALL")));
    }

    @Test
    public void testRemoveDimensionsWithCubePlanRule() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(
                "model nmodel_basic's agg group still contains dimensions TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        cubeMgr.updateCubePlan("ncube_basic", cubeBasic -> {
            val rule = new NRuleBasedCuboidsDesc();
            rule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 26));
            rule.setMeasures(Arrays.asList(1001, 1002, 1003));
            cubeBasic.setRuleBasedCuboidsDesc(rule);
        });
        val request = newSemanticRequest();
        request.setAllNamedColumns(
                request.getAllNamedColumns().stream().filter(c -> c.getId() != 26).collect(Collectors.toList()));
        modelService.updateDataModelSemantic("default", request);
    }

    @Test
    public void testChangeJoinType() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("inner");
        });
        semanticService.handleSemanticUpdate("default", MODEL_NAME, originModel);
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);

        log.debug("events are {}", events);
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);
    }

    @Test
    public void testChangePartitionDesc() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        val cube = dfMgr.getDataflowByModelName(originModel.getName()).getCubePlan();
        val ids = cube.getAllCuboidLayouts().stream().map(NCuboidLayout::getId).collect(Collectors.toList());

        modelMgr.updateDataModel(MODEL_NAME, model -> {
            val partitionDesc = model.getPartitionDesc();
            partitionDesc.setCubePartitionType(PartitionDesc.PartitionType.UPDATE_INSERT);
        });
        semanticService.handleSemanticUpdate("default", originModel.getName(), originModel);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);

        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);
        //        val savedIds = ((AddCuboidEvent) events.get(1)).getLayoutIds();
        //        Assert.assertTrue(CollectionUtils.isEqualCollection(savedIds,
        //                Arrays.<Long> asList(1000001L, 1L, 1001L, 1002L, 2001L, 3001L, 20000001001L)));
    }

    @Test
    public void testOnlyAddDimensions() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> model.setAllNamedColumns(model.getAllNamedColumns().stream()
                .peek(c -> c.setStatus(NDataModel.ColumnStatus.DIMENSION)).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate("default", MODEL_NAME, originModel);
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);

        Assert.assertEquals(0, events.size());

    }

    @Test
    public void testOnlyChangeMeasures() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
            if (m.id == 1011) {
                m.id = 1017;
            }
        }).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate("default", MODEL_NAME, originModel);

        var events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        Assert.assertEquals(0, events.size());

        cubeMgr.updateCubePlan("ncube_basic", copyForWrite -> {
            val rule = new NRuleBasedCuboidsDesc();
            rule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            rule.setMeasures(Arrays.asList(1001, 1000));
            copyForWrite.setRuleBasedCuboidsDesc(rule);
        });
        semanticService.handleSemanticUpdate("default", MODEL_NAME, originModel);
        events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        Assert.assertEquals(0, events.size());

        val cube = cubeMgr.getCubePlan("ncube_basic");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1011));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1011));
        }
    }

    @Test
    public void testOnlyChangeMeasuresWithRule() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getName(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.id == 1017) {
                        m.id = 1018;
                    }
                }).collect(Collectors.toList())));

        semanticService.handleSemanticUpdate("default", originModel.getName(), originModel);

        val cube = cubeMgr.getCubePlan("ncube_basic_inner");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1017));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1017));
        }
        val newRule = cube.getRuleBasedCuboidsDesc();
        Assert.assertTrue(!newRule.getMeasures().contains(1017));
    }

    @Test
    public void testAllChanged() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getName(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.id == 1011) {
                        m.id = 1017;
                    }
                }).collect(Collectors.toList())));
        modelMgr.updateDataModel(originModel.getName(), model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("left");
        });
        modelMgr.updateDataModel(originModel.getName(),
                model -> model.setAllNamedColumns(model.getAllNamedColumns().stream().peek(c -> {
                    c.setStatus(NDataModel.ColumnStatus.DIMENSION);
                    if (c.getId() == 26) {
                        c.setStatus(NDataModel.ColumnStatus.EXIST);
                    }
                }).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate("default", originModel.getName(), originModel);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);

        Assert.assertEquals(2, events.size());
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);

        val cube = cubeMgr.getCubePlan("ncube_basic_inner");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1011));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1011));
        }
    }

    @Test
    public void testOnlyRuleChanged() throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfMgr.getDataflow("ncube_basic_inner");
        val originSegLayoutSize = df.getSegments().get(0).getCuboidsMap().size();
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        val cube = df.getCubePlan();
        val nc1 = NDataCuboid.newDataCuboid(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseCuboidLayouts().get(0).getId());
        val nc2 = NDataCuboid.newDataCuboid(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseCuboidLayouts().get(1).getId());
        val nc3 = NDataCuboid.newDataCuboid(df, df.getSegments().get(0).getId(),
                cube.getRuleBaseCuboidLayouts().get(2).getId());
        update.setToAddOrUpdateCuboids(nc1, nc2, nc3);
        dfMgr.updateDataflow(update);

        val newCube = cubePlanManager.updateCubePlan(cube.getName(), copyForWrite -> {
            val newRule = new NRuleBasedCuboidsDesc();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            newRule.setMeasures(Arrays.asList(1001, 1002));
            copyForWrite.setRuleBasedCuboidsDesc(newRule);
        });
        semanticService.handleCubeUpdateRule("default", df.getModel().getName(), cube.getRuleBasedCuboidsDesc(),
                newCube.getRuleBasedCuboidsDesc());

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);

        Assert.assertEquals(2, events.size());
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);
        val df2 = NDataflowManager.getInstance(getTestConfig(), "default").getDataflow(df.getName());
        Assert.assertEquals(originSegLayoutSize, df2.getFirstSegment().getCuboidsMap().size());
    }

    @Test
    public void testOnlyRemoveMeasures() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");

        val cubePlan = cubePlanManager.getCubePlan("ncube_basic_inner");
        val originModel = getTestInnerModel();
        modelManager.updateDataModel(originModel.getName(), model -> model.setAllMeasures(model.getAllMeasures().stream()
                .filter(m -> m.id != 1002 && m.id != 1001 && m.id != 1011).collect(Collectors.toList())));
        semanticService.handleSemanticUpdate("default", cubePlan.getModelName(), originModel);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(0, events.size());

        val newCube = cubePlanManager.getCubePlan(cubePlan.getName());
        Assert.assertNotEquals(cubePlan.getRuleBasedCuboidsDesc().getLayoutIdMapping().toString(),
                newCube.getRuleBasedCuboidsDesc().getLayoutIdMapping().toString());
    }

    private NDataModel getTestInnerModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic_inner");
        return model;
    }

    private NDataModel getTestBasicModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
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
        newRequest.setAllNamedColumns(request.getAllNamedColumns().stream().filter(NamedColumn::isDimension)
                .peek(nc -> nc.setAliasDotColumn(replaceTableName.apply(nc.getAliasDotColumn())))
                .collect(Collectors.toList()));
        return newRequest;
    }

    private ModelRequest newCreateModelRequest() throws Exception {
        val request = newSemanticRequest();
        request.setName("new_model_basic");
        return request;
    }

    private ModelRequest newSemanticRequest() throws Exception {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setName("nmodel_basic");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private NDataModel getTestModel() {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
        return model;
    }
}
