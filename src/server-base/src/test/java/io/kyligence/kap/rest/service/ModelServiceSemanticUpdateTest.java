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

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
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
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.ColumnStatus;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import lombok.val;
import lombok.var;

public class ModelServiceSemanticUpdateTest extends NLocalFileMetadataTestCase {

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
        modelService.updateDataModelSemantic(request);

        val model = getTestModel();
        Assert.assertEquals("GMV_AVG", model.getEffectiveMeasureMap().get(1017).getName());
        Assert.assertNull(model.getEffectiveMeasureMap().get(1002));
        Assert.assertNull(model.getEffectiveMeasureMap().get(1003));
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(1, eventDao.getEvents().size());
    }

    @Test
    public void testModelUpdateMeasureName() throws Exception {
        val request = newSemanticRequest();
        request.getSimplifiedMeasures().get(0).setName("NEW_MEASURE");
        val originId = request.getSimplifiedMeasures().get(0).getId();
        modelService.updateDataModelSemantic(request);

        val model = getTestModel();
        Assert.assertEquals("NEW_MEASURE", model.getEffectiveMeasureMap().get(originId).getName());
    }

    @Test
    public void testRenameTableAlias() throws Exception {
        var request = newSemanticRequest();
        val OLD_ALIAS = "TEST_ORDER";
        val NEW_ALIAS = "NEW_ALIAS";
        val colCount = request.getAllNamedColumns().stream().filter(n -> n.aliasDotColumn.startsWith("TEST_ORDER"))
                .count();
        request = changeAlias(request, OLD_ALIAS, NEW_ALIAS);
        modelService.updateDataModelSemantic(request);

        val model = getTestModel();
        val tombCount = model.getAllNamedColumns().stream().filter(n -> n.aliasDotColumn.startsWith("TEST_ORDER"))
                .peek(col -> {
                    Assert.assertEquals(ColumnStatus.TOMB, col.status);
                }).count();
        Assert.assertEquals(colCount, tombCount);
        val otherTombCount = model.getAllNamedColumns().stream().filter(n -> !n.aliasDotColumn.startsWith("TEST_ORDER"))
                .filter(nc -> !nc.isExist()).count();
        Assert.assertEquals(1, otherTombCount);
        Assert.assertEquals(203, model.getAllNamedColumns().size());
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(1, eventDao.getEvents().size());
    }

    @Test
    public void testRenameTableAliasUsedAsMeasure() throws Exception {
        val request = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure.json"), ModelRequest.class);
        modelService.createModel(request);

        val updateRequest = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias.json"),
                ModelRequest.class);
        modelService.updateDataModelSemantic(updateRequest);

        var model = modelService.getDataModelManager("default").getDataModelDesc(request.getName());
        Assert.assertThat(
                model.getAllMeasures().stream().filter(m -> !m.tomb).sorted(Comparator.comparing(k -> k.id))
                        .map(MeasureDesc::getName).collect(Collectors.toList()),
                CoreMatchers.is(Lists.newArrayList("COUNT_ALL", "MAX1")));

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfMgr.getDataflowByModelName(model.getName());
        dfMgr.updateDataflow(df.getName(), copyForWrite -> {
            copyForWrite.setReconstructing(false);
        });

        // make sure update again is ok
        val updateRequest2 = JsonUtil.readValue(
                getClass().getResourceAsStream("/ut_request/model_update/model_with_measure_change_alias_twice.json"),
                ModelRequest.class);
        modelService.updateDataModelSemantic(updateRequest2);
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
        request.setAllNamedColumns(request.getAllNamedColumns().stream().filter(c -> c.isDimension() && c.id != 25)
                .collect(Collectors.toList()));
        val newCol = new NDataModel.NamedColumn();
        newCol.name = "PRICE2";
        newCol.aliasDotColumn = "TEST_KYLIN_FACT.PRICE";
        newCol.status = NDataModel.ColumnStatus.DIMENSION;
        request.getAllNamedColumns().add(newCol);
        ComputedColumnDesc ccDesc = request.getComputedColumnDescs().stream()
                .filter(cc -> "DEAL_YEAR".equals(cc.getColumnName())).findFirst().orElse(null);
        Assert.assertNotNull(ccDesc);
        NamedColumn ccCol = request.getAllNamedColumns().stream()
                .filter(c -> c.aliasDotColumn.equals(ccDesc.getFullName())).findFirst().orElse(null);
        Assert.assertNotNull(ccCol);
        Assert.assertTrue(ccCol.status == ColumnStatus.DIMENSION);
        int ccColId = ccCol.getId();
        request.getComputedColumnDescs().remove(ccDesc);

        val prevId = getTestModel().getAllNamedColumns().stream()
                .filter(n -> n.aliasDotColumn.equals(newCol.aliasDotColumn)).findFirst().map(n -> n.id).orElse(0);
        modelService.updateDataModelSemantic(request);

        val model = getTestModel();
        Assert.assertEquals(newCol.name, model.getNameByColumnId(prevId));
        Assert.assertNull(model.getEffectiveDimenionsMap().get(25));
        Assert.assertFalse(model.getComputedColumnNames().contains("DEAL_YEAR"));
        Assert.assertNull(model.getEffectiveDimenionsMap().get(ccColId));
        Assert.assertNull(model.getEffectiveColsMap().get(ccColId));
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(1, eventDao.getEvents().size());

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        dfMgr.updateDataflow(dfMgr.getDataflowByModelName(request.getName()).getName(),
                copyForWrite -> copyForWrite.setReconstructing(false));

        newCol.name = "PRICE3";
        request.getComputedColumnDescs().add(ccDesc);
        modelService.updateDataModelSemantic(request);
        val model2 = getTestModel();
        Assert.assertEquals(newCol.name, model2.getNameByColumnId(prevId));
        Assert.assertTrue(model2.getComputedColumnNames().contains("DEAL_YEAR"));
        NamedColumn newCcCol = model2.getAllNamedColumns().stream()
                .filter(c -> c.aliasDotColumn.equals(ccDesc.getFullName())).filter(c -> c.isExist()).findFirst()
                .orElse(null);
        Assert.assertNotNull(newCcCol);
        Assert.assertNotEquals(ccColId, newCcCol.id);
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
                request.getAllNamedColumns().stream().filter(c -> c.id != 26).collect(Collectors.toList()));
        modelService.updateDataModelSemantic(request);
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
                .peek(nc -> nc.aliasDotColumn = replaceTableName.apply(nc.aliasDotColumn))
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
