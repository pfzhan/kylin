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

import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.rest.response.ParameterResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.ModelSemanticUpdateRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import lombok.val;

public class ModelServiceSemanticUpdateTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        modelMgr.updateDataModel("nmodel_basic", copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        });
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
        Assert.assertEquals("GMV_AVG", model.getEffectiveMeasureMap().get(1012).getName());
        Assert.assertNull(model.getEffectiveMeasureMap().get(1002));
        Assert.assertNull(model.getEffectiveMeasureMap().get(1003));
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(1, eventDao.getEvents().size());
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
        modelService.updateDataModelSemantic(request);

        val model = getTestModel();
        Assert.assertEquals(newCol.name, model.getNameByColumnId(11));
        Assert.assertNull(model.getEffectiveDimenionsMap().get(25));
        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        Assert.assertEquals(1, eventDao.getEvents().size());

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        dfMgr.updateDataflow(dfMgr.getDataflowByModelName(request.getName()).getName(),  copyForWrite -> copyForWrite.setReconstructing(false));

        newCol.name = "PRICE3";
        modelService.updateDataModelSemantic(request);
        val model2 = getTestModel();
        Assert.assertEquals(newCol.name, model2.getNameByColumnId(11));
    }

    @Test
    public void testRemoveDimensionsWithCubePlanRule() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("model nmodel_basic's agg group still contains dimensions TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP");
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

    private ModelSemanticUpdateRequest newSemanticRequest() throws Exception {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelSemanticUpdateRequest.class);
        request.setProject("default");
        request.setName("nmodel_basic");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelSemanticUpdateRequest.class);
    }

    private NDataModel getTestModel() {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
        return model;
    }
}
