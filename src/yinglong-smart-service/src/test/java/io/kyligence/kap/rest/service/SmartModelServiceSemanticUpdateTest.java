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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import lombok.val;

public class SmartModelServiceSemanticUpdateTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private ModelSmartService modelSmartService = Mockito.spy(new ModelSmartService());

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
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        modelService.setSemanticUpdater(semanticService);
        indexPlanService.setSemanticUpater(semanticService);
        modelService.setIndexPlanService(indexPlanService);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(semanticService, "modelSmartSupporter", modelSmartService);
    }

    @Before
    public void setup() {
        ExecutableUtils.initJobFactory();
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
    public void testSCD2ModelWithAlias() throws Exception {
        getTestConfig().setProperty("kylin.query.non-equi-join-model-enabled", "true");
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "scd2");
        val model = modelMgr.getDataModelDescByAlias("same_scd2_dim_tables");
        val req = newSemanticRequest(model.getId(), "scd2");
        val modelFromReq = modelService.convertToDataModel(req);
        Assert.assertEquals(2, modelFromReq.getJoinTables().size());
        val join = modelFromReq.getJoinTables().get(1).getAlias().equalsIgnoreCase("TEST_SCD2_1")
                ? modelFromReq.getJoinTables().get(1)
                : modelFromReq.getJoinTables().get(0);
        Assert.assertEquals("TEST_SCD2_1", join.getAlias());
        Assert.assertEquals(
                "\"TEST_KYLIN_FACT\".\"SELLER_ID\" = \"TEST_SCD2_1\".\"BUYER_ID\" "
                        + "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" >= \"TEST_SCD2_1\".\"START_DATE\" "
                        + "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" < \"TEST_SCD2_1\".\"END_DATE\"",
                join.getJoin().getNonEquiJoinCondition().getExpr());
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
        List<NDataModel.NamedColumn> otherColumns = model.getAllNamedColumns().stream().filter(column -> !column.isDimension())
                .collect(Collectors.toList());
        request.setOtherColumns(otherColumns);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }
}
