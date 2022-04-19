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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
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

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;

public class ComputedColumnRestoreTest extends SemiAutoTestBase {

    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;
    private JdbcRawRecStore jdbcRawRecStore;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Mock
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

    @Override
    public String getProject() {
        return "ssb";
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        prepareData();
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        rawRecService = new RawRecService();
        projectService = new ProjectService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
        QueryHistoryTaskScheduler queryHistoryTaskScheduler = QueryHistoryTaskScheduler.getInstance(getProject());
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "querySmartSupporter", rawRecService);
        queryHistoryTaskScheduler.init();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelChangeSupporters", Arrays.asList(rawRecService));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
        QueryHistoryTaskScheduler.shutdownByProject(getProject());
    }

    @Test
    public void testRecCCWithKeywords() throws Exception {
        //prepare model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                new String[] { "SELECT sum(\"hour\"+1) from SSB.CUSTOMER" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert model exists
        Set<String> strings = modelManager.listAllModelIds();
        Assert.assertEquals(1, strings.size());

        String uuid = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        // check cc
        ComputedColumnDesc ccNew = modelManager.getDataModelDesc(uuid).getComputedColumnDescs().get(0);
        Assert.assertNotNull(ccNew);
        String ccName = ccNew.getColumnName();

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        //  select sum(hour_1 + 1) from ssb.customer;
        //  propose cc using the column named by keyword
        String[] sql = new String[] { String.join("", "SELECT SUM(", ccName, " + 1) from SSB.CUSTOMER") };
        AbstractContext context2 = ProposerJob.genOptRec(getTestConfig(), getProject(), sql);
        rawRecService.transferAndSaveRecommendations(context2);
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(3, rawRecItems.size());

    }

    private void prepareData() {
        //prepare data, create table, then add a column named with sql keyword
        String tableName = "SSB.CUSTOMER";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc customer = tableManager.getTableDesc(tableName);
        ColumnDesc[] columns = customer.getColumns();
        columns[0].setName("hour");
        customer.setColumns(columns);
        tableManager.updateTableDesc(customer);
    }

}
