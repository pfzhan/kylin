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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.MockSecondStorage;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;
import io.kyligence.kap.job.dao.JobInfoDao;
import io.kyligence.kap.job.delegate.JobMetadataDelegate;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobMapperFilter;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.delegate.JobMetadataContract;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "javax.management.*", "javax.script.*" })
@Slf4j
public class TableReloadServiceWithSecondStorageTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @InjectMocks
    private final TableService tableService = new TableService();

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Autowired
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private UserGroupInformation userGroupInformation;

    @Before
    public void setup() throws IOException, InterruptedException {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        userGroupInformation = Mockito.mock(UserGroupInformation.class);
        // Use thenAnswer instead of thenReturn, a workaround for https://github.com/powermock/powermock/issues/992
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.when(UserGroupInformation.getLoginUser()).thenAnswer((invocation -> userGroupInformation));

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);

        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        JobMetadataInvoker jobMetadataInvoker = new JobMetadataInvoker();
        ReflectionTestUtils.setField(tableService, "jobMetadataInvoker", jobMetadataInvoker);
        PowerMockito.when(SpringContext.getBean(JobMetadataContract.class))
                .thenAnswer(invocation -> new JobMetadataDelegate());

        createTestMetadata();
        getTestConfig().setProperty("kylin.query.engine.sparder-additional-files",
                "../../../build/conf/spark-executor-log4j.xml");

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");


        JobInfoMapper jobInfoMapper = Mockito.spy(JobInfoMapper.class);
        Mockito.when(jobInfoMapper.selectByJobFilter(Mockito.any(JobMapperFilter.class))).thenReturn(new ArrayList<>());
        JobInfoDao jobInfoDao = Mockito.spy(JobInfoDao.class);
        ReflectionTestUtils.setField(jobInfoDao, "jobInfoMapper", jobInfoMapper);
        Mockito.when(jobInfoDao.getJobs(Mockito.any(String.class))).thenReturn(new ArrayList<>());
        PowerMockito.when(SpringContext.getBean(JobInfoDao.class)).thenAnswer(invocation -> jobInfoDao);

    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testReloadTableWithSecondStorage() throws Exception {
        val model = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val project = "default";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, project);
        SecondStorageUtil.initModelMetaData("default", model);
        Assert.assertTrue(indexPlanManager.getIndexPlan(model).containBaseTableLayout());
        ModelRequest request = new ModelRequest();
        request.setWithSecondStorage(true);
        request.setUuid(model);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "IS_EFFECTUAL");

        val tableMeta = tableService.extractTableMeta(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, PROJECT).get(0);
        Mockito.when(userGroupInformation.doAs(Mockito.any(PrivilegedExceptionAction.class))).thenReturn(tableMeta);

        tableService.innerReloadTable(PROJECT, tableIdentity, true);

        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));
    }

    private void removeColumn(String tableIdentity, String... column) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val columns = Sets.newHashSet(column);
        val newColumns = Stream.of(factTable.getColumns()).filter(col -> !columns.contains(col.getName()))
                .toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }
}
