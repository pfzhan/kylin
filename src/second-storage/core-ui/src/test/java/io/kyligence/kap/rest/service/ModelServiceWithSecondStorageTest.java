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
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.kyligence.kap.job.delegate.JobMetadataDelegate;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.rest.delegate.JobMetadataContract;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
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
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.clickhouse.MockSecondStorage;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import lombok.val;
import lombok.var;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "javax.management.*", "javax.script.*" })
public class ModelServiceWithSecondStorageTest extends NLocalFileMetadataTestCase {

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

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);

        // Use thenAnswer instead of thenReturn, a workaround for https://github.com/powermock/powermock/issues/992
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.when(SpringContext.getApplicationContext()).thenAnswer(invocation -> applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenAnswer((invocation -> PowerMockito.mock(PermissionFactory.class)));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenAnswer(invocation -> PowerMockito.mock(PermissionGrantingStrategy.class));
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class))
                .thenAnswer(invocation -> new SecondStorageService());
        PowerMockito.when(SpringContext.getBean(JobMetadataInvoker.class))
                .thenAnswer(invocation -> new JobMetadataInvoker());
        PowerMockito.when(SpringContext.getBean(JobMetadataContract.class))
                .thenAnswer(invocation -> new JobMetadataDelegate());

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.engine.sparder-additional-files",
                "../../../build/conf/spark-executor-log4j.xml");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        prjManager.forceDropProject("broken_test");
        prjManager.forceDropProject("bad_query_test");

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);
        ExecutableUtils.initJobFactory();
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testChangePartitionDescAndSegWithSecondStorage() throws Exception {
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, project);
        SecondStorageUtil.initModelMetaData(project, model);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val modelRequest = prepare();
        modelRequest.setWithSecondStorage(false);
        modelRequest.getPartitionDesc().setPartitionDateColumn(null);
        modelRequest.setWithBaseIndex(true);
        modelRequest.setSaveOnly(true);

        val indexResponse = modelService.updateDataModelSemantic(project, modelRequest);
        Assert.assertFalse(indexResponse.isCleanSecondStorage());
    }

    @Test
    public void testConvertToRequestWithSecondStorage() throws IOException {
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
        BuildBaseIndexResponse changedResponse = mock(BuildBaseIndexResponse.class);
        Mockito.doCallRealMethod().when(modelService).changeSecondStorageIfNeeded("default", request, () -> true);

        when(changedResponse.hasTableIndexChange()).thenReturn(true);
        modelService.changeSecondStorageIfNeeded(project, request, () -> true);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val modelRequest = modelService.convertToRequest(modelService.getModelById(model, project));
        Assert.assertTrue(modelRequest.isWithSecondStorage());
    }

    @Test
    public void testChangePartitionDescWithSecondStorage() throws Exception {
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
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
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val modelRequest = prepare();
        modelRequest.setWithSecondStorage(true);
        modelRequest.getPartitionDesc().setPartitionDateColumn("TRANS_ID");
        modelService.updateDataModelSemantic("default", modelRequest);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));
    }

    private ModelRequest prepare() throws IOException {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        final String project = "default";
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val modelId = model.getId();

        modelMgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        model = modelMgr.getDataModelDesc(modelId);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(project);
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListNodesByProject() throws IOException {
        val project = "default";
        MockSecondStorage.mock(project, new ArrayList<>(), this);
        val nodeGroupManagerOption = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);

        Assert.assertTrue(nodeGroupManagerOption.isPresent());
        val nodeGroupManager = nodeGroupManagerOption.get();

        NodeGroup nodeGroup1 = new NodeGroup();
        nodeGroup1.setNodeNames(Lists.newArrayList("node01", "node02"));
        NodeGroup nodeGroup2 = new NodeGroup();
        nodeGroup2.setNodeNames(Lists.newArrayList("node01"));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            nodeGroupManager.createAS(nodeGroup1);
            return null;
        }, project);

        val mockNodeMap = (Map<String, Node>) (ReflectionTestUtils.getField(SecondStorageNodeHelper.class, "NODE_MAP"));
        mockNodeMap.put("node01", new Node().setName("node01").setIp("127.0.0.1").setPort(9000));
        mockNodeMap.put("node02", new Node().setName("node02").setIp("127.0.0.2").setPort(9000));
        mockNodeMap.put("node03", new Node().setName("node03").setIp("127.0.0.3").setPort(9000));

        Assert.assertEquals(2, SecondStorageNodeHelper.getALlNodesInProject(project).size());
        Assert.assertEquals(3, SecondStorageNodeHelper.getALlNodes().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllListNodes() throws IOException {
        MockSecondStorage.mock("default", new ArrayList<>(), this);

        val mockNodeMap = (Map<String, Node>) (ReflectionTestUtils.getField(SecondStorageNodeHelper.class, "NODE_MAP"));
        mockNodeMap.put("node01", new Node().setName("node01").setIp("127.0.0.1").setPort(9000));
        mockNodeMap.put("node02", new Node().setName("node02").setIp("127.0.0.2").setPort(9000));
        mockNodeMap.put("node03", new Node().setName("node03").setIp("127.0.0.3").setPort(9000));
        Assert.assertEquals(3, SecondStorageNodeHelper.getALlNodes().size());
    }
}
