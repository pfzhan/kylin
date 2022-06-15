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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.model.util.scd2.SCD2SqlConverter;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.OpenAccSqlResponse;
import io.kyligence.kap.rest.response.OpenModelRecResponse;
import io.kyligence.kap.rest.response.OpenSuggestionResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import lombok.val;
import lombok.var;

public class SmartModelServiceTest extends SourceTestCase {
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private final ModelSmartService modelSmartService = Mockito.spy(new ModelSmartService());

    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(new ProjectSmartService());

    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());

    @Mock
    private final JobSupporter jobService = Mockito.spy(JobSupporter.class);

    @Mock
    private final AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private final StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelQueryService, "aclEvaluate", aclEvaluate);
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
        ReflectionTestUtils.setField(semanticService, "modelSmartSupporter", modelSmartService);
        ReflectionTestUtils.setField(modelService, "projectService", projectService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);

        ReflectionTestUtils.setField(modelSmartService, "modelService", modelService);
        ReflectionTestUtils.setField(modelSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);
        ReflectionTestUtils.setField(tableService, "jobService", jobService);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testSuggestModel() {
        List<String> sqls = Lists.newArrayList();
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelSmartService.couldAnsweredByExistedModel(getProject(), sqls);
        Assert.assertTrue(result);
    }

    @Test
    public void testAnswerBySnapshot() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val tableManager = NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject());
        val table = tableManager.copyForWrite(tableManager.getTableDesc("DEFAULT.TEST_ORDER"));
        table.setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");
        tableManager.updateTableDesc(table);

        List<String> sqls = Lists.newArrayList("select order_id, count(*) from test_order group by order_id limit 1");
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelSmartService.couldAnsweredByExistedModel(getProject(), sqls);
        Assert.assertTrue(result);
    }

    @Test
    public void testMultipleModelContextSelectedTheSameModel() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(dataflow.getConfig(),
                dataflow.getProject());
        val table1 = tableMetadataManager.copyForWrite(tableMetadataManager.getTableDesc("EDW.TEST_CAL_DT"));
        table1.setLastSnapshotPath("default/table_snapshot/EDW.TEST_CAL_DT/a27a7f08-792a-4514-a5ec-3182ea5474cc");
        tableMetadataManager.updateTableDesc(table1);

        val table2 = tableMetadataManager.copyForWrite(tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER"));
        table2.setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");
        tableMetadataManager.updateTableDesc(table2);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(getProject(), copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });

        val sqls = Lists.newArrayList("select order_id, count(*) from test_order group by order_id limit 1",
                "select cal_dt, count(*) from edw.test_cal_dt group by cal_dt limit 1",
                "SELECT count(*) \n" + "FROM \n" + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n"
                        + "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                        + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                        + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\" group by test_kylin_fact.cal_dt");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, true, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(3, response.getReusedModels().size());
        Assert.assertEquals(0, response.getNewModels().size());
        response.getReusedModels().forEach(recommendedModelResponse -> {
            List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
            Assert.assertTrue(indexes.isEmpty());
        });

        AbstractContext proposeContext2 = modelSmartService.suggestModel(getProject(), sqls.subList(0, 2), true, true);
        val response2 = modelSmartService.buildModelSuggestionResponse(proposeContext2);
        Assert.assertEquals(2, response2.getReusedModels().size());
        Assert.assertEquals(0, response2.getNewModels().size());
        response2.getReusedModels().forEach(recommendedModelResponse -> {
            List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
            Assert.assertTrue(indexes.isEmpty());
        });
    }

    @Test
    public void testOptimizeModelNeedMergeIndex() {
        String project = "newten";

        // prepare initial model
        String sql = "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact "
                + "where cal_dt = '2012-01-02' group by lstg_format_name, cal_dt";
        AbstractContext smartContext = ProposerJob.proposeForAutoMode(getTestConfig(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        // assert initial result
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCount);
        Assert.assertEquals(2, dataModel.getAllMeasures().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(dataModel.getUuid()).getAllLayouts().size());

        // transfer auto model to semi-auto
        // make model online
        transferProjectToSemiAutoMode(getTestConfig(), project);
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);

        // optimize with a batch of sql list
        List<String> sqlList = Lists.newArrayList();
        sqlList.add(sql);
        sqlList.add("select lstg_format_name, cal_dt, sum(price) from test_kylin_fact "
                + "where lstg_format_name = 'USA' group by lstg_format_name, cal_dt");
        sqlList.add("select lstg_format_name, cal_dt, count(item_count) from test_kylin_fact "
                + "where cal_dt = '2012-01-02' group by lstg_format_name, cal_dt");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, true, true);

        // assert optimization result
        List<AbstractContext.ModelContext> modelContextsAfterOptimization = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextsAfterOptimization.size());
        AbstractContext.ModelContext modelContextAfterOptimization = modelContextsAfterOptimization.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMap = modelContextAfterOptimization.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap.size()); // if no merge, the result will be 3.

        // apply recommendations
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelSmartService.saveRecResult(modelSuggestionResponse, project);

        // assert result after apply recommendations
        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        Assert.assertEquals(3, modelAfterSuggestModel.getAllMeasures().size());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelAfterSuggestModel.getUuid());
        Assert.assertEquals(3, indexPlan.getAllLayouts().size());

        // remove proposed indexes
        indexPlan.getAllLayouts().forEach(l -> indexPlanService.removeIndex(project, targetModel.getUuid(), l.getId()));
        IndexPlan indexPlanRefreshed = indexPlanManager.getIndexPlan(targetModel.getUuid());
        Assert.assertTrue(indexPlanRefreshed.getAllLayouts().isEmpty());

        // suggest again and assert result again
        AbstractContext proposeContextSecond = modelSmartService.suggestModel(project, sqlList, true, true);
        List<AbstractContext.ModelContext> modelContextsTwice = proposeContextSecond.getModelContexts();
        Assert.assertEquals(1, modelContextsTwice.size());
        AbstractContext.ModelContext modelContextTwice = modelContextsTwice.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMapTwice = modelContextTwice.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMapTwice.size());
    }

    @Test
    public void testSuggestModelWithSimpleQuery() {
        String project = "newten";
        transferProjectToSemiAutoMode(getTestConfig(), project);

        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select floor(date'2020-11-17' TO day), ceil(date'2020-11-17' TO day) from test_kylin_fact");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, false, true);
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelSmartService.saveRecResult(modelSuggestionResponse, project);

        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();
        long dimensionCountRefreshed = targetModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCountRefreshed);
        Assert.assertEquals(1, targetModel.getAllMeasures().size());
    }

    @Test
    public void testSuggestOrOptimizeModels() {
        String project = "newten";
        // prepare initial model
        AbstractContext smartContext = ProposerJob.proposeForAutoMode(getTestConfig(), project,
                new String[] { "select price from test_kylin_fact" });
        smartContext.saveMetadata();
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        transferProjectToSemiAutoMode(getTestConfig(), project);
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCount);
        Assert.assertEquals(1, dataModel.getAllMeasures().size());

        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, true, true);
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelSmartService.saveRecResult(modelSuggestionResponse, project);

        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        Assert.assertEquals(2, modelAfterSuggestModel.getAllMeasures().size());
    }

    public static void transferProjectToSemiAutoMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @Test
    public void testOptimizeModel_Twice() {
        String project = "newten";
        val indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);

        Function<OpenSqlAccelerateRequest, OpenSqlAccelerateRequest> rewriteReq = req -> {
            req.setForce2CreateNewModel(false);
            return req;
        };
        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        OpenSuggestionResponse normalResponse = modelSmartService
                .suggestOrOptimizeModels(smartRequest(project, normSql));
        Assert.assertEquals(1, normalResponse.getModels().size());

        normSql = "select test_order.order_id,sum(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id";
        normalResponse = modelSmartService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));
        Assert.assertEquals(1, normalResponse.getModels().size());

        normSql = "select test_order.order_id,buyer_id,max(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id,LSTG_FORMAT_NAME";
        normalResponse = modelSmartService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));

        Assert.assertEquals(3,
                indexMgr.getIndexPlan(normalResponse.getModels().get(0).getUuid()).getAllLayouts().size());
    }

    @Test
    public void testAccSql() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        //create new model
        OpenAccSqlResponse normalResponse = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getOptimizedModels().size());

        //create new model and add advice for model
        String sql2 = "select max(buyer_id) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id";
        val request2 = smartRequest(project, sql2);
        String sql3 = "select max(order_id) from test_order";
        request2.getSqls().add(sql3);
        request2.setForce2CreateNewModel(false);
        normalResponse = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(1, normalResponse.getOptimizedModels().size());

        //acc again, due to model online, so have no impact
        normalResponse = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(0, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getOptimizedModels().size());
        Assert.assertEquals(2, normalResponse.getErrorSqlList().size());

        // acc again, due to model online and withOptimalModel=true, so have optimalModels and no error sql
        request2.setWithOptimalModel(true);
        normalResponse = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(0, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getErrorSqlList().size());
        Assert.assertEquals(2, normalResponse.getOptimalModels().size());
    }

    @Test
    public void testErrorAndConstantOptimalModelResponse() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        String sql2 = "select 1";
        String sql3 = "select select";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        request.getSqls().add(sql2);
        request.getSqls().add(sql3);
        OpenAccSqlResponse normalResponse = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(1, normalResponse.getErrorSqlList().size());
        Assert.assertEquals(1, normalResponse.getOptimalModels().size());
        OpenModelRecResponse openModelRecResponse = normalResponse.getOptimalModels().get(0);
        Assert.assertEquals("CONSTANT", openModelRecResponse.getAlias());
    }

    @Test
    public void testOptimizeModelWithProposingJoin() {
        String project = "newten";
        NProjectManager projectMgr = NProjectManager.getInstance(getTestConfig());
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);

        // create a base model
        String normSql = "select test_order.order_id,buyer_id from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        OpenSuggestionResponse normalResponse = modelSmartService
                .suggestOrOptimizeModels(smartRequest(project, normSql));
        Assert.assertEquals(1, normalResponse.getModels().size());
        String modelId = normalResponse.getModels().get(0).getUuid();
        final NDataModel model1 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(1, model1.getJoinTables().size());
        Assert.assertEquals(17, model1.getAllNamedColumns().size());

        // without proposing new join, accelerate failed
        normSql = "select test_order.order_id,sum(price) from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "left join edw.test_cal_dt on test_kylin_fact.cal_dt=test_cal_dt.cal_dt "
                + "group by test_order.order_id";
        OpenSqlAccelerateRequest request1 = smartRequest(project, normSql);
        request1.setForce2CreateNewModel(false);
        normalResponse = modelSmartService.suggestOrOptimizeModels(request1);
        Assert.assertEquals(0, normalResponse.getModels().size());
        Assert.assertEquals(1, normalResponse.getErrorSqlList().size());

        // with proposing new join, accelerate success
        projectMgr.updateProject(project, copyForWrite -> {
            copyForWrite.getConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        });
        OpenSqlAccelerateRequest request2 = smartRequest(project, normSql);
        request2.setForce2CreateNewModel(false);
        normalResponse = modelSmartService.suggestOrOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse.getModels().size());
        NDataModel model2 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(2, model2.getJoinTables().size());
        Assert.assertEquals(117, model2.getAllNamedColumns().size());

        // proposing new index
        normSql = "select test_order.order_id,buyer_id,max(price) from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id,LSTG_FORMAT_NAME";
        OpenSqlAccelerateRequest request3 = smartRequest(project, normSql);
        request3.setForce2CreateNewModel(false);
        normalResponse = modelSmartService.suggestOrOptimizeModels(request3);
        Assert.assertEquals(1, normalResponse.getModels().size());
        Assert.assertEquals(3, indexMgr.getIndexPlan(modelId).getAllLayouts().size());
    }

    @Test
    public void testModelNonEquiJoinBrokenRepair() {
        /* 1.create scd2 model
         * 2.turn off scd2 configuration
         * 3.unload fact table , model become broken
         * 4.reload the fact table, model should be offline when model is scd2 and scd2 is turned off
         */
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "true");
        String project = "newten";
        transferProjectToSemiAutoMode(getTestConfig(), project);
        String scd2Sql = "select test_order.order_id,buyer_id from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "and buyer_id>=seller_id and buyer_id<leaf_categ_id " //
                + "group by test_order.order_id,buyer_id";
        val scd2Response = modelSmartService.suggestOrOptimizeModels(smartRequest(project, scd2Sql));

        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        val normalResponse = modelSmartService.suggestOrOptimizeModels(smartRequest(project, normSql));

        String nonEquivModelId = scd2Response.getModels().get(0).getUuid();
        String normalModelId = normalResponse.getModels().get(0).getUuid();

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel scd2Model = modelManager.getDataModelDesc(nonEquivModelId);
        NDataModel normalModel = modelManager.getDataModelDesc(normalModelId);
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(scd2Model, project));
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(normalModel, project));
        Assert.assertTrue(SCD2CondChecker.INSTANCE.isScd2Model(scd2Model));

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), project);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableDesc.setMvcc(-1);

        // online -> broken
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);
        NDataModel nonEquivOnline2Broken = modelManager.getDataModelDesc(nonEquivModelId);
        NDataModel normalOnline2Broken = modelManager.getDataModelDesc(normalModelId);
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, convertModelStatus(nonEquivOnline2Broken, project));
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, convertModelStatus(normalOnline2Broken, project));

        // broken -> repair
        TableExtDesc orCreateTableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);
        tableExtService.loadTable(tableDesc, orCreateTableExt, project);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            NDataModel nonEquivBroken2Repair = modelManager.getDataModelDesc(nonEquivModelId);
            NDataModel normalBroken2Repair = modelManager.getDataModelDesc(nonEquivModelId);
            Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(nonEquivBroken2Repair, project));
            Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(normalBroken2Repair, project));
        });
    }

    private OpenSqlAccelerateRequest smartRequest(String project, String sql) {
        OpenSqlAccelerateRequest scd2Request = new OpenSqlAccelerateRequest();
        scd2Request.setProject(project);
        scd2Request.setSqls(Lists.newArrayList(sql));
        scd2Request.setAcceptRecommendation(true);
        scd2Request.setForce2CreateNewModel(true);
        scd2Request.setWithEmptySegment(true);
        scd2Request.setWithModelOnline(true);
        return scd2Request;
    }

    private ModelStatusToDisplayEnum convertModelStatus(NDataModel model, String project) {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        long inconsistentSegmentCount = dataflowManager.getDataflow(model.getUuid())
                .getSegments(SegmentStatusEnum.WARNING).size();
        return modelService.convertModelStatusToDisplay(model, model.getProject(), inconsistentSegmentCount);
    }

    @Test
    public void testProposeDimAsMeasureToAnswerCountDistinctExpr() {
        overwriteSystemProp("kylin.query.implicit-computed-column-convert", "FALSE");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "TRUE");
        val sqls = Lists.newArrayList("select \n"
                + "count(distinct (case when ORDER_ID > 0  then price when ORDER_ID > 10 then SELLER_ID  else null end))  \n"
                + "FROM TEST_KYLIN_FACT ");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, response.getNewModels().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().size());
        Assert.assertEquals(3, response.getNewModels().get(0).getIndexes().get(0).getDimensions().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().get(0).getMeasures().size());

        Assert.assertEquals("ORDER_ID",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(0).getDimension().getName());
        Assert.assertEquals("PRICE",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(1).getDimension().getName());
        Assert.assertEquals("SELLER_ID",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(2).getDimension().getName());
        Assert.assertEquals("COUNT_ALL",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(0).getMeasure().getName());
    }

    @Test
    public void testProposeMeasureWhenSubQueryOnFilter() {
        val sqls = Lists.newArrayList("select LO_ORDERDATE,sum(LO_TAX) from SSB.LINEORDER "
                + "where LO_ORDERDATE = (select max(LO_ORDERDATE) from SSB.LINEORDER) group by LO_ORDERDATE ;");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, response.getNewModels().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().get(0).getDimensions().size());
        Assert.assertEquals(3, response.getNewModels().get(0).getIndexes().get(0).getMeasures().size());

        Assert.assertEquals("LO_ORDERDATE",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(0).getDimension().getName());
        Assert.assertEquals("COUNT_ALL",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(0).getMeasure().getName());
        Assert.assertEquals("MAX_LINEORDER_LO_ORDERDATE",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(1).getMeasure().getName());
        Assert.assertEquals("SUM_LINEORDER_LO_TAX",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(2).getMeasure().getName());
    }

    @Test
    public void testCouldAnsweredByExistedModels() {
        val project = "streaming_test";
        val proposeContext0 = modelSmartService.probeRecommendation(project, Collections.emptyList());
        Assert.assertTrue(proposeContext0.getProposedModels().isEmpty());

        val sqlList = Collections.singletonList("SELECT * FROM SSB.P_LINEORDER_STR");
        val proposeContext1 = modelSmartService.probeRecommendation(project, sqlList);
        Assert.assertTrue(proposeContext1.getProposedModels().isEmpty());

        val sqls = Lists.newArrayList("select * from SSB.LINEORDER");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertTrue(response.getReusedModels().isEmpty());

        thrown.expect(KylinException.class);
        modelSmartService.probeRecommendation("not_existed_project", sqlList);
    }

    @Test
    public void testProposeWhenAggPushdown() {
        // before agg push down, propose table index and agg index
        val sqls = Lists.newArrayList("SELECT \"自定义 SQL 查询\".\"CAL_DT\" ,\n"
                + "       SUM (\"自定义 SQL 查询\".\"SELLER_ID\") AS \"TEMP_Calculation_54915774428294\",\n"
                + "               COUNT (DISTINCT \"自定义 SQL 查询\".\"CAL_DT\") AS \"TEMP_Calculation_97108873613918\",\n"
                + "                     COUNT (DISTINCT (CASE\n"
                + "                                          WHEN (\"t0\".\"x_measure__0\" > 0) THEN \"t0\".\"LSTG_FORMAT_NAME\"\n"
                + "                                          ELSE CAST (NULL AS VARCHAR (1))\n"
                + "                                      END)) AS \"TEMP_Calculation_97108873613911\"\n" + "FROM\n"
                + "  (SELECT *\n" + "   FROM TEST_KYLIN_FACT) \"自定义 SQL 查询\"\n" + "INNER JOIN\n"
                + "     (SELECT LSTG_FORMAT_NAME, ORDER_ID, SUM (\"PRICE\") AS \"X_measure__0\"\n"
                + "      FROM TEST_KYLIN_FACT  GROUP  BY LSTG_FORMAT_NAME, ORDER_ID) \"t0\" ON \"自定义 SQL 查询\".\"ORDER_ID\" = \"t0\".\"ORDER_ID\"\n"
                + "GROUP  BY \"自定义 SQL 查询\".\"CAL_DT\"");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        SuggestionResponse response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(2, response.getNewModels().get(0).getIndexPlan().getIndexes().size());
        Assert.assertTrue(response.getNewModels().get(0).getIndexPlan().getIndexes().get(0).isTableIndex());
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(1).getId()));

        // after agg push down, propose two agg index
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "TRUE");
        proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(2, response.getNewModels().get(0).getIndexPlan().getIndexes().size());
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(0).getId()));
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(1).getId()));
    }

    @Test
    public void testOnlineSCD2Model() throws Exception {
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));
        String projectName = "default";
        val scd2Model = createNonEquiJoinModel(projectName, "scd2_non_equi");

        //turn off scd2 model
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
        });
        thrown.expect(KylinException.class);
        thrown.expectMessage("This model can’t go online as it includes non-equal join conditions");
        modelService.updateDataModelStatus(scd2Model.getId(), "default", "ONLINE");
    }

    private NDataModel createNonEquiJoinModel(String projectName, String modelName) throws Exception {
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        NDataModel model = modelManager.getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        model.setPartitionDesc(null);
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject(projectName);
        modelRequest.setAlias(modelName);
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.getSimplifiedJoinTableDescs().get(0).getSimplifiedJoinDesc()
                .setSimplifiedNonEquiJoinConditions(genNonEquiJoinCond());

        return modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    private List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> genNonEquiJoinCond() {
        NonEquiJoinCondition.SimplifiedNonEquiJoinCondition join1 = new NonEquiJoinCondition.SimplifiedNonEquiJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.TEST_EXTENDED_COLUMN", SqlKind.GREATER_THAN_OR_EQUAL);
        NonEquiJoinCondition.SimplifiedNonEquiJoinCondition join2 = new NonEquiJoinCondition.SimplifiedNonEquiJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.BUYER_ID", SqlKind.LESS_THAN);
        return Arrays.asList(join1, join2);
    }

    @Test
    public void testCloneSCD2Model() throws Exception {
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));

        String projectName = "default";

        val scd2Model = createNonEquiJoinModel(projectName, "scd2_non_equi");

        //turn off scd2 model
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
        });

        modelService.cloneModel(scd2Model.getId(), "clone_scd2_non_equi", projectName);

        List<NDataModelResponse> newModels = modelService.getModels("clone_scd2_non_equi", projectName, true, "", null,
                "last_modify", true);

        Assert.assertEquals(1, newModels.size());

        Assert.assertEquals(ModelStatusToDisplayEnum.OFFLINE, newModels.get(0).getStatus());
    }

    @Test
    public void testCreateModelNonEquiJoin() throws Exception {

        val newModel = createNonEquiJoinModel("default", "non_equi_join");
        String sql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(newModel.getJoinTables().get(0).getJoin(),
                genNonEquiJoinCond());
        Assert.assertEquals(
                "select * from  \"DEFAULT\".\"TEST_KYLIN_FACT\" AS \"TEST_KYLIN_FACT\" LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" AS \"TEST_ORDER\" ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\" AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\">=\"TEST_ORDER\".\"TEST_EXTENDED_COLUMN\") AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\"<\"TEST_ORDER\".\"BUYER_ID\")",
                sql);

        Assert.assertTrue(newModel.getJoinTables().get(0).getJoin().isNonEquiJoin());
    }

    @Test
    public void testOptimizedModelWithModelViewSql() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());

        // use model view sql, can suggest optimized model
        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql2 = String.format("select order_id from %s.%s group by order_id", project,
                normalResponse.getCreatedModels().get(0).getAlias());
        val request2 = smartRequest(project, sql2);
        request2.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse1 = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse1.getOptimizedModels().size());
        Assert.assertEquals(normalResponse.getCreatedModels().get(0).getAlias(),
                normalResponse1.getOptimizedModels().get(0).getAlias());
    }

    @Test
    public void testOptimizedModelWithJoinViewModel() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        String sql2 = "select order_id,seller_id from test_kylin_fact group by order_id,seller_id";
        val request = smartRequest(project, sql1);
        request.getSqls().add(sql2);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(2, normalResponse.getCreatedModels().size());

        // use two view model join sql, can suggest two optimized model
        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql3 = String.format(
                "select * from (select order_id as a from %s.%s group by order_id ) t1 join"
                        + " (select order_id as b from %s.%s group by order_id) t2 on t1.a = t2.b",
                project, normalResponse.getCreatedModels().get(0).getAlias(), project,
                normalResponse.getCreatedModels().get(1).getAlias());
        val request2 = smartRequest(project, sql3);
        request2.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse1 = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(2, normalResponse1.getOptimizedModels().size());
        Assert.assertEquals(normalResponse1.getOptimizedModels().get(0).getAlias(),
                normalResponse.getCreatedModels().get(0).getAlias());
        Assert.assertEquals(normalResponse1.getOptimizedModels().get(1).getAlias(),
                normalResponse.getCreatedModels().get(1).getAlias());
    }

    @Test
    public void testOptimizedModelWithCloneViewModel() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());

        String sql2 = "select order_id,seller_id from test_kylin_fact group by order_id,seller_id";
        request.getSqls().add(sql2);
        request.setForce2CreateNewModel(true);
        OpenAccSqlResponse normalResponse1 = modelSmartService.suggestAndOptimizeModels(request);
        Assert.assertEquals(2, normalResponse1.getCreatedModels().size());
        Optional<OpenModelRecResponse> test_order = normalResponse1.getCreatedModels().stream()
                .filter(e -> e.getAlias().contains("TEST_ORDER")).findAny();
        Assert.assertTrue(test_order.isPresent());

        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql3 = String.format(
                "select * from (select order_id as a from %s.%s group by order_id ) t1 join"
                        + " (select order_id as b from %s.%s group by order_id) t2 on t1.a = t2.b",
                project, normalResponse.getCreatedModels().get(0).getAlias(), project, test_order.get().getAlias());
        String sql4 = "select seller_id from test_kylin_fact group by seller_id";
        String sql5 = "select test_order.order_id from test_order group by test_order.order_id";
        val request2 = smartRequest(project, sql3);
        request2.setForce2CreateNewModel(false);
        request2.getSqls().add(sql4);
        request2.getSqls().add(sql5);
        OpenAccSqlResponse normalResponse2 = modelSmartService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(3, normalResponse2.getOptimizedModels().size());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(0).getAlias(),
                normalResponse1.getCreatedModels().get(0).getAlias());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(1).getAlias(),
                normalResponse1.getCreatedModels().get(1).getAlias());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(2).getAlias(),
                normalResponse.getCreatedModels().get(0).getAlias());
    }

    @Test
    public void testProbeRecommendation_throwsException() {
        when(modelService.isProjectNotExist(Mockito.anyString())).thenReturn(true);
        try {
            modelSmartService.probeRecommendation("SOME_PROJECT", null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckBatchSqlSize() {
        List<String> list = Collections.nCopies(201, "sql");
        Assert.assertTrue(list.size() > 200);
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(modelSmartService, "checkBatchSqlSize", getTestConfig(), list));
    }
}
