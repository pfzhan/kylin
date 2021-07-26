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

import static io.kyligence.kap.rest.request.MultiPartitionMappingRequest.MappingRequest;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.BrokenEntityProxy;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler;
import io.kyligence.kap.engine.spark.job.ExecutableAddSegmentHandler;
import io.kyligence.kap.engine.spark.job.ExecutableMergeOrRefreshHandler;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.cuboid.CuboidStatus;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.LayoutPartition;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnumToDisplay;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.job.JobBucket;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.BadModelException.CauseType;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.model.util.scd2.SCD2SqlConverter;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.execution.SucceedChainedTestExecutable;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.FusionModelResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.ModelSuggestionResponse;
import io.kyligence.kap.rest.response.NCubeDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.OpenModelSuggestionResponse;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SimplifiedColumnResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelServiceTest extends CSVSourceTestCase {

    private final String MODEL_UT_INNER_JOIN_ID = "82fa7671-a935-45f5-8779-85703601f49a";

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Autowired
    private final TableService tableService = Mockito.spy(new TableService());

    @Autowired
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Autowired
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Autowired
    private final ProjectService projectService = Mockito.spy(new ProjectService());

    @InjectMocks
    private final SegmentHelper segmentHelper = new SegmentHelper();

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

    private final static String[] timeZones = { "GMT+8", "CST", "PST", "UTC" };

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "projectService", projectService);
        modelService.setSemanticUpdater(semanticService);
        modelService.setSegmentHelper(segmentHelper);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        RDBMSQueryHistoryDAO rdbmsQueryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        Mockito.doReturn(rdbmsQueryHistoryDAO).when(modelService).getQueryHistoryDao();
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);

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
    public void testGetModels() {

        List<NDataModelResponse> models2 = modelService.getModels("nmodel_full_measure_test", "default", false, "",
                null, "last_modify", true);
        Assert.assertEquals(1, models2.size());
        List<NDataModelResponse> model3 = modelService.getModels("nmodel_full_measure_test", "default", true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, model3.size());
        List<NDataModelResponse> model4 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                null, "last_modify", true);
        Assert.assertEquals(1, model4.size());
        Assert.assertEquals(99, model4.get(0).getStorage());
        Assert.assertEquals(100, model4.get(0).getSource());
        Assert.assertEquals("99.00", model4.get(0).getExpansionrate());
        Assert.assertEquals(0, model4.get(0).getUsage());
        List<NDataModelResponse> model5 = modelService.getModels("nmodel_full_measure_test", "default", false, "adm",
                Arrays.asList("DISABLED"), "last_modify", true);
        Assert.assertEquals(0, model5.size());

        List<NDataModelResponse> models6 = modelService.getModels("", "default", false, "", null, "last_modify", true,
                "nmodel_full_measure_test", null, null);
        Assert.assertEquals(1, models6.size());

        List<NDataModelResponse> models7 = modelService.getModels("", "default", false, "", null, "last_modify", true,
                "admin", null, null);
        Assert.assertEquals(7, models7.size());

        List<NDataModelResponse> models8 = modelService.getModels("nmodel_full_measure_test", "default", false, "",
                null, "last_modify", true, "admin", 0L, 1L);
        Assert.assertEquals(0, models8.size());

        String brokenModelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = dataModelManager.getDataModelDesc(brokenModelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        dataModelManager.updateDataBrokenModelDesc(brokenModel);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(brokenModelId);
        val brokenEntity = BrokenEntityProxy.getProxy(IndexPlan.class, indexPlan.getResourcePath());
        brokenEntity.setUuid(brokenModelId);
        brokenEntity.setMvcc(indexPlan.getMvcc());
        brokenEntity.setProject("default");
        Mockito.doReturn(brokenEntity).when(modelService).getIndexPlan(brokenModelId, "default");

        List<NDataModelResponse> models9 = modelService.getModels("nmodel_basic_inner", "default", false, "", null,
                "last_modify", true, "admin", null, null);
        Assert.assertEquals(1, models9.size());
        Assert.assertEquals(0, models9.get(0).getRecommendationsCount());
        Assert.assertEquals(0, models9.get(0).getAvailableIndexesCount());
        Assert.assertEquals(0, models9.get(0).getTotalIndexes());
        Assert.assertEquals(0, models9.get(0).getEmptyIndexesCount());
        Assert.assertEquals(0, models9.get(0).getLastBuildTime());
    }

    @Test
    public void testWarningStateOfModel() {
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val df = dsMgr.getDataflow(modelId);
        // clean segment
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(20L, 30L));
        dsMgr.updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);

        val models = modelService.getModels(df.getModelAlias(), getProject(), true, "", null, "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, models.get(0).getStatus());
    }

    @Test
    public void testGetModelsMvcc() {
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", false, "", null,
                "last_modify", true);
        var model = models.get(0);
        modelService.renameDataModel(model.getProject(), model.getUuid(), "new_alias");
        models = modelService.getModels("new_alias", "default", false, "", null, "last_modify", true);
        Assert.assertEquals(1, models.size());
        model = models.get(0);
        Assert.assertEquals(1, model.getMvcc());
    }

    @Test
    public void testSortModels() {

        List<NDataModelResponse> models = modelService.getModels("", "default", false, "", null, "usage", true);
        Assert.assertEquals(7, models.size());
        Assert.assertEquals("nmodel_basic_inner", models.get(0).getAlias());
        models = modelService.getModels("", "default", false, "", null, "usage", false);
        Assert.assertEquals("nmodel_basic_inner", models.get(models.size() - 1).getAlias());
        models = modelService.getModels("", "default", false, "", null, "storage", true);
        Assert.assertEquals("nmodel_basic", models.get(0).getAlias());
        models = modelService.getModels("", "default", false, "", null, "storage", false);
        Assert.assertEquals("nmodel_basic", models.get(models.size() - 1).getAlias());

        models = modelService.getModels("", "default", false, "", null, "expansionrate", true);
        Assert.assertEquals("nmodel_basic_inner", models.get(0).getAlias());
    }

    @Test
    public void testGetNonFlattenModel() {
        String project = "cc_test";
        String modelName = "test_model";
        NDataModelResponse model = modelService
                .getModels(modelName, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertEquals(8, model.getNamedColumns().size());
        Assert.assertEquals(8, model.getAllNamedColumns().stream().filter(NamedColumn::isDimension).count());

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.updateDataModel(model.getId(), copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> join.setFlattenable(JoinTableDesc.NORMALIZED));
        });
        NDataModel originModel = modelManager.getDataModelDescByAlias(modelName);
        originModel.getJoinTables().forEach(join -> Assert.assertFalse(join.isFlattenable()));

        //if onlyNormalDim set false, getModel can return nonflatten table dimension
        model = modelService
                .getModels(modelName, project, false, null, Lists.newArrayList(), null, false, null, null, null, false)
                .get(0);
        Assert.assertEquals(14, model.getNamedColumns().size());
        Assert.assertEquals(14, model.getAllNamedColumns().stream().filter(NamedColumn::isDimension).count());
    }

    @Test
    public void testGetNonFlattenModelOfBrokenModel() {
        String project = "cc_test";
        String modelName = "test_model";
        NDataModelResponse model = modelService
                .getModels(modelName, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertEquals(8, model.getNamedColumns().size());
        Assert.assertEquals(8, model.getAllNamedColumns().stream().filter(NamedColumn::isDimension).count());

        // update model to broken
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.updateDataModel(model.getUuid(), copyForWrite -> {
            copyForWrite.setBroken(true);
            copyForWrite.setBrokenReason(NDataModel.BrokenReason.EVENT);
        });
        NDataModel modelAfterUpdate = modelManager.getDataModelDescByAlias(modelName);
        Assert.assertTrue(modelAfterUpdate.isBroken());

        //if onlyNormalDim set false, getModel can return nonflatten table dimension
        model = modelService
                .getModels(modelName, project, false, null, Lists.newArrayList(), null, false, null, null, null, false)
                .get(0);
        Assert.assertEquals(8, model.getNamedColumns().size());
        Assert.assertEquals(8, model.getAllNamedColumns().stream().filter(NamedColumn::isDimension).count());
        Assert.assertTrue(model.isBroken());
    }

    @Test
    public void testOfflineAndOnlineAllModels() {
        String projectName = "default";
        Set<String> modelIds = modelService.listAllModelIdsInProject(projectName);

        List<String> statusList = Lists.newArrayList();
        for (String id : modelIds) {
            String modelStatus = modelService.getModelStatus(id, projectName).toString();
            statusList.add(modelStatus);
        }

        Assert.assertEquals("ONLINE", statusList.get(1));
        Assert.assertEquals("ONLINE", statusList.get(2));
        Assert.assertEquals("ONLINE", statusList.get(5));

        modelService.offlineAllModelsInProject(projectName);
        for (String id : modelIds) {
            String modelStatus = modelService.getModelStatus(id, projectName).toString();
            Assert.assertEquals("OFFLINE", modelStatus);
        }

        modelService.onlineAllModelsInProject(projectName);
        for (String id : modelIds) {
            String modelStatus = modelService.getModelStatus(id, projectName).toString();
            Assert.assertEquals("ONLINE", modelStatus);
        }
    }

    @Test
    public void testGetFilteredModels() {
        // Prepare mocked batch, hybrid, streaming, and second storage models
        List<NDataModelResponse> mockedModels = Lists.newArrayList();
        NDataModel modelSpy1 = Mockito.spy(new NDataModel());
        when(modelSpy1.getModelType()).thenReturn(NDataModel.ModelType.BATCH);
        mockedModels.add(new NDataModelResponse(modelSpy1));
        NDataModel modelSpy2 = Mockito.spy(new NDataModel());
        when(modelSpy2.getModelType()).thenReturn(NDataModel.ModelType.HYBRID);
        mockedModels.add(new NDataModelResponse(modelSpy2));
        mockedModels.add(new NDataModelResponse(modelSpy2));
        NDataModel modelSpy3 = Mockito.spy(new NDataModel());
        when(modelSpy3.getModelType()).thenReturn(NDataModel.ModelType.STREAMING);
        mockedModels.add(new NDataModelResponse(modelSpy3));
        NDataModelResponse modelSpy4 = Mockito.spy(new NDataModelResponse(new NDataModel()));
        when(modelSpy4.isSecondStorageEnabled()).thenReturn(true);
        mockedModels.add(modelSpy4);
        mockedModels.add(modelSpy4);
        mockedModels.add(modelSpy4);

        when(modelService.getModels("", "default", true, "ADMIN", Arrays.asList("ONLINE"), "last_modify", true, null,
                null, null, true)).thenReturn(mockedModels);
        doReturn(new ArrayList<>()).when(modelService).addOldParams(anyString(), any());

        DataResult<List<NDataModel>> modelResult1 = modelService.getModels("", true, "default", "ADMIN",
                Arrays.asList("ONLINE"), "", 0, 10, "last_modify", true, null, Arrays.asList(ModelAttributeEnum.BATCH,
                        ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID, ModelAttributeEnum.SECOND_STORAGE),
                null, null, true);

        DataResult<List<NDataModel>> modelResult2 = modelService.getModels("", true, "default", "ADMIN",
                Arrays.asList("ONLINE"), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING), null, null, true);

        DataResult<List<NDataModel>> modelResult3 = modelService.getModels("", true, "default", "ADMIN",
                Arrays.asList("ONLINE"), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.SECOND_STORAGE), null, null, true);

        DataResult<List<NDataModel>> modelResult4 = modelService.getModels("", true, "default", "ADMIN",
                Arrays.asList("ONLINE"), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.SECOND_STORAGE), null, null, true);

        Assert.assertEquals(7, modelResult1.getTotalSize());
        Assert.assertEquals(2, modelResult2.getTotalSize());
        Assert.assertEquals(4, modelResult3.getTotalSize());
        Assert.assertEquals(3, modelResult4.getTotalSize());
    }

    @Test
    @Ignore
    public void testGetModelsWithCC() {
        List<NDataModelResponse> models = modelService.getModels("nmodel_basic", "default", true, "", null, "", false);
        Assert.assertEquals(1, models.size());
        NDataModelResponse model = models.get(0);
        Assert.assertTrue(model.getSimpleTables().stream().map(t -> t.getColumns()).flatMap(List::stream)
                .anyMatch(SimplifiedColumnResponse::isComputedColumn));
    }

    @Test
    public void testGetSegmentsByRange() {
        Segments<NDataSegment> segments = modelService.getSegmentsByRange("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(1, segments.size());
    }

    @Test
    public void testGetSegmentNotFullIndex() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.markIndexesToBeDeleted(modelId,
                    indexPlan.getAllLayouts().stream().collect(Collectors.toSet()));
            copyForWrite.getIndexes().clear();
        });
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveLayouts(dataflow.getSegments().get(0).getSegDetails().getLayouts().get(0));
        dataflowManager.updateDataflow(dataflowUpdate);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default", "0", "" + Long.MAX_VALUE, "ONLINE", null, null, true, "start_time", false);
        Assert.assertThat(segments.size(), is(0));
    }

    @Test
    public void testGetSegmentsResponseSort() {
        Date now = new Date();
        List<NDataSegmentResponse> mockSegments = Lists.newArrayList();
        NDataSegmentResponse segmentResponse1 = new NDataSegmentResponse();
        segmentResponse1.setId("1");
        segmentResponse1.setRowCount(1);
        segmentResponse1.setCreateTime(DateUtils.addHours(now, -1).getTime());

        NDataSegmentResponse segmentResponse2 = new NDataSegmentResponse();
        segmentResponse2.setId("2");
        segmentResponse2.setRowCount(2);
        segmentResponse2.setCreateTime(now.getTime());

        NDataSegmentResponse segmentResponse3 = new NDataSegmentResponse();
        segmentResponse3.setId("3");
        segmentResponse3.setRowCount(3);
        segmentResponse3.setCreateTime(DateUtils.addHours(now, 1).getTime());

        mockSegments.add(segmentResponse1);
        mockSegments.add(segmentResponse3);
        mockSegments.add(segmentResponse2);

        Mockito.doReturn(mockSegments).when(modelService).getSegmentsResponseCore(ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.any());

        Mockito.doAnswer(invocation -> {
            List<NDataSegmentResponse> segmentResponseList = invocation.getArgument(2);
            for (NDataSegmentResponse segmentResponse : segmentResponseList) {
                segmentResponse.setSecondStorageSize(Longs.tryParse(segmentResponse.getId()));
            }
            return null;
        }).when(modelService).addSecondStorageResponse(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any());

        List<NDataSegmentResponse> segmentResponseList = modelService.getSegmentsResponse(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0", "" + Long.MAX_VALUE, "", "second_storage_size",
                false);

        Assert.assertEquals(segmentResponseList.get(0).getId(), "3");
    }

    @Test
    public void testGetSegmentsResponse() {
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default", "0", "" + Long.MAX_VALUE, "ONLINE", "start_time", false);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(3380224, segments.get(0).getBytesSize());
        Assert.assertEquals("16", segments.get(0).getAdditionalInfo().get("file_count"));
        Assert.assertEquals("ONLINE", segments.get(0).getStatusToDisplay().toString());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);

        Segments<NDataSegment> segs = new Segments();
        val seg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "", "start_time", false);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals("LOADING", segments.get(0).getStatusToDisplay().toString());

        seg.setStatus(SegmentStatusEnum.READY);
        segs.add(seg);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToUpdateSegs(segs.toArray(new NDataSegment[segs.size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "", "start_time", false);
        Assert.assertEquals(2, segments.size());
        Assert.assertEquals("REFRESHING", segments.get(1).getStatusToDisplay().toString());

        Segments<NDataSegment> segs2 = new Segments<>();
        Segments<NDataSegment> segs3 = new Segments<>();

        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val seg2 = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        seg2.setStatus(SegmentStatusEnum.READY);
        seg2.setEncodingDataSkew(true);
        seg2.setSnapshotReady(true);
        seg2.setDictReady(true);
        seg2.setFlatTableReady(true);
        seg2.setFactViewReady(true);
        segs3.add(seg2);
        val segToRemove = dataflow.getSegment(segments.get(1).getId());
        segs2.add(segToRemove);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(segs2.toArray(new NDataSegment[segs2.size()]));
        dataflowUpdate.setToUpdateSegs(segs3.toArray(new NDataSegment[segs3.size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        segments = modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "0",
                "" + Long.MAX_VALUE, "", "start_time", false);
        Assert.assertEquals(3, segments.size());
        Assert.assertEquals("MERGING", segments.get(2).getStatusToDisplay().toString());

        // KE-25547, complete segment response
        val seg2Resp = segments.stream().filter(s -> s.getId().equals(seg2.getId())).findFirst().get();
        Assert.assertNotNull(seg2Resp);
        Assert.assertEquals(seg2.isEncodingDataSkew(), seg2Resp.isEncodingDataSkew());
        Assert.assertEquals(seg2.isSnapshotReady(), seg2Resp.isSnapshotReady());
        Assert.assertEquals(seg2.isDictReady(), seg2Resp.isDictReady());
        Assert.assertEquals(seg2.isFlatTableReady(), seg2Resp.isFlatTableReady());
        Assert.assertEquals(seg2.isFactViewReady(), seg2Resp.isFactViewReady());
    }

    @Test
    public void testGetSegmentResponseWithPartitions() {
        val project = "multi_level_partition";
        val dataflowId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        var segments = modelService.getSegmentsResponse(dataflowId, project, "0", "" + Long.MAX_VALUE, "", "", false);
        Assert.assertEquals(5, segments.size());
        Assert.assertEquals(4, segments.get(0).getMultiPartitionCount());
        Assert.assertEquals(4, segments.get(0).getMultiPartitionCountTotal());
        Assert.assertEquals(5588, segments.get(0).getBytesSize());
        Assert.assertEquals(56, segments.get(0).getRowCount());
        Assert.assertEquals(773349, segments.get(0).getSourceBytesSize());
        Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(0).getStatusToDisplay());

        Assert.assertEquals(3, segments.get(1).getMultiPartitionCount());
        Assert.assertEquals(4, segments.get(1).getMultiPartitionCountTotal());
        Assert.assertEquals(4191, segments.get(1).getBytesSize());
        Assert.assertEquals(42, segments.get(1).getRowCount());
        Assert.assertEquals(773349, segments.get(1).getSourceBytesSize());
        Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(1).getStatusToDisplay());

        Assert.assertEquals(3, segments.get(2).getMultiPartitionCount());
        Assert.assertEquals(4, segments.get(2).getMultiPartitionCountTotal());
        Assert.assertEquals(4191, segments.get(2).getBytesSize());
        Assert.assertEquals(42, segments.get(2).getRowCount());
        Assert.assertEquals(773349, segments.get(2).getSourceBytesSize());
        Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(2).getStatusToDisplay());

        Assert.assertEquals(2, segments.get(3).getMultiPartitionCount());
        Assert.assertEquals(4, segments.get(3).getMultiPartitionCountTotal());
        Assert.assertEquals(2794, segments.get(3).getBytesSize());
        Assert.assertEquals(28, segments.get(3).getRowCount());
        Assert.assertEquals(773349, segments.get(3).getSourceBytesSize());
        Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(3).getStatusToDisplay());

        Assert.assertEquals(2, segments.get(4).getMultiPartitionCount());
        Assert.assertEquals(4, segments.get(4).getMultiPartitionCountTotal());
        Assert.assertEquals(2794, segments.get(4).getBytesSize());
        Assert.assertEquals(28, segments.get(4).getRowCount());
        Assert.assertEquals(773349, segments.get(4).getSourceBytesSize());
        Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(4).getStatusToDisplay());

        // status test
        // loading
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        val segment1Id = segments.get(0).getId();
        dataflowManager.appendPartitions(dataflowId, segment1Id, Lists.<String[]> newArrayList(new String[] { "4" }));
        segments = modelService.getSegmentsResponse(dataflowId, project, "0", "" + Long.MAX_VALUE, "", "", false);
        Assert.assertEquals(SegmentStatusEnumToDisplay.LOADING, segments.get(0).getStatusToDisplay());

        // refreshing
        val segment2 = dataflowManager.getDataflow(dataflowId).copy().getSegments().get(1);
        segment2.getMultiPartitions().get(0).setStatus(PartitionStatusEnum.REFRESH);
        val dfUpdate = new NDataflowUpdate(dataflowId);
        dfUpdate.setToUpdateSegs(segment2);
        dataflowManager.updateDataflow(dfUpdate);
        segments = modelService.getSegmentsResponse(dataflowId, project, "0", "" + Long.MAX_VALUE, "", "", false);
        Assert.assertEquals(SegmentStatusEnumToDisplay.REFRESHING, segments.get(1).getStatusToDisplay());
    }

    @Test
    public void testGetSegmentPartitions() {
        val project = "multi_level_partition";
        val dataflowId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val segment1Id = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        val segment2Id = "d75a822c-788a-4592-a500-cf20186dded1";

        // append a new partition to segment1
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.appendPartitions(dataflowId, segment1Id, Lists.<String[]> newArrayList(new String[] { "4" }));
        // make the first partition in segment2 to refresh status
        val segment2 = dataflowManager.getDataflow(dataflowId).copy().getSegment(segment2Id);
        segment2.getMultiPartitions().get(0).setStatus(PartitionStatusEnum.REFRESH);
        val dfUpdate = new NDataflowUpdate(dataflowId);
        dfUpdate.setToUpdateSegs(segment2);
        dataflowManager.updateDataflow(dfUpdate);

        val partitions1 = modelService.getSegmentPartitions(project, dataflowId, segment1Id, null, "last_modified_time",
                false);
        Assert.assertEquals(5, partitions1.size());
        Assert.assertEquals(0, partitions1.get(0).getPartitionId());
        Assert.assertArrayEquals(new String[] { "0" }, partitions1.get(0).getValues());
        Assert.assertEquals(PartitionStatusEnumToDisplay.ONLINE, partitions1.get(0).getStatus());
        Assert.assertEquals(42, partitions1.get(0).getSourceCount());
        Assert.assertEquals(1397, partitions1.get(0).getBytesSize());
        Assert.assertEquals(4, partitions1.get(4).getPartitionId());
        Assert.assertArrayEquals(new String[] { "4" }, partitions1.get(4).getValues());
        Assert.assertEquals(PartitionStatusEnumToDisplay.LOADING, partitions1.get(4).getStatus());
        Assert.assertEquals(42, partitions1.get(0).getSourceCount());
        Assert.assertEquals(0, partitions1.get(4).getBytesSize());

        val partitions2 = modelService.getSegmentPartitions(project, dataflowId, segment2Id, null, "last_modified_time",
                false);
        Assert.assertEquals(3, partitions2.size());
        Assert.assertEquals(0, partitions2.get(0).getPartitionId());
        Assert.assertArrayEquals(new String[] { "0" }, partitions2.get(0).getValues());
        Assert.assertEquals(PartitionStatusEnumToDisplay.REFRESHING, partitions2.get(0).getStatus());
        Assert.assertEquals(42, partitions1.get(0).getSourceCount());
        Assert.assertEquals(1397, partitions2.get(0).getBytesSize());
        Assert.assertEquals(1, partitions2.get(1).getPartitionId());
        Assert.assertArrayEquals(new String[] { "1" }, partitions2.get(1).getValues());
        Assert.assertEquals(PartitionStatusEnumToDisplay.ONLINE, partitions2.get(1).getStatus());
        Assert.assertEquals(42, partitions1.get(0).getSourceCount());
        Assert.assertEquals(1397, partitions2.get(1).getBytesSize());

        // filter by status
        val onlinePartitions2 = modelService.getSegmentPartitions(project, dataflowId, segment2Id,
                Lists.newArrayList("ONLINE"), "last_modified_time", true);
        Assert.assertEquals(2, onlinePartitions2.size());
        Assert.assertEquals(2, onlinePartitions2.get(0).getPartitionId());
        Assert.assertArrayEquals(new String[] { "2" }, onlinePartitions2.get(0).getValues());
        Assert.assertEquals(PartitionStatusEnumToDisplay.ONLINE, onlinePartitions2.get(0).getStatus());
        Assert.assertEquals(1, onlinePartitions2.get(1).getPartitionId());
        Assert.assertEquals(PartitionStatusEnumToDisplay.ONLINE, onlinePartitions2.get(1).getStatus());
    }

    @Test
    public void testGetSegmentPartition_not_exist_id() {
        val project = "multi_level_partition";
        val dataflowId = "747f864b-9721-4b97-acde-0aa8e8656cba";

        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find the segment by ID \"not_exist_id\". Please check and try again.");
        modelService.getSegmentPartitions(project, dataflowId, "not_exist_id", null, "last_modified_time", false);
    }

    @Test
    public void testUpdateMultiPartitionMapping() {
        val project = "multi_level_partition";
        val modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val mappingRequest = new MultiPartitionMappingRequest();
        mappingRequest.setProject(project);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);

        // add mapping
        modelService.updateMultiPartitionMapping(project, modelId, mappingRequest);
        var model = modelManager.getDataModelDesc(modelId);
        Assert.assertNull(model.getMultiPartitionKeyMapping().getMultiPartitionCols());
        Assert.assertNull(model.getMultiPartitionKeyMapping().getAliasColumnRefs());

        // update mapping
        mappingRequest.setPartitionCols(Lists.newArrayList("test_kylin_fact.lstg_site_id"));
        mappingRequest.setAliasCols(Lists.newArrayList("test_kylin_fact.leaf_categ_id"));
        val valueMappings = Lists.<MappingRequest<List<String>, List<String>>> newArrayList();
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("0"), Lists.newArrayList("10")));
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("1"), Lists.newArrayList("10")));
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("2"), Lists.newArrayList("11")));
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("3"), Lists.newArrayList("11")));
        mappingRequest.setValueMapping(valueMappings);
        modelService.updateMultiPartitionMapping(project, modelId, mappingRequest);
        model = modelManager.getDataModelDesc(modelId);
        var mapping = model.getMultiPartitionKeyMapping();
        val aliasColumn = model.findColumn("leaf_categ_id");
        Assert.assertEquals(1, mapping.getAliasColumns().size());
        Assert.assertEquals(aliasColumn, mapping.getAliasColumns().get(0));
        Assert.assertNotNull(mapping.getAliasValue(Lists.newArrayList("0")));
        Assert.assertEquals(Lists.<List<String>> newArrayList(Lists.newArrayList("10")),
                mapping.getAliasValue(Lists.newArrayList("0")));
        Assert.assertNotNull(mapping.getAliasValue(Lists.newArrayList("1")));
        Assert.assertEquals(Lists.<List<String>> newArrayList(Lists.newArrayList("10")),
                mapping.getAliasValue(Lists.newArrayList("1")));
        Assert.assertNotNull(mapping.getAliasValue(Lists.newArrayList("2")));
        Assert.assertEquals(Lists.<List<String>> newArrayList(Lists.newArrayList("11")),
                mapping.getAliasValue(Lists.newArrayList("2")));
        Assert.assertNotNull(mapping.getAliasValue(Lists.newArrayList("3")));
        Assert.assertEquals(Lists.<List<String>> newArrayList(Lists.newArrayList("11")),
                mapping.getAliasValue(Lists.newArrayList("3")));

        // invalid request
        // wrong size
        mappingRequest
                .setAliasCols(Lists.newArrayList("test_kylin_fact.leaf_categ_id", "test_kylin_fact.lstg_format_name"));
        try {
            modelService.updateMultiPartitionMapping(project, modelId, mappingRequest);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof IllegalArgumentException);
            Assert.assertTrue(ex.getMessage().contains(
                    "Can’t update the mapping relationships of the partition column. The value for the parameter “multi_partition_columns“ doesn’t match the partition column defined in the model. Please check and try again."));
        }
        // wrong partition column
        mappingRequest.setPartitionCols(Lists.newArrayList("test_kylin_fact.lstg_format_name"));
        mappingRequest.setAliasCols(Lists.newArrayList("test_kylin_fact.leaf_categ_id"));
        try {
            modelService.updateMultiPartitionMapping(project, modelId, mappingRequest);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertTrue(ex.getMessage().contains(
                    "Can’t update the mapping relationships of the partition column. The value for the parameter “multi_partition_columns“ doesn’t match the partition column defined in the model. Please check and try again."));
        }
        // wrong value mapping, missing partition3
        mappingRequest.setPartitionCols(Lists.newArrayList("test_kylin_fact.lstg_site_id"));
        valueMappings.clear();
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("0"), Lists.newArrayList("10")));
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("1"), Lists.newArrayList("10")));
        valueMappings.add(new MappingRequest<>(Lists.newArrayList("2"), Lists.newArrayList("11")));
        mappingRequest.setValueMapping(valueMappings);
        try {
            modelService.updateMultiPartitionMapping(project, modelId, mappingRequest);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertTrue(
                    ex.getMessage().contains("Can’t update the mapping relationships of the partition column"));
        }
        // wrong type model
        val project2 = "default";
        val modelId2 = "82fa7671-a935-45f5-8779-85703601f49a";
        val mappingRequest2 = new MultiPartitionMappingRequest();
        mappingRequest2.setProject(project2);
        try {
            modelService.updateMultiPartitionMapping(project2, modelId2, mappingRequest2);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertTrue(ex.getMessage().contains(
                    "\"ut_inner_join_cube_partial\" is not a multilevel partitioning model. Please check and try again."));
        }
    }

    @Test
    public void testMultiPartitionValues() {
        val project = "multi_level_partition";
        val modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        var values = modelService.getMultiPartitionValues(project, modelId);
        Assert.assertEquals(4, values.size());
        Assert.assertArrayEquals(new String[] { "0" }, values.get(0).getPartitionValue());
        Assert.assertEquals(3, values.get(0).getBuiltSegmentCount());
        Assert.assertEquals(5, values.get(0).getTotalSegmentCount());

        Assert.assertArrayEquals(new String[] { "1" }, values.get(1).getPartitionValue());
        Assert.assertEquals(4, values.get(1).getBuiltSegmentCount());
        Assert.assertEquals(5, values.get(1).getTotalSegmentCount());

        Assert.assertArrayEquals(new String[] { "2" }, values.get(2).getPartitionValue());
        Assert.assertEquals(4, values.get(2).getBuiltSegmentCount());
        Assert.assertEquals(5, values.get(2).getTotalSegmentCount());

        Assert.assertArrayEquals(new String[] { "3" }, values.get(3).getPartitionValue());
        Assert.assertEquals(3, values.get(3).getBuiltSegmentCount());
        Assert.assertEquals(5, values.get(3).getTotalSegmentCount());

        // add a new value and a existed value
        modelService.addMultiPartitionValues(project, modelId,
                Lists.<String[]> newArrayList(new String[] { "13" }, new String[] { "3" }));
        values = modelService.getMultiPartitionValues(project, modelId);
        Assert.assertEquals(5, values.size());
        Assert.assertArrayEquals(new String[] { "13" }, values.get(4).getPartitionValue());
        Assert.assertEquals(0, values.get(4).getBuiltSegmentCount());
        Assert.assertEquals(5, values.get(4).getTotalSegmentCount());
        // delete a existed value and a non-exist value
        modelService.deletePartitions(project, null, modelId, Sets.newHashSet(4L, 5L));
        values = modelService.getMultiPartitionValues(project, modelId);
        Assert.assertEquals(4, values.size());
        Assert.assertArrayEquals(new String[] { "0" }, values.get(0).getPartitionValue());
        Assert.assertArrayEquals(new String[] { "1" }, values.get(1).getPartitionValue());
        Assert.assertArrayEquals(new String[] { "2" }, values.get(2).getPartitionValue());
        Assert.assertArrayEquals(new String[] { "3" }, values.get(3).getPartitionValue());

        List<String[]> partitionValues = Lists.<String[]> newArrayList(new String[] { "2" });
        modelService.deletePartitionsByValues(project, null, modelId, partitionValues);
        values = modelService.getMultiPartitionValues(project, modelId);
        Assert.assertEquals(3, values.size());
        Assert.assertEquals(3L, values.get(2).getId());
        Assert.assertArrayEquals(new String[] { "3" }, values.get(2).getPartitionValue());

        // add a empty value and a value with part of blank
        modelService.addMultiPartitionValues(project, modelId,
                Lists.<String[]> newArrayList(new String[] { "  14  " }, new String[] { "  " }));
        values = modelService.getMultiPartitionValues(project, modelId);
        Assert.assertEquals(4, values.size());
        Assert.assertArrayEquals(new String[] { "14" }, values.get(3).getPartitionValue());

        try {
            partitionValues = Lists.<String[]> newArrayList(new String[] { "not-exist-value" });
            modelService.deletePartitionsByValues(project, null, modelId, partitionValues);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertTrue(ex.getMessage()
                    .contains("The subpartition(s) “not-exist-value“ doesn’t exist. Please check and try again."));
        }

    }

    @Test
    public void testIndexQueryHitCount() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(zoneId).toLocalDate();
        long currentDate = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());

        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(TimeUtil.minusDays(currentDate, 7), 1);
                            put(TimeUtil.minusDays(currentDate, 8), 2);
                            put(TimeUtil.minusDays(currentDate, 31), 100);
                        }
                    }));
                }
            });
        });

        val index = modelService.getAggIndices(getProject(), modelId, null, null, false, 0, 10, null, true).getIndices()
                .stream().filter(aggIndex -> aggIndex.getId() == 0L).findFirst().orElse(null);
        Assert.assertEquals(3, index.getQueryHitCount());
    }

    @Test
    public void testGetAggIndices() {
        IndicesResponse indices = modelService.getAggIndices("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null,
                null, false, 0, 10, null, true);
        Assert.assertEquals(5, indices.getIndices().size());
        Assert.assertTrue(indices.getIndices().get(0).getId() < IndexEntity.TABLE_INDEX_START_ID);

        final String contentSegIndexId = "200";
        indices = modelService.getAggIndices("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null, contentSegIndexId,
                false, 0, 10, null, true);
        Assert.assertTrue(indices.getIndices().stream()
                .allMatch(index -> String.valueOf(index.getId()).contains(contentSegIndexId)));

        final String contentSegDimension = "ORDer";
        indices = modelService.getAggIndices("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null,
                contentSegDimension, false, 0, 10, null, true);
        Assert.assertTrue(indices.getIndices().stream().allMatch(index -> index.getDimensions().stream()
                .anyMatch(d -> d.contains(contentSegDimension.toUpperCase(Locale.ROOT)))));

        final String contentSegMeasure = "GMV";
        indices = modelService.getAggIndices("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null, contentSegMeasure,
                true, 0, 10, null, true);
        Assert.assertTrue(indices.getIndices().stream()
                .allMatch(index -> index.getMeasures().stream().anyMatch(d -> d.contains(contentSegMeasure))));

        indices = modelService.getAggIndices("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null, null, true, 0, 3,
                null, true);
        Assert.assertEquals(5, indices.getSize());
        Assert.assertEquals(3, indices.getIndices().size());
    }

    @Test
    public void testGetTableIndices() {

        IndicesResponse indices = modelService.getTableIndices("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertEquals(4, indices.getIndices().size());
        Assert.assertTrue(IndexEntity.isTableIndex(indices.getIndices().get(0).getId()));

    }

    @Test
    public void testGetIndices() {

        IndicesResponse indices = modelService.getIndices("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertEquals(9, indices.getIndices().size());
    }

    @Test
    public void testGetIndicesById_AVAILABLE() {
        IndicesResponse indices = modelService.getIndicesById("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 0L);

        Assert.assertEquals(0L, indices.getIndices().get(0).getId());
        Assert.assertEquals(CuboidStatus.AVAILABLE, indices.getIndices().get(0).getStatus());
        Assert.assertEquals(252928L, indices.getIndices().get(0).getStorageSize());
    }

    @Test
    public void testGetIndicesById_NoSegments_EMPTYStatus() {
        IndicesResponse indices = modelService.getIndicesById("default", MODEL_UT_INNER_JOIN_ID, 130000L);
        Assert.assertEquals(130000L, indices.getIndices().get(0).getId());
        Assert.assertEquals(CuboidStatus.EMPTY, indices.getIndices().get(0).getStatus());
        Assert.assertEquals(0L, indices.getIndices().get(0).getStorageSize());
        Assert.assertEquals(0L, indices.getStartTime());
        Assert.assertEquals(0L, indices.getEndTime());
    }

    @Test
    public void testGetIndicesById_NoReadySegments() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        dfMgr.appendSegment(dfMgr.getDataflow(MODEL_UT_INNER_JOIN_ID),
                new SegmentRange.TimePartitionedSegmentRange(100L, 200L));
        IndicesResponse indices = modelService.getIndicesById("default", MODEL_UT_INNER_JOIN_ID, 130000L);
        Assert.assertEquals(130000L, indices.getIndices().get(0).getId());
        Assert.assertEquals(CuboidStatus.EMPTY, indices.getIndices().get(0).getStatus());
        Assert.assertEquals(0L, indices.getIndices().get(0).getStorageSize());
        Assert.assertEquals(0L, indices.getStartTime());
        Assert.assertEquals(0L, indices.getEndTime());
    }

    @Test
    public void testGetModelJson() throws IOException {
        String modelJson = modelService.getModelJson("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        Assert.assertTrue(JsonUtil.readValue(modelJson, NDataModel.class).getUuid()
                .equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
    }

    @Test
    public void testDropModelExceptionName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find model named \"nmodel_basic2222\". Please check and try again.");
        modelService.dropModel("nmodel_basic2222", "default");
    }

    @Test
    public void testDropModelPass() throws NoSuchFieldException, IllegalAccessException {
        String modelId = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94";
        String project = "default";
        JobManager jobManager = JobManager.getInstance(getTestConfig(), project);
        val jobId = jobManager.addIndexJob(new JobParam(modelId, "admin"));
        Assert.assertNull(jobId);
        AtomicBoolean clean = new AtomicBoolean(false);

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
            return null;
        }, "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding", "default", true, "", null,
                "last_modify", true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));
        // Assert.assertTrue(clean.get());
    }

    @Test
    public void testDropStreamingModelPass() throws NoSuchFieldException, IllegalAccessException {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        String project = "streaming_test";

        val config = getTestConfig();
        val prjMgr = NProjectManager.getInstance(config);
        prjMgr.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        });

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel(modelId, project);
            return null;
        }, project);
        List<NDataModelResponse> models = modelService.getModels("stream_merge", project, true, "", null, "last_modify",
                true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNull(buildJobMeta);
        Assert.assertNull(mergeJobMeta);
    }

    @Test
    public void testPurgeModelManually() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        modelService.purgeModelManually("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
        List<NDataSegment> segments = modelService.getSegmentsByRange("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));
    }

    @Test
    public void testPurgeModelManually_TableOriented_Exception() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        NDataModel modelUpdate = modelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t purge data by specifying model \"test_encoding\" under the current project settings.");
        modelService.purgeModelManually("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
    }

    @Test
    public void testGetAffectedSegmentsResponse_FullBuildAndEmptyModel() {

        List<NDataSegment> segments = modelService.getSegmentsByRange("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(1, segments.size());
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        dfMgr.updateDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setSegments(new Segments());
        });
        RefreshAffectedSegmentsResponse response = modelService.getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.TEST_KYLIN_FACT", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(0L, response.getByteSize());
    }

    @Test
    public void testGetAffectedSegmentsResponse_NoRelatedModel() {
        RefreshAffectedSegmentsResponse response = modelService.getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.NO_TABLE", "0", "" + Long.MAX_VALUE);
        Assert.assertEquals(0, response.getByteSize());
    }

    @Test
    public void testGetAffectedSegmentsResponse_TwoModelWithDiffSegment() {
        prepareTwoOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df1 = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df2 = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        //purge segments first
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToRemoveSegs(df1.getSegments().toArray(new NDataSegment[0]));
        df1 = dfMgr.updateDataflow(update1);
        dfMgr.appendSegment(df1, new SegmentRange.TimePartitionedSegmentRange(10L, 30L));
        dfMgr.updateDataflow(df1.getId(), copyForWrite -> {
            copyForWrite.getSegments().get(0).setStatus(SegmentStatusEnum.READY);
        });
        NDataflowUpdate update2 = new NDataflowUpdate(df2.getUuid());
        update2.setToRemoveSegs(df2.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        dfMgr.appendSegment(df2, new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        dfMgr.updateDataflow(df2.getId(), copyForWrite -> {
            copyForWrite.getSegments().get(0).setStatus(SegmentStatusEnum.READY);
        });

        val response = modelService.getRefreshAffectedSegmentsResponse("default", "DEFAULT.TEST_KYLIN_FACT", "0", "50");
        Assert.assertEquals("0", response.getAffectedStart());
        Assert.assertEquals("30", response.getAffectedEnd());
    }

    @Test
    public void testPurgeModelExceptionName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find model named \"nmodel_basic2222\". Please check and try again.");
        modelService.purgeModelManually("nmodel_basic2222", "default");
    }

    @Test
    public void testCloneModelException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model \"nmodel_basic_inner\" already exists. Please rename it.");
        modelService.cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner", "default");
    }

    @Test
    public void testCloneModelExceptionName() {
        thrown.expectCause(is(KylinException.class));
        thrown.expectMessageInTransaction("Can’t find model named \"nmodel_basic2222\". Please check and try again.");
        modelService.cloneModel("nmodel_basic2222", "nmodel_basic_inner222", "default");
    }

    @Test
    public void testCloneModel() {
        String modelId = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        modelManager.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(10));
        Assert.assertEquals(10, modelManager.getDataModelDesc(modelId).getRecommendationsCount());
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));
        modelService.cloneModel(modelId, "test_encoding_new", "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding_new", "default", true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(randomUser, models.get(0).getOwner());
        Assert.assertEquals(0, models.get(0).getRecommendationsCount());

        // test clone model without locked layout
        String indexPlanId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        indexPlanManager.updateIndexPlan(indexPlanId, copyForWrite -> {
            var indexPlan = indexPlanManager.getIndexPlan(indexPlanId);
            val ruleBaseIndex = indexPlan.getRuleBasedIndex();
            UpdateRuleBasedCuboidRequest request = new UpdateRuleBasedCuboidRequest();
            request.setProject("default");
            request.setModelId(indexPlanId);
            request.setLoadData(false);
            request.setGlobalDimCap(null);
            request.setAggregationGroups(ruleBaseIndex.getAggregationGroups().subList(0, 1));
            RuleBasedIndex newRuleBasedCuboid = request.convertToRuleBasedIndex();
            copyForWrite.setRuleBasedIndex(newRuleBasedCuboid, false, true);
        });

        modelService.cloneModel(indexPlanId, "test_clone_with_locked", "default");
        List<NDataModelResponse> newModels = modelService.getModels("test_clone_with_locked", "default", true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, newModels.size());
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(indexPlanId);
        Assert.assertEquals(1, originIndexPlan.getToBeDeletedIndexes().size());
        IndexPlan clonedIndexPlan = indexPlanManager.getIndexPlan(newModels.get(0).getUuid());
        Assert.assertEquals(0, clonedIndexPlan.getToBeDeletedIndexes().size());
        val df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(newModels.get(0).getUuid());
        Assert.assertEquals(df.getStatus(), RealizationStatusEnum.OFFLINE);
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

        Assert.assertTrue(newModels.size() == 1);

        Assert.assertEquals(newModels.get(0).getStatus(), ModelStatusToDisplayEnum.OFFLINE);
    }

    @Test
    public void testRenameModel() {
        modelService.renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_name");
        List<NDataModelResponse> models = modelService.getModels("new_name", "default", true, "", null, "last_modify",
                true);
        Assert.assertEquals("new_name", models.get(0).getAlias());
    }

    @Test
    public void testRenameModelException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find model named \"nmodel_basic222\". Please check and try again.");
        modelService.renameDataModel("default", "nmodel_basic222", "new_name");
    }

    @Test
    public void testRenameModelException2() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model \"nmodel_basic_inner\" already exists. Please rename it.");
        modelService.renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner");
    }

    @Test
    public void testUpdateDataModelStatus() {
        modelService.updateDataModelStatus("cb596712-3a09-46f8-aea1-988b43fe9b6c", "default", "OFFLINE");
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", true, "", null,
                "last_modify", true);
        Assert.assertTrue(models.get(0).getUuid().equals("cb596712-3a09-46f8-aea1-988b43fe9b6c")
                && models.get(0).getStatus() == ModelStatusToDisplayEnum.OFFLINE);
    }

    @Test
    public void testUpdateFusionDataModelStatus() {
        val project = "streaming_test";
        val mgr = NDataflowManager.getInstance(getTestConfig(), project);
        var batchStatus = mgr.getDataflow("334671fd-e383-4fc9-b5c2-94fce832f77a").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, batchStatus);
        var streamingStatus = mgr.getDataflow("b05034a8-c037-416b-aa26-9e6b4a41ee40").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, streamingStatus);
        modelService.updateDataModelStatus("b05034a8-c037-416b-aa26-9e6b4a41ee40", project, "ONLINE");

        batchStatus = mgr.getDataflow("334671fd-e383-4fc9-b5c2-94fce832f77a").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, batchStatus);
        streamingStatus = mgr.getDataflow("b05034a8-c037-416b-aa26-9e6b4a41ee40").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, streamingStatus);

        List<NDataModelResponse> models = modelService.getModels("streaming_test", project, true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertFalse(models.get(0).isHasSegments());
        Assert.assertTrue(models.get(0) instanceof FusionModelResponse);
        Assert.assertTrue(((FusionModelResponse)models.get(0)).getBatchSegments().isEmpty());
        Assert.assertEquals(ModelStatusToDisplayEnum.OFFLINE, models.get(0).getStatus());
    }

    @Test
    public void testUpdateFusionDataModelStatus1() {
        val project = "streaming_test";
        val mgr = NDataflowManager.getInstance(getTestConfig(), project);
        var batchDataflow = mgr.getDataflow("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        var batchStatus = batchDataflow.getStatus();
        Assert.assertEquals(RealizationStatusEnum.ONLINE, batchStatus);

        modelService.updateDataModelStatus("cd2b9a23-699c-4699-b0dd-38c9412b3dfd", project, "OFFLINE");
        var streamingDataflow = mgr.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        var streamingStatus = streamingDataflow.getStatus();
        Assert.assertEquals(RealizationStatusEnum.ONLINE, streamingStatus);

        List<NDataModelResponse> models = modelService.getModels("model_streaming", project, true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertTrue(models.get(0).isHasSegments());
        Assert.assertTrue(models.get(0) instanceof FusionModelResponse);
        Assert.assertNotNull(((FusionModelResponse)models.get(0)).getBatchSegments());
        Assert.assertEquals(ModelStatusToDisplayEnum.ONLINE, models.get(0).getStatus());

        modelService.updateDataModelStatus("4965c827-fbb4-4ea1-a744-3f341a3b030d", project, "OFFLINE");
        batchStatus = mgr.getDataflow("cd2b9a23-699c-4699-b0dd-38c9412b3dfd").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, batchStatus);
        streamingStatus = mgr.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, streamingStatus);

    }

    @Test
    public void testUpdateFusionDataModelStatus2() {
        val project = "streaming_test";
        val mgr = NDataflowManager.getInstance(getTestConfig(), project);
        var batchDataflow = mgr.getDataflow("334671fd-e383-4fc9-b5c2-94fce832f77a");
        var batchStatus = batchDataflow.getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, batchStatus);

        var streamingDataflow = mgr.getDataflow("b05034a8-c037-416b-aa26-9e6b4a41ee40");
        var streamingStatus = streamingDataflow.getStatus();
        val streamingSeg = mgr.appendSegmentForStreaming(streamingDataflow,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 1L, createKafkaPartitionOffset(0, 100L),
                        createKafkaPartitionOffset(0, 200L)));
        streamingSeg.setStatus(SegmentStatusEnum.READY);
        val update = new NDataflowUpdate(batchDataflow.getUuid());
        update.setToUpdateSegs(streamingSeg);
        mgr.updateDataflow(update);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, streamingStatus);

        modelService.updateDataModelStatus("b05034a8-c037-416b-aa26-9e6b4a41ee40", project, "ONLINE");

        batchStatus = mgr.getDataflow("334671fd-e383-4fc9-b5c2-94fce832f77a").getStatus();
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, batchStatus);
        streamingStatus = mgr.getDataflow("b05034a8-c037-416b-aa26-9e6b4a41ee40").getStatus();
        Assert.assertEquals(RealizationStatusEnum.ONLINE, streamingStatus);

        List<NDataModelResponse> models = modelService.getModels("streaming_test", project, true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertTrue(models.get(0).isHasSegments());
        // batch: online & no index, streaming:offline  ==> WARNING
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, models.get(0).getStatus());
    }

    @Test
    public void testUpdateDataModelStatus_ModelNotExist_Exception() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find model named \"nmodel_basic222\". Please check and try again.");
        modelService.updateDataModelStatus("nmodel_basic222", "default", "OFFLINE");
    }

    @Test
    @Ignore
    public void testUpdateDataModelStatus_NoReadySegments_Exception() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("No ready segment in model 'nmodel_basic_inner', can not online the model!");
        modelService.updateDataModelStatus("741ca86a-1f13-46da-a59f-95fb68615e3a", "default", "ONLINE");
    }

    @Test
    @Ignore("dataflow's checkAllowOnline method is removed")
    public void testUpdateDataModelStatus_SmallerThanQueryRange_Exception() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Some segments in model 'all_fixed_length' are not ready, can not online the model!");
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setUuid(UUID.randomUUID().toString());
        dataLoadingRange.setColumnName("CAL_DT");
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .createDataLoadingRange(dataLoadingRange);
        modelService.updateDataModelStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "ONLINE");
        modelService.updateDataModelStatus("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default", "ONLINE");
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

    @Test
    public void testGetSegmentRangeByModel() {
        SegmentRange segmentRange = modelService.getSegmentRangeByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "0", "2322442");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange);
        SegmentRange segmentRange2 = modelService.getSegmentRangeByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }

    @Test
    public void testGetRelatedModels_HasNoErrorJobs() {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(modelService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getExecutablesByStatus(ExecutableState.ERROR)).thenReturn(Lists.newArrayList());
        List<RelatedModelResponse> responses = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic");
        Assert.assertEquals(2, responses.size());
        Assert.assertEquals(false, responses.get(0).isHasErrorJobs());
    }

    @Test
    public void testGetRelatedModels_HasErrorJobs() {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(modelService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getExecutablesByStatus(ExecutableState.ERROR)).thenReturn(mockJobs());
        List<RelatedModelResponse> responses = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals(true, responses.get(0).isHasErrorJobs());
    }

    @Test
    public void testGetRelatedModels() {
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "EDW.TEST_CAL_DT", "");
        Assert.assertEquals(0, models.size());
        List<RelatedModelResponse> models2 = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, models2.size());
    }

    @Test
    public void testGetRelatedModels_OneModelBasedModel() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val modelUpdate = modelManager
                .copyForWrite(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT", "");
        Assert.assertEquals(3, models.size());
        val modelUpdate2 = modelManager
                .copyForWrite(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        modelUpdate2.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate2);
    }

    @Test
    public void testIsModelsUsingTable() {
        boolean result = modelService.isModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetModelUsingTable() {
        val result = modelService.getModelsUsingTable("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertEquals(4, result.size());
    }

    @Test
    public void testSuggestModel() {
        List<String> sqls = Lists.newArrayList();
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelService.couldAnsweredByExistedModel(getProject(), sqls);
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
        val result = modelService.couldAnsweredByExistedModel(getProject(), sqls);
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
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
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
        AbstractContext proposeContext = modelService.suggestModel(getProject(), sqls, true, true);
        val response = modelService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(3, response.getReusedModels().size());
        Assert.assertEquals(0, response.getNewModels().size());
        response.getReusedModels().forEach(recommendedModelResponse -> {
            List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
            Assert.assertTrue(indexes.isEmpty());
        });

        AbstractContext proposeContext2 = modelService.suggestModel(getProject(), sqls.subList(0, 2), true, true);
        val response2 = modelService.buildModelSuggestionResponse(proposeContext2);
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
        AbstractContext proposeContext = modelService.suggestModel(project, sqlList, true, true);

        // assert optimization result
        List<AbstractContext.ModelContext> modelContextsAfterOptimization = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextsAfterOptimization.size());
        AbstractContext.ModelContext modelContextAfterOptimization = modelContextsAfterOptimization.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMap = modelContextAfterOptimization.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap.size()); // if no merge, the result will be 3.

        // apply recommendations
        ModelSuggestionResponse modelSuggestionResponse = modelService.buildModelSuggestionResponse(proposeContext);
        modelService.saveRecResult(modelSuggestionResponse, project);

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
        AbstractContext proposeContextSecond = modelService.suggestModel(project, sqlList, true, true);
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
        AbstractContext proposeContext = modelService.suggestModel(project, sqlList, false, true);
        ModelSuggestionResponse modelSuggestionResponse = modelService.buildModelSuggestionResponse(proposeContext);
        modelService.saveRecResult(modelSuggestionResponse, project);

        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();
        long dimensionCountRefreshed = targetModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCountRefreshed);
        Assert.assertEquals(1, targetModel.getAllMeasures().size());
    }

    @Test
    public void testSuggestOrOptimizeModels() throws Exception {
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
        AbstractContext proposeContext = modelService.suggestModel(project, sqlList, true, true);
        ModelSuggestionResponse modelSuggestionResponse = modelService.buildModelSuggestionResponse(proposeContext);
        modelService.saveRecResult(modelSuggestionResponse, project);

        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        Assert.assertEquals(2, modelAfterSuggestModel.getAllMeasures().size());
    }

    public static void transferProjectToSemiAutoMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    private void prepareTwoOnlineModels() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
    }

    private void prepareTwoLagBehindModels() {
        //all lag behind
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        val df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update2);
    }

    private void prepareOneLagBehindAndOneOnlineModels() {
        //one ONLINE one Lag_behind
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update1);
    }

    @Test
    public void testDeleteSegmentById_SegmentIsLocked() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.READY);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        dataflowManager.refreshSegment(df, segmentRange);

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_LOCKED(), dataSegment.displayIdName()));

        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() }, false);
    }

    @Test
    public void testDeleteSegmentById_isNotExist() {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find the segment by ID \"not_exist_01\". Please check and try again.");
        //refresh exception
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { "not_exist_01" }, false);
    }

    @Test
    public void testDeleteSegmentById_cleanIndexPlanToBeDeleted() {
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        String project = "default";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = dataModelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NIndexPlanManager.getInstance(getTestConfig(), project).updateIndexPlan(modelId, copyForWrite -> {
            val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                    .filter(layoutEntity -> 1000001L == layoutEntity.getId()).collect(Collectors.toSet());
            copyForWrite.markIndexesToBeDeleted(modelId, toBeDeletedSet);
        });
        Assert.assertTrue(CollectionUtils.isNotEmpty(
                NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId).getToBeDeletedIndexes()));
        val df1 = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        Assert.assertEquals(df1.getStatus(), RealizationStatusEnum.ONLINE);
        modelService.deleteSegmentById(modelId, project, new String[] { "ef783e4d-e35f-4bd9-8afd-efd64336f04d" },
                false);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId);

        Assert.assertTrue(CollectionUtils.isEmpty(dataflow.getSegments()));
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getAllToBeDeleteLayoutId()));
        Assert.assertEquals(dataflow.getStatus(), RealizationStatusEnum.OFFLINE);
    }

    @Test
    public void testPurgeSegmentById_cleanIndexPlanToBeDeleted() {
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        String project = "default";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = dataModelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NIndexPlanManager.getInstance(getTestConfig(), project).updateIndexPlan(modelId, copyForWrite -> {
            val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                    .filter(layoutEntity -> 1000001L == layoutEntity.getId()).collect(Collectors.toSet());
            copyForWrite.markIndexesToBeDeleted(modelId, toBeDeletedSet);
        });
        Assert.assertTrue(CollectionUtils.isNotEmpty(
                NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId).getToBeDeletedIndexes()));

        modelService.purgeModelManually(modelId, project);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId);

        Assert.assertTrue(CollectionUtils.isEmpty(dataflow.getSegments()));
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getAllToBeDeleteLayoutId()));
    }

    @Test
    public void testDeleteSegmentById_TableOrientedModel_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t delete the segment(s) in model \"nmodel_basic_inner\" under the current project settings.");
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() }, false);
    }

    @Test
    public void testPurgeModelClearLockedIndex() {
        String project = "default";
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        // remove
        long tobeDeleteLayoutId = 20000000001L;

        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);

        //clear segment from df
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        //add two segment(include full layout)
        val update2 = new NDataflowUpdate(df.getUuid());
        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(seg1, seg2);

        List<NDataLayout> layouts = Lists.newArrayList();
        indexManager.getIndexPlan(modelId).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(df, seg1.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(df, seg2.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        dfManager.updateDataflow(update2);
        // mark a layout tobedelete
        indexManager.updateIndexPlan(modelId,
                copyForWrite -> copyForWrite.markWhiteIndexToBeDelete(modelId, Sets.newHashSet(tobeDeleteLayoutId)));
        Assert.assertFalse(
                NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().isEmpty());
        modelService.purgeModel(modelId, project);
        Assert.assertTrue(
                NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().isEmpty());
    }

    @Test
    public void testRefreshSegmentClearLockedIndex() {
        String project = "default";
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);

        //clear segment from df
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        //add two segment(include full layout)
        val update2 = new NDataflowUpdate(df.getUuid());
        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(seg1, seg2);
        List<NDataLayout> layouts = Lists.newArrayList();
        indexManager.getIndexPlan(modelId).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(df, seg1.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(df, seg2.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        dfManager.updateDataflow(update2);

        // remove
        long tobeDeleteLayoutId = 20000000001L;

        // mark a layout tobedelete
        indexManager.updateIndexPlan(modelId,
                copyForWrite -> copyForWrite.markWhiteIndexToBeDelete(modelId, Sets.newHashSet(tobeDeleteLayoutId)));
        Assert.assertFalse(indexManager.getIndexPlan(modelId).getToBeDeletedIndexes().isEmpty());

        //remove tobedelete layout from seg1
        val newDf = dfManager.getDataflow(modelId);
        dfManager.updateDataflowDetailsLayouts(newDf.getSegments().get(0), layouts.stream()
                .filter(layout -> layout.getLayoutId() != tobeDeleteLayoutId).collect(Collectors.toList()));

        // remove seg2 and tobedelete layout should be cleared from indexplan
        val update3 = new NDataflowUpdate(newDf.getUuid());
        update3.setToRemoveSegs(newDf.getSegments().get(1));
        dfManager.updateDataflow(update3);

        Assert.assertTrue(indexManager.getIndexPlan(modelId).getToBeDeletedIndexes().isEmpty());
    }

    @Test
    public void testMergeSegment() {
        val dfId = new String("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfManager.getDataflow(dfId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
        Segments<NDataSegment> segments = new Segments<>();

        // first segment
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-02-01");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment1 = dfManager.appendSegment(df, segmentRange);
        dataSegment1.setStatus(SegmentStatusEnum.READY);
        dataSegment1.setSegmentRange(segmentRange);
        segments.add(dataSegment1);

        // second segment
        start = SegmentRange.dateToLong("2010-02-01");
        end = SegmentRange.dateToLong("2010-04-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment2 = dfManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);

        // third segment
        start = SegmentRange.dateToLong("2010-04-01");
        end = SegmentRange.dateToLong("2010-05-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment3 = dfManager.appendSegment(df, segmentRange);
        dataSegment3.setStatus(SegmentStatusEnum.READY);
        dataSegment3.setSegmentRange(segmentRange);
        segments.add(dataSegment3);

        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dfManager.updateDataflow(update);

        try {
            modelService.mergeSegmentsManually(new MergeSegmentParams("default", dfId,
                    new String[] { dataSegment1.getId(), dataSegment3.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "Can’t merge the selected segments, as there are gap(s) in between. Please check and try again.",
                    e.getMessage());
        }

        modelService.mergeSegmentsManually(new MergeSegmentParams("default", dfId,
                new String[] { dataSegment1.getId(), dataSegment2.getId(), dataSegment3.getId() }));
        val execManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val executables = getRunningExecutables("default", "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(JobTypeEnum.INDEX_MERGE, executables.get(0).getJobType());
        AbstractExecutable job = executables.get(0);
        Assert.assertEquals(1, job.getTargetSegments().size());

        val mergedSegment = dfManager.getDataflow(dfId).getSegment(job.getTargetSegments().get(0));
        Assert.assertEquals(SegmentRange.dateToLong("2010-01-01"), mergedSegment.getSegRange().getStart());
        Assert.assertEquals(SegmentRange.dateToLong("2010-05-01"), mergedSegment.getSegRange().getEnd());

        try {
            //refresh exception
            modelService.mergeSegmentsManually(
                    new MergeSegmentParams("default", dfId, new String[] { dataSegment2.getId() }));
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals("Can’t remove, refresh or merge segment \"" + dataSegment2.displayIdName()
                    + "\", as it’s LOCKED. Please try again later.", e.getMessage());
        }
        // clear segments
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
    }

    @Test
    public void testMergeLoadingSegments() {
        val dfId = new String("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfManager.getDataflow(dfId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        Segments<NDataSegment> segments = new Segments<>();

        // first segment
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-02-01");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment1 = dfManager.appendSegment(df, segmentRange);
        dataSegment1.setStatus(SegmentStatusEnum.NEW);
        dataSegment1.setSegmentRange(segmentRange);
        segments.add(dataSegment1);

        // second segment
        start = SegmentRange.dateToLong("2010-02-01");
        end = SegmentRange.dateToLong("2010-03-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment2 = dfManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);

        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT,
                MsgPicker.getMsg().getSEGMENT_STATUS(SegmentStatusEnumToDisplay.LOADING.name()),
                dataSegment1.displayIdName()));
        modelService.mergeSegmentsManually(
                new MergeSegmentParams("default", dfId, new String[] { dataSegment1.getId(), dataSegment2.getId() }));

        // clear segments
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
    }

    @Test
    public void testRefreshSegmentById_SegmentToRefreshIsLocked_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);

        start = SegmentRange.dateToLong("2010-01-02");
        end = SegmentRange.dateToLong("2010-01-03");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dataSegment2 = dataflowManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        //refresh normally
        modelService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { dataSegment2.getId() }));
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSEGMENT_LOCKED(), dataSegment2.displayIdName()));
        //refresh exception
        modelService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { dataSegment2.getId() }));
    }

    @Test
    public void testRefreshSegmentById_isNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find the segment by ID \"not_exist_01\". Please check and try again.");
        //refresh exception
        modelService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { "not_exist_01" }));
    }

    @Test
    public void testDeleteSegmentById_UnconsecutiveSegmentsToDelete_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        Segments<NDataSegment> segments = new Segments<>();

        long start;
        long end;
        for (int i = 0; i <= 6; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //remove normally
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(0).getId() }, false);
        //2 dataflows
        val df2 = dataflowManager.getDataflow(dataModel.getUuid());
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(2).getId(), segments.get(3).getId() }, false);
        Assert.assertEquals(6, df2.getSegments().size());
    }

    @Test
    public void testCreateModel_ExistedAlias_Exception() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model \"nmodel_basic\" already exists. Please rename it.");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");
        NDataModel result = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertNotEquals(0L, result.getLastModified());
    }

    @Test
    public void testCreateModelWithNoCC() {
        try {
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            NDataModel model = modelManager.getDataModelDesc("b780e4e4-69af-449e-b09f-05c90dfa04b6");
            ModelRequest modelRequest = new ModelRequest(model);
            modelRequest.setUuid("no_cc_model");
            modelRequest.setAlias("no_cc_model");
            modelRequest.setLastModified(0L);
            modelRequest.setProject("default");
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Throwable e) {
            Assert.fail("Should not have thrown any exception");
        }
    }

    @Test
    public void testCreateModel_AutoMaintain_Exception() throws Exception {
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        prjManager.updateProject(copy);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setUuid("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t add model manually under this project.");
        modelService.createModel(modelRequest.getProject(), modelRequest);

    }

    @Test
    public void testCreateModel_PartitionIsNull() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setPartitionDesc(null);
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(1, df.getSegments().size());

        modelManager.dropModel(newModel);
    }

    @Test
    public void testCreateModelAndBuildManually() throws Exception {
        setupPushdownEnv();
        testGetLatestData();
        testCreateModel_PartitionNotNull();
        testBuildSegmentsManually_WithPushDown();
        testBuildSegmentsManually();
        testChangePartitionDesc();
        testChangePartitionDesc_OriginModelNoPartition();
        testChangePartitionDesc_NewModelNoPartitionColumn();
        cleanPushdownEnv();
    }

    @Test
    public void testCreateModel_passFullLoad() throws Exception {
        setupPushdownEnv();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().forEach(modelManager::dropModel);
        var modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table1.json"),
                ModelRequest.class);
        modelRequest.setProject("default");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.setPartitionDesc(null);
        val saved = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("sad", saved.getMeasureNameByMeasureId(100002));
        Assert.assertEquals("SAD", saved.getMeasureNameByMeasureId(100000));
        modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_full_load.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    private List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> genNonEquiJoinCond() {
        NonEquiJoinCondition.SimplifiedNonEquiJoinCondition join1 = new NonEquiJoinCondition.SimplifiedNonEquiJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.TEST_EXTENDED_COLUMN", SqlKind.GREATER_THAN_OR_EQUAL);
        NonEquiJoinCondition.SimplifiedNonEquiJoinCondition join2 = new NonEquiJoinCondition.SimplifiedNonEquiJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.BUYER_ID", SqlKind.LESS_THAN);
        return Arrays.asList(join1, join2);
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

        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        return newModel;
    }

    @Test
    public void testOptimizeModel_Twice() {
        String project = "newten";
        val projectMgr = NProjectManager.getInstance(getTestConfig());
        val indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        projectMgr.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        });

        Function<OpenSqlAccelerateRequest, OpenSqlAccelerateRequest> rewriteReq = req -> {
            req.setForce2CreateNewModel(false);
            return req;
        };

        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        OpenModelSuggestionResponse normalResponse = modelService
                .suggestOrOptimizeModels(smartRequest(project, normSql));

        normSql = "select test_order.order_id,sum(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id";
        normalResponse = modelService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));

        normSql = "select test_order.order_id,buyer_id,max(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id,LSTG_FORMAT_NAME";
        normalResponse = modelService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));

        Assert.assertEquals(3,
                indexMgr.getIndexPlan(normalResponse.getModels().get(0).getUuid()).getAllLayouts().size());
    }

    @Test
    public void testCreateModelNonEquiJoin() throws Exception {

        val newModel = createNonEquiJoinModel("default", "non_equi_join");
        String sql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(newModel.getJoinTables().get(0).getJoin(),
                genNonEquiJoinCond());
        Assert.assertEquals(sql,
                "select * from  \"DEFAULT\".\"TEST_KYLIN_FACT\" AS \"TEST_KYLIN_FACT\" LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" AS \"TEST_ORDER\" ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\" AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\">=\"TEST_ORDER\".\"TEST_EXTENDED_COLUMN\") AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\"<\"TEST_ORDER\".\"BUYER_ID\")");

        Assert.assertTrue(newModel.getJoinTables().get(0).getJoin().isNonEquiJoin());
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
        val scd2Response = modelService.suggestOrOptimizeModels(smartRequest(project, scd2Sql));

        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        val normalResponse = modelService.suggestOrOptimizeModels(smartRequest(project, normSql));

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

    private void testGetLatestData() throws Exception {
        ExistedDataRangeResponse response = modelService.getLatestDataRange("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null);
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE), response.getEndTime());
    }

    private void addModelInfo(ModelRequest modelRequest) {
        modelRequest.setProject("default");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.setStart("1325347200000");
        modelRequest.setEnd("1388505600000");
    }

    public void testChangePartitionDesc() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.getPartitionDesc().setPartitionDateColumn("TRANS_ID");
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", model.getPartitionDesc().getPartitionDateColumn());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(model.getUuid());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), "default").updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, "default");
        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_OriginModelNoPartition() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.setPartitionDesc(null);
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertNull(model.getPartitionDesc());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_NewModelNoPartitionColumn() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.getPartitionDesc().setPartitionDateColumn("");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testCreateModel_PartitionNotNull() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(0, df.getSegments().size());

        modelManager.dropModel(newModel);
    }

    @Test
    public void testCreateModelWithDefaultMeasures() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setStart("0");
        modelRequest.setEnd("100");
        modelRequest.setUuid(null);
        modelRequest.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        List<NDataModelResponse> models = modelService.getModels("new_model", "default", false, "ADMIN", null, "",
                false);
        Assert.assertEquals("COUNT_ALL", models.get(0).getSimplifiedMeasures().get(0).getName());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testBuildSegmentsManually_TableOrientedModel_Exception() throws Exception {
        thrown.expectInTransaction(KylinException.class);
        thrown.expectMessageInTransaction(
                "Can’t manually build segments in model \"nmodel_basic\" under the current project settings.");
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "0", "100");
    }

    @Test
    public void testUnlinkModel() {
        modelService.unlinkModel("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel nDataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(ManagementType.MODEL_BASED, nDataModel.getManagementType());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Model nmodel_basic_inner is model based, can not unlink it!");
        modelService.unlinkModel("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
    }

    @Test
    public void testGetCCUsage() {
        ComputedColumnUsageResponse usages = modelService.getComputedColumnUsages("default");
        Assert.assertEquals(2, usages.getUsageMap().get("TEST_KYLIN_FACT.DEAL_AMOUNT").getModels().size());
        Assert.assertNull(usages.getUsageMap().get("TEST_KYLIN_FACT.SELLER_COUNTRY_ABBR"));
        Assert.assertEquals(1,
                usages.getUsageMap().get("TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR").getModels().size());
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testAddSameNameDiffExprNormal() throws IOException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.SAME_NAME_DIFF_EXPR == ccException.getCauseType()
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT")
                        && ccException.getConflictingModel().equals("nmodel_basic_inner")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "The name of computed column 'TEST_KYLIN_FACT.DEAL_AMOUNT' has already been used in "
                                        + "model 'nmodel_basic_inner', and the expression is "
                                        + "'TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");

            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_basic", "default", true, null, null,
                "", false);
        Assert.assertEquals(1, dataModelDescs.size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        Unsafe.changeAccessibleObject(field, true);
        field.set(deserialized.getComputedColumnDescs().get(0), "1+1");
        modelService.getDataModelManager("default").updateDataModelDesc(deserialized);
        // TODO should use modelService.updateModelAndDesc("default", deserialized);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict2()
            throws IOException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("There is already a column named CAL_DT on table DEFAULT.TEST_KYLIN_FACT,"
                + " please change your computed column name");

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_basic", "default", true, null, null,
                "", false);
        Assert.assertEquals(1, dataModelDescs.size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("columnName");
        Unsafe.changeAccessibleObject(field, true);
        field.set(deserialized.getComputedColumnDescs().get(0), "cal_dt");
        modelService.getDataModelManager("default").updateDataModelDesc(deserialized);
        // TODO should use modelService.updateModelAndDesc("default", deserialized);
    }

    /*
     * start to test with model new_ci_left_join_model, which is structurely same as ci_left_join_model,
     * but with different alias
     */

    @Test
    public void testCCExpressionNotReferingHostAlias1() throws IOException {
        expectedEx.expect(BadModelException.class);
        expectedEx.expectMessage(
                "A computed column should be defined on root fact table if its expression is not referring its hosting alias table,"
                        + " cc: BUYER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR");
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        //replace last cc's host alias
        contents = StringUtils.reverse(
                StringUtils.reverse(contents).replaceFirst(StringUtils.reverse("\"tableAlias\": \"TEST_KYLIN_FACT\""),
                        StringUtils.reverse("\"tableAlias\": \"BUYER_ACCOUNT\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testCCExpressionNotReferingHostAlias2() throws IOException {
        expectedEx.expect(BadModelException.class);
        expectedEx.expectMessage(
                "A computed column should be defined on root fact table if its expression is not referring its hosting alias table,"
                        + " cc: BUYER_ACCOUNT.DEAL_AMOUNT");
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        //replace first cc's host alias
        String str = "\"columnName\": \"DEAL_AMOUNT\",";
        int index = contents.indexOf(str);
        contents = contents.substring(0, str.length() + index) + "\"tableAlias\": \"BUYER_ACCOUNT\","
                + contents.substring(str.length() + index);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameNormal() throws IOException {

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameOnDifferentAliasTable() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;

                return BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR == ccException.getCauseType()
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("SELLER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR")
                        && ccException.getMessage().equals(
                                "Computed column LEFTJOIN_SELLER_COUNTRY_ABBR's expression is already defined in model nmodel_basic, "
                                        + "to reuse it you have to define it on alias table: TEST_KYLIN_FACT");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                        + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }",
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                        + "      \"tableAlias\": \"SELLER_ACCOUNT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprSameNameOnDifferentAliasTableCannotProvideAdvice() throws Exception {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME == ccException.getCauseType()
                        && ccException.getConflictingModel().equals("nmodel_cc_test")
                        && ccException.getBadCC().equals("TEST_ORDER.ID_PLUS_1") && ccException.getAdvise() == null
                        && ccException.getMessage().equals(
                                "Computed column ID_PLUS_1 is already defined in model nmodel_cc_test, no suggestion could be provided to reuse it");
            }
        });

        //save ut_left_join_cc_model, which is a model defining cc on lookup table
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
        val request = new ModelRequest(deserialized);
        request.setProject("default");
        request.setStart("0");
        request.setEnd("100");
        request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        request.setUuid(null);
        modelService.createModel(request.getProject(), request);

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        contents = contents.replaceFirst("\"type\": \"LEFT\"", "\"type\": \"INNER\"");
        contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

        bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testSeekAdviseOnLookTable() throws Exception {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.SAME_NAME_DIFF_EXPR == ccException.getCauseType()
                        && ccException.getConflictingModel().equals("nmodel_cc_test")
                        && "UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)".equals(ccException.getAdvise())
                        && ccException.getBadCC().equals("BUYER_ACCOUNT.COUNTRY_UPPER")
                        && ccException.getMessage().equals(
                                "The name of computed column 'BUYER_ACCOUNT.COUNTRY_UPPER' has already been used "
                                        + "in model 'nmodel_cc_test', and the expression is "
                                        + "'UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        //save nmodel_cc_test, which is a model defining cc on lookup table
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        val request = new ModelRequest(deserialized);
        request.setProject("default");
        request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        request.setStart("0");
        request.setEnd("100");
        request.setUuid(UUID.randomUUID().toString());
        modelService.createModel(request.getProject(), request);
        //TODO modelService.updateModelToResourceStore(deserialized, "default");

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        contents = StringUtils.reverse(StringUtils.reverse(contents).replaceFirst(
                Pattern.quote(StringUtils.reverse("\"expression\": \"UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)\",")),
                StringUtils.reverse("\"expression\": null, ")));
        contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

        bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testAddEquivalentCcConflict() throws IOException {

        NDataModelManager dataModelManager = modelService.getDataModelManager("default");
        Serializer<NDataModel> serializer = dataModelManager.getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        ComputedColumnDesc newCC = new ComputedColumnDesc();
        newCC.setColumnName("CC_TEMP");
        newCC.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        newCC.setTableAlias("TEST_KYLIN_FACT");
        newCC.setExpression("SUBSTRING(BUYER_ACCOUNT.ACCOUNT_COUNTRY from 0 for 1)");
        newCC.setDatatype("string");
        deserialized.getComputedColumnDescs().add(newCC);
        ComputedColumnDesc newCC2 = new ComputedColumnDesc();
        newCC2.setColumnName("CC_TEMP2");
        newCC2.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        newCC2.setTableAlias("TEST_KYLIN_FACT");
        newCC2.setExpression("SUBSTRING(BUYER_ACCOUNT.ACCOUNT_COUNTRY, 0, 1)");
        newCC2.setDatatype("string");
        deserialized.getComputedColumnDescs().add(newCC2);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(deserialized, new DataOutputStream(baos));

        ByteArrayInputStream newBias = new ByteArrayInputStream(baos.toByteArray());
        NDataModel newModel = serializer.deserialize(new DataInputStream(newBias));

        thrown.expect(BadModelException.class);
        thrown.expectMessage("This expression has already been used by other computed columns in this model.");
        modelService.checkComputedColumn(newModel, "default", "TEST_KYLIN_FACT.CC_TEMP");
    }

    @Test
    public void testNewModelAddSameExprDiffNameOnDifferentAliasTable() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType() == BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("SELLER_ACCOUNT.LEFTJOIN_SELLER_COUNTRY_ABBR_2")
                        && ccException.getMessage().equals(
                                "Computed column LEFTJOIN_SELLER_COUNTRY_ABBR_2's expression is already defined in model nmodel_basic, to reuse it you have to define it on alias table: TEST_KYLIN_FACT");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                        + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }",
                " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                        + "      \"tableAlias\": \"SELLER_ACCOUNT\",\n"
                        + "      \"columnName\": \"LEFTJOIN_SELLER_COUNTRY_ABBR_2\",\n"
                        + "      \"expression\": \"SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)\",\n"
                        + "      \"datatype\": \"string\",\n"
                        + "      \"comment\": \"first char of country of seller account\"\n" + "    }");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExpr1() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType() == BadModelException.CauseType.SAME_NAME_DIFF_EXPR
                        && ccException.getAdvise().equals("SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR")
                        && ccException.getMessage()
                                .equals("The name of computed column 'TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR' "
                                        + "has already been used in model 'nmodel_basic', and the expression is "
                                        + "'SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)",
                "SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,2)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExpr2() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType() == BadModelException.CauseType.SAME_NAME_DIFF_EXPR
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "The name of computed column 'TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME' "
                                        + "has already been used in model 'nmodel_basic', and the expression is "
                                        + "'CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)",
                "SUBSTR(CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME),0,1)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameExprDiffName() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return ccException.getCauseType() == BadModelException.CauseType.SAME_EXPR_DIFF_NAME
                        && ccException.getAdvise().equals("LEFTJOIN_BUYER_COUNTRY_ABBR")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR_2")
                        && ccException.getMessage().equals(
                                "The expression of computed column has already been used in model 'nmodel_basic' as "
                                        + "'LEFTJOIN_BUYER_COUNTRY_ABBR'. Please modify the name to keep consistent, "
                                        + "or use a different expression.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_BUYER_COUNTRY_ABBR_2");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testNewModelAddSameNameDiffExprModelToNonDefaultProject() throws IOException {
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)",
                "SUBSTR(CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME),0,1)");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //it's adding to non-default project, should be okay because cc conflict check is by project
        modelService.getDataModelManager("newten").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "non-default");
    }

    @Test
    public void testNewModelAddDiffNameSameExprModelToNonDefaultProject() throws IOException {
        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_BUYER_COUNTRY_ABBR_2");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        //it's adding to non-default project, should be okay because cc conflict check is by project
        modelService.getDataModelManager("newten").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "non-default");
    }

    @Test
    public void testCCAdviseNormalCase() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.SAME_NAME_DIFF_EXPR == ccException.getCauseType()
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "The name of computed column 'TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME' "
                                        + "has already been used in model 'nmodel_basic', and the expression is "
                                        + "'CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testCCAdviseWithNonExistingName() throws IOException {

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("No advice could be provided");

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace(" \"columnName\": \"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\",",
                " \"columnName\": \"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME_2\",");
        contents = contents.replace(" \"column\": \"TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\"",
                " \"column\": \"TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME_2\"");
        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);
    }

    @Test
    public void testCCNameCheck() {
        ModelService.checkCCName("cc_1");
        try {
            // HIVE
            ModelService.checkCCName("LOCAL");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("The computed column name \"LOCAL\" is a SQL keyword. Please choose another name.",
                    e.getMessage());
        }

        try {
            // CALCITE
            ModelService.checkCCName("MSCK");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("The computed column name \"MSCK\" is a SQL keyword. Please choose another name.",
                    e.getMessage());
        }

    }

    @Test
    public void testCCAdviseUnmatchingSubgraph() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.SAME_NAME_DIFF_EXPR == ccException.getCauseType()
                        && ccException.getAdvise() == null && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "The name of computed column 'TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME' "
                                        + "has already been used in model 'nmodel_basic', and the expression is "
                                        + "'CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)\"", "null");

        //replace last join's type, which is for SELLER_ACCOUNT
        contents = StringUtils.reverse(StringUtils.reverse(contents)
                .replaceFirst(StringUtils.reverse("\"type\": \"LEFT\""), StringUtils.reverse("\"type\": \"INNER\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    @Test
    public void testCCAdviseMatchingSubgraph() throws IOException {
        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return BadModelException.CauseType.SAME_NAME_DIFF_EXPR == ccException.getCauseType()
                        && ccException.getAdvise().equals("CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME")

                        && ccException.getMessage().equals(
                                "The name of computed column 'TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME' "
                                        + "has already been used in model 'nmodel_basic', and the expression is "
                                        + "'CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)'. "
                                        + "Please modify the expression to keep consistent, or use a different name.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        contents = contents.replace("\"CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)\"", "null");

        //replace last join's type, which is for SELLER_ACCOUNT
        contents = StringUtils.reverse(StringUtils.reverse(contents)
                .replaceFirst(StringUtils.reverse("\"type\": \"LEFT\""), StringUtils.reverse("\"type\": \"INNER\"")));

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setSeekingCCAdvice(true);

        modelService.checkComputedColumn(deserialized, "default", null);

    }

    /*
     * now test conflict within a model
     */

    @Test
    public void testSameNameSameExprInOneModelNormal() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return CauseType.SELF_CONFLICT_WITH_SAME_NAME == ccException.getCauseType()
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "This name has already been used by other computed columns in this model. Please modify it.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT\",\n" + "      \"expression\": \"PRICE * ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    public void testDiffNameSameExprInOneModelNormal() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return CauseType.SELF_CONFLICT_WITH_SAME_EXPRESSION == ccException.getCauseType()
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "This expression has already been used by other computed columns in this model. Please modify it.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT_2\",\n" + "      \"expression\": \"PRICE * ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    @Test
    //compared with testDiffNameSameExprInOneModelNormal, expression is normalized
    public void testDiffNameSameExprInOneModelWithSlightlyDifferentExpression() throws IOException {

        expectedEx.expect(new BaseMatcher() {
            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof BadModelException)) {
                    return false;
                }
                BadModelException ccException = (BadModelException) item;
                return CauseType.SELF_CONFLICT_WITH_SAME_EXPRESSION == ccException.getCauseType()
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "This expression has already been used by other computed columns in this model. Please modify it.");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"columnName\": \"DEAL_AMOUNT_2\",\n"
                + "      \"expression\": \"TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT\",\n"
                + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
        contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
        //TODO modelService.updateModelToResourceStore(deserialized, "default");
    }

    /**
     * start to the side effect of bad model
     */

    /**
     * if a bad model is detected, it should not affect the existing table desc
     * <p>
     * same bad model as testDiffNameSameExprInOneModelWithSlightlyDifferentExpression
     */
    @Test
    public void testCreateBadModelWontAffectTableDesc() throws IOException {

        try {
            Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
            String contents = StringUtils.join(Files.readAllLines(
                    new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                    Charset.defaultCharset()), "\n");

            String str = "\"computed_columns\": [";
            int i = contents.indexOf(str) + str.length();
            String oneMoreCC = " {\n" //
                    + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                    + "      \"columnName\": \"DEAL_AMOUNT_2\",\n"
                    + "      \"expression\": \"TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT\",\n"
                    + "      \"datatype\": \"decimal\",\n" //
                    + "      \"comment\": \"bla bla bla\"\n" //
                    + "    },";
            contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
            //TODO modelService.updateModelToResourceStore(deserialized, "default");
        } catch (BadModelException e) {
            modelService.getTableManager("default").resetProjectSpecificTableDesc();
            TableDesc aDefault = modelService.getTableManager("default").getTableDesc("DEFAULT.TEST_KYLIN_FACT");
            Set<String> allColumnNames = Arrays.stream(aDefault.getColumns()).map(ColumnDesc::getName)
                    .collect(Collectors.toSet());
            Assert.assertFalse(allColumnNames.contains("DEAL_AMOUNT_2"));
        }
    }

    @Test
    /**
     * testSeekAdviseOnLookTable
     */
    public void testSeekAdviceWontAffectTableDesc() throws Exception {

        try {
            //save nmodel_cc_test, which is a model defining cc on lookup table
            Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
            String contents = StringUtils.join(Files.readAllLines(
                    new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                    Charset.defaultCharset()), "\n");
            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            val request = new ModelRequest(deserialized);
            request.setStart("0");
            request.setEnd("100");
            request.setProject("default");
            request.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
            modelService.createModel(request.getProject(), request);
            //TODO modelService.updateModelToResourceStore(deserialized, "default");

            List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_cc_test", "default", true, null,
                    null, "", false);
            Assert.assertTrue(dataModelDescs.size() == 1);

            contents = StringUtils.reverse(StringUtils.reverse(contents).replaceFirst(
                    Pattern.quote(StringUtils.reverse("\"expression\": \"UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)\",")),
                    StringUtils.reverse("\"expression\": null, ")));
            contents = contents.replace("nmodel_cc_test", "nmodel_cc_test_2");

            bais = IOUtils.toInputStream(contents, Charset.defaultCharset());

            deserialized = serializer.deserialize(new DataInputStream(bais));
            deserialized.setUuid(UUID.randomUUID().toString());
            deserialized.setSeekingCCAdvice(true);

            modelService.checkComputedColumn(deserialized, "default", null);

        } catch (BadModelException e) {
            modelService.getTableManager("default").resetProjectSpecificTableDesc();
            TableDesc aDefault = modelService.getTableManager("default").getTableDesc("DEFAULT.TEST_ACCOUNT");
            Assert.assertEquals(5, aDefault.getColumns().length);
        }
    }

    @Test
    public void testPreProcessBeforeModelSave() throws IOException {
        NDataModelManager modelManager = modelService.getDataModelManager("default");
        Serializer<NDataModel> serializer = modelManager.getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");
        InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setCachedAndShared(true);
        NDataModel updated = modelManager.copyForWrite(deserialized);
        List<ComputedColumnDesc> newCCs1 = Lists.newArrayList(deserialized.getComputedColumnDescs());
        ComputedColumnDesc ccDesc1 = new ComputedColumnDesc();
        ccDesc1.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc1.setColumnName("CC1");
        ccDesc1.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 1");
        ccDesc1.setDatatype("decimal");
        newCCs1.add(ccDesc1);
        updated.setComputedColumnDescs(newCCs1);
        List<ComputedColumnDesc> newCCs2 = Lists.newArrayList(deserialized.getComputedColumnDescs());
        ComputedColumnDesc ccDesc2 = new ComputedColumnDesc();
        ccDesc2.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc2.setColumnName("CC2");
        ccDesc2.setExpression("CC1 * 2");
        ccDesc2.setDatatype("decimal");
        newCCs2.add(ccDesc1);
        newCCs2.add(ccDesc2);
        updated.setComputedColumnDescs(newCCs2);

        Assert.assertEquals("CC1 * 2", ccDesc2.getInnerExpression());
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT` + 1) * 2",
                ccDesc2.getInnerExpression());

        ccDesc1.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 2");
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT` + 2) * 2",
                ccDesc2.getInnerExpression());

        ccDesc2.setExpression("CC1 * 3");
        modelService.preProcessBeforeModelSave(updated, "default");
        Assert.assertEquals("(`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT` + 2) * 3",
                ccDesc2.getInnerExpression());
    }

    @Test
    @Ignore("will create cube with model")
    public void testBuildSegmentsManuallyException1() throws Exception {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "match")
                .getDataModelDesc("match");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setManagementType(ManagementType.MODEL_BASED);
        modelRequest.setLastModified(0L);
        modelRequest.setProject("match");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not build segments, please define table index or aggregate index first!");
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelService.buildSegmentsManually("match", "new_model", "0", "100");
    }

    public void testBuildSegmentsManually() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val jobInfo = modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "1577811661000", "1609430400000", true, Sets.newHashSet(), null, 0, false);

        Assert.assertEquals(jobInfo.getJobs().size(), 1);
        Assert.assertEquals(jobInfo.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals("yyyy-MM-dd", modelDesc.getPartitionDesc().getPartitionDateFormat());

        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);

        String pattern = "yyyy-MM-dd";
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(0, executables.get(1).getPriority());
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1577808000000", pattern),
                dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1609430400000", pattern),
                dataflow.getSegments().get(0).getSegRange().getEnd());

        // multi-partition model
        String multiPartitionModelUuid = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        dataflow = dataflowManager.getDataflow(multiPartitionModelUuid);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val jobInfo2 = modelService.buildSegmentsManually("default", multiPartitionModelUuid, "1577811661000",
                "1609430400000", true, Sets.newHashSet(), null, 0, true);
        Assert.assertEquals(1, jobInfo2.getJobs().size());
        Assert.assertEquals(jobInfo2.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val job2 = NExecutableManager.getInstance(getTestConfig(), "default")
                .getJob(jobInfo2.getJobs().get(0).getJobId());
        Assert.assertEquals(3, job2.getTargetPartitions().size());

    }

    public void testBuildSegmentsManually_WithPushDown() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val minAndMaxTime = PushDownUtil.getMaxAndMinTime(modelUpdate.getPartitionDesc().getPartitionDateColumn(),
                modelUpdate.getRootFactTableName(), "default");
        val dateFormat = DateFormat.proposeDateFormat(minAndMaxTime.getFirst());
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                DateFormat.getFormattedDate(minAndMaxTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(minAndMaxTime.getSecond(), dateFormat));
        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT));
        sdf.setTimeZone(TimeZone.getDefault());

        long t1 = sdf.parse("2012/01/01").getTime();
        long t2 = sdf.parse("2014/01/01").getTime();

        Assert.assertEquals(t1, dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(t2, dataflow.getSegments().get(0).getSegRange().getEnd());
        val result = PushDownUtil.getMaxAndMinTimeWithTimeOut(modelUpdate.getPartitionDesc().getPartitionDateColumn(),
                modelUpdate.getRootFactTableName(), "default");
        Assert.assertNotNull(result);
    }

    private void prepareModelToManually(String project, String modelId) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
    }

    private void cleanSegment(String project, String modelId) {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflow = dataflowManager.updateDataflow(dataflowUpdate);
        Assert.assertEquals(0, dataflow.getSegments().size());

    }

    @Test
    public void testBuildSegmentsManually_IncrementBuild_ChangePartition() throws Exception {
        for (String timeZone : timeZones) {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            DateFormat.cleanCache();

            String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            String project = getProject();
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            modelManager.updateDataModel(modelId, copyForWrite -> {
                copyForWrite.setManagementType(ManagementType.MODEL_BASED);
                copyForWrite.setPartitionDesc(null);
            });
            String pattern = "yyyyMMdd";
            PartitionDesc partitionDesc = new PartitionDesc();
            partitionDesc.setPartitionDateColumn("TEST_KYLIN_FACT.CAL_DT");
            partitionDesc.setPartitionDateFormat(pattern);
            modelService.incrementBuildSegmentsManually(project, modelId, "1577811661000", "1609430400000",
                    partitionDesc, null);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            var dataflow = dataflowManager.getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertEquals(DateFormat.getFormatTimeStamp("1577808000000", pattern),
                    dataflow.getSegments().get(0).getSegRange().getStart());
        }
    }

    @Test
    public void testBuildSegmentManually_PartitionValue_Not_Support() throws Exception {
        List<String[]> multiPartitionValues = Lists.newArrayList();
        multiPartitionValues.add(new String[] { "cn" });
        try {
            modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "", true,
                    Sets.newHashSet(), multiPartitionValues);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    "Model \"nmodel_basic\" hasn’t set a partition column yet. Please set it first and try again."));
        }
    }

    @Test
    public void testBuildSegmentsManually_NoPartition_Exception() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        modelManager.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            copyForWrite.setPartitionDesc(null);
        });

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflow = dataflowManager.updateDataflow(dataflowUpdate);
        Assert.assertEquals(0, dataflow.getSegments().size());
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "", true,
                Sets.newHashSet(), null, 0, false);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertTrue(dataflow.getSegments().get(0).getSegRange().isInfinite());
        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
        AbstractExecutable job = executables.get(0);
        Assert.assertEquals(0, job.getPriority());
        Assert.assertTrue(((NSparkCubingJob) job).getHandler() instanceof ExecutableAddSegmentHandler);
        thrown.expectInTransaction(KylinException.class);
        thrown.expectMessageInTransaction(String.format(Locale.ROOT,
                MsgPicker.getMsg().getSEGMENT_STATUS(SegmentStatusEnumToDisplay.LOADING.name()), dataflowManager
                        .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().get(0).displayIdName()));
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
    }

    @Test
    @Ignore
    public void testBuildSegmentsManually_NoPartition_FullSegExisted() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        val request = new ModelRequest(JsonUtil.deepCopy(modelDesc, NDataModel.class));
        request.setSimplifiedMeasures(modelDesc.getEffectiveMeasures().values().stream()
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.getAllNamedColumns().forEach(c -> c.setName(c.getAliasDotColumn().replace(".", "_")));
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toList()));
        request.setComputedColumnDescs(request.getComputedColumnDescs());
        request.setPartitionDesc(null);
        request.setProject(project);
        modelService.updateDataModelSemantic(project, request);
        try {
            modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        } catch (TransactionException exception) {
            Assert.assertTrue(exception.getCause() instanceof KylinException);
            Assert.assertEquals(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL(), exception.getCause().getMessage());
        }
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
    }

    @Test
    public void testUpdateModelDataCheckDesc() {
        modelService.updateModelDataCheckDesc("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 7, 10, 2);
        final NDataModel dataModel = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        final DataCheckDesc dataCheckDesc = dataModel.getDataCheckDesc();
        Assert.assertEquals(7, dataCheckDesc.getCheckOptions());
        Assert.assertEquals(10, dataCheckDesc.getFaultThreshold());
        Assert.assertEquals(2, dataCheckDesc.getFaultActions());
    }

    @Test
    public void testGetAffectedModelsByToggleTableType() {
        val response = modelService.getAffectedModelsByToggleTableType("DEFAULT.TEST_KYLIN_FACT", "default");
        Assert.assertEquals(4, response.getModels().size());
        Assert.assertEquals(5633024L, response.getByteSize());
    }

    @Test
    @Ignore
    public void testSetIncrementing_LimitedFactTable_exception() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val joinTableDesc = new JoinTableDesc();
        joinTableDesc.setTable("DEFAULT.TEST_KYLIN_FACT");
        model.setJoinTables(Lists.newArrayList(joinTableDesc));
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_KYLIN_FACT' incremental loading, due to another incremental loading table existed in model 'nmodel_basic'!");
        modelService.checkSingleIncrementingLoadingTable("default", "DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testUpdateAndGetModelConfig() {
        val project = "default";
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelConfigRequest = new ModelConfigRequest();
        modelConfigRequest.setProject(project);
        modelConfigRequest.setAutoMergeEnabled(false);
        modelConfigRequest.setAutoMergeTimeRanges(Lists.newArrayList(AutoMergeTimeEnum.WEEK));
        modelService.updateModelConfig(project, model, modelConfigRequest);

        var modelConfigResponses = modelService.getModelConfig(project, null);
        modelConfigResponses.forEach(modelConfigResponse -> {
            if (modelConfigResponse.getModel().equals(model)) {
                Assert.assertEquals(false, modelConfigResponse.getAutoMergeEnabled());
                Assert.assertEquals(1, modelConfigResponse.getAutoMergeTimeRanges().size());
            }
        });

        // get model config by fuzzy matching model alias
        modelConfigResponses = modelService.getModelConfig(project, "nmodel");
        Assert.assertEquals(3, modelConfigResponses.size());
        modelConfigResponses.forEach(modelConfigResponse -> {
            Assert.assertTrue(modelConfigResponse.getAlias().contains("nmodel"));
        });
    }

    @Test
    public void testUpdateModelConfig_BaseCuboid() {
        val configKey = "kylin.cube.aggrgroup.is-base-cuboid-always-valid";
        val project = "default";
        val model = "82fa7671-a935-45f5-8779-85703601f49a";
        val modelConfigRequest = new ModelConfigRequest();
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        long initialSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();

        modelConfigRequest.setOverrideProps(new LinkedHashMap<String, String>() {
            {
                put(configKey, "false");
            }
        });
        modelService.updateModelConfig(project, model, modelConfigRequest);

        long updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize - 1, updatedSize);

        var modelConfigResponses = modelService.getModelConfig(project, null);
        modelConfigResponses.forEach(modelConfigResponse -> {
            if (modelConfigResponse.getModel().equals(model)) {
                Assert.assertEquals("false", modelConfigResponse.getOverrideProps().get(configKey));
            }
        });

        modelConfigRequest.setOverrideProps(new LinkedHashMap<String, String>());

        modelService.updateModelConfig(project, model, modelConfigRequest);
        updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize, updatedSize);
    }

    @Test
    public void testUpdateModelConfigBringBackDeletedLayout() {
        val project = "default";
        val model = "82fa7671-a935-45f5-8779-85703601f49a";
        val modelConfigRequest = new ModelConfigRequest();
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        long initialSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        indexPlanService.removeIndexes(project, model, Sets.newHashSet(10001L, 20001L));
        long updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize - 2, updatedSize);
        // override prop other than is-base-cuboid-always-valid
        modelConfigRequest.setOverrideProps(new LinkedHashMap<String, String>() {
            {
                put("kylin.query.metadata.expose-computed-column", "true");
            }
        });
        modelService.updateModelConfig(project, model, modelConfigRequest);
        updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize - 2, updatedSize);
        // switch off is-base-cuboid-always-valid
        modelConfigRequest.setOverrideProps(new LinkedHashMap<String, String>() {
            {
                put("kylin.cube.aggrgroup.is-base-cuboid-always-valid", "false");
            }
        });
        modelService.updateModelConfig(project, model, modelConfigRequest);
        updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize - 3, updatedSize);
        // switch on is-base-cuboid-always-valid
        modelConfigRequest.setOverrideProps(new LinkedHashMap<String, String>());
        modelService.updateModelConfig(project, model, modelConfigRequest);
        updatedSize = indexPlanManager.getIndexPlan(model).getRuleBaseLayouts().size();
        Assert.assertEquals(initialSize - 2, updatedSize);
    }

    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject("default");
        job1.setName("sparkjob1");
        job1.setTargetSubject("741ca86a-1f13-46da-a59f-95fb68615e3a");
        SucceedChainedTestExecutable job2 = new SucceedChainedTestExecutable();
        job2.setProject("default");
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");
        SucceedChainedTestExecutable job3 = new SucceedChainedTestExecutable();
        job3.setProject("default");
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }

    @Test
    public void testIllegalCreateModelRequest() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");

        List<NDataModel.NamedColumn> namedColumns = modelRequest.getAllNamedColumns().stream()
                .filter(col -> col.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList());

        // duplicate dimension names
        NDataModel.NamedColumn dimension = new NDataModel.NamedColumn();
        dimension.setId(38);
        dimension.setName("CAL_DT1");
        dimension.setAliasDotColumn("TEST_CAL_DT.CAL_DT");
        dimension.setStatus(NDataModel.ColumnStatus.DIMENSION);

        namedColumns.add(dimension);
        modelRequest.setSimplifiedDimensions(namedColumns);
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertTrue(StringUtils.contains(ex.getMessage(),
                    "Dimension name \"CAL_DT1\" already exists. Please rename it."));
        }

        // invalid dimension name
        dimension.setName("CAL_DT1@!");
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertTrue(StringUtils.contains(ex.getMessage(),
                    "The dimension name \"CAL_DT1@!\" is invalid. Please use only characters, numbers, spaces and symbol(_ -()%?). "
                            + getTestConfig().getMaxModelDimensionMeasureNameLength()
                            + " characters at maximum are supported."));
        }

        StringBuilder name = new StringBuilder();
        for (int i = 0; i < getTestConfig().getMaxModelDimensionMeasureNameLength() + 1; ++i)
            name.append('a');
        dimension.setName(name.toString());
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertTrue(StringUtils.contains(ex.getMessage(),
                    getTestConfig().getMaxModelDimensionMeasureNameLength() + " characters at maximum are supported."));
        }

        namedColumns.remove(dimension);

        // invalid measure name
        List<SimplifiedMeasure> measures = Lists.newArrayList();
        SimplifiedMeasure measure1 = new SimplifiedMeasure();
        measure1.setName("illegal_measure_name@!");
        measure1.setExpression("COUNT_DISTINCT");
        measure1.setReturnType("hllc(10)");
        ParameterResponse parameterResponse = new ParameterResponse("column", "TEST_KYLIN_FACT");
        measure1.setParameterValue(Lists.newArrayList(parameterResponse));
        measures.add(measure1);
        modelRequest.setSimplifiedMeasures(measures);

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(KylinException.class, e.getClass());
            Assert.assertTrue(StringUtils.contains(e.getMessage(),
                    "The measure name \"illegal_measure_name@!\" is invalid. Please use Chinese or English characters, numbers, spaces or symbol(_ -()%?). "
                            + getTestConfig().getMaxModelDimensionMeasureNameLength()
                            + " characters at maximum are supported."));
        }

        // duplicate measure name
        measure1.setName("count_1");

        SimplifiedMeasure measure2 = new SimplifiedMeasure();
        measure2.setName("count_1");
        measure2.setExpression("COUNT_DISTINCT");
        measure2.setReturnType("hllc(10)");
        measure2.setParameterValue(Lists.newArrayList(parameterResponse));
        measures.add(measure2);

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(KylinException.class, e.getClass());
            Assert.assertTrue(
                    StringUtils.contains(e.getMessage(), "Measure name \"count_1\" already exists. Please rename it."));
        }

        // duplicate measure definitions
        measure2.setName("count_2");

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(KylinException.class, e.getClass());
            Assert.assertTrue(StringUtils.contains(e.getMessage(),
                    "The definition of this measure  is the same as measure \"count_2\". Please modify it."));
        }

        measures.remove(measure2);

        // duplicate join conditions
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setAlias("TEST_ACCOUNT");
        joinTableDesc.setTable("DEFAULT.TEST_ACCOUNT");
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType("INNER");
        joinDesc.setPrimaryKey(new String[] { "TEST_ACCOUNT.ACCOUNT_ID", "TEST_ACCOUNT.ACCOUNT_ID" });
        joinDesc.setForeignKey(new String[] { "TEST_KYLIN_FACT.SELLER_ID", "TEST_KYLIN_FACT.SELLER_ID" });

        joinTableDesc.setJoin(joinDesc);
        modelRequest.setJoinTables(Lists.newArrayList(joinTableDesc));

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(KylinException.class, e.getClass());
            Assert.assertTrue(StringUtils.contains(e.getMessage(),
                    "Can’t create the join condition between \"TEST_ACCOUNT.ACCOUNT_ID\" and \"TEST_KYLIN_FACT.SELLER_ID\", because a same one already exists."));
        }
    }

    @Test
    public void testCreateModelWithFilterCondition() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setStart("0");
        modelRequest.setEnd("100");
        modelRequest.setUuid(null);
        modelRequest.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");

        String filterCond = "trans_id = 0 and TEST_KYLIN_FACT.order_id < 100 and DEAL_AMOUNT > 123";
        String expectedFilterCond = "(((TEST_KYLIN_FACT.TRANS_ID = 0) AND (TEST_KYLIN_FACT.ORDER_ID < 100)) AND ((TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) > 123))";
        modelRequest.setFilterCondition(filterCond);

        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);

        Assert.assertEquals(expectedFilterCond, newModel.getFilterCondition());
        modelManager.dropModel(newModel);
    }

    @Test
    public void testBuildIndexManually_TableOriented_exception() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t manually build indexes in model \"all_fixed_length\" under the current project settings.");
        modelService.buildIndicesManually(modelId, project, 3);
    }

    @Test
    public void testBuildIndexManually() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        List<NDataLayout> tobeRemoveCuboidLayouts = Lists.newArrayList();
        Segments<NDataSegment> segments = df.getSegments();
        for (NDataSegment segment : segments) {
            tobeRemoveCuboidLayouts.addAll(segment.getLayoutsMap().values());
        }
        dfUpdate.setToRemoveLayouts(tobeRemoveCuboidLayouts.toArray(new NDataLayout[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelService.buildIndicesManually(modelId, project, 0);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        Assert.assertEquals(0, executables.get(0).getPriority());

    }

    @Test
    public void testBuildIndexManuallyWithoutLayout() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelService.buildIndicesManually(modelId, project, 3);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_LAYOUT, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testBuildIndexManuallyWithoutSegment() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelService.buildIndicesManually(modelId, project, 3);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_SEGMENT, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(0, executables.size());

    }

    //test refreshSegments:all Online model, all lag beghind model, One Online One lag behind model
    //first test exception
    @Test
    public void testRefreshSegments_AffectedSegmentRangeChanged_Exception() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t refresh at the moment, as the segment range has changed. Please try again later.");
        RefreshAffectedSegmentsResponse response = new RefreshAffectedSegmentsResponse();
        response.setAffectedStart("12");
        response.setAffectedEnd("120");
        Mockito.doReturn(response).when(modelService).getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.TEST_KYLIN_FACT", "0", "12223334");
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_NoSegments_Exception() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("No segments to refresh, please select new range and try again!");
        List<NDataSegment> segments = modelService.getSegmentsByRange("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));

        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_ENCODING");
        loadingRange.setColumnName("TEST_ENCODING.int_dict");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, 12223334L));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelService.refreshSegments("default", "DEFAULT.TEST_ENCODING", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_TwoOnlineModelHasNewSegment_Exception() throws IOException {
        prepareTwoOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        dfMgr.refreshSegment(df, df.getSegments().get(0).getSegRange());

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getSEGMENT_CAN_NOT_REFRESH());
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_OneLagBehindAndOneOnlineModel_LagBehindHasRefreshingException()
            throws IOException {
        prepareOneLagBehindAndOneOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");

        val df = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        dfMgr.refreshSegment(df, df.getSegments().get(0).getSegRange());

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t refresh some segments, as they are being built at the moment. Please try again later.");
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    //now test cases without exception
    @Test
    public void testRefreshSegmentsByDataRange_TwoOnlineModelAndHasReadySegs() throws IOException {
        prepareTwoOnlineModels();
        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");

        val executables = getRunningExecutables("default", null);

        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoOnlineModelNoExistedSegmentAndFullBuild() throws IOException {
        prepareTwoOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df1 = dfMgr.getDataflowByModelAlias("nmodel_basic");
        val df2 = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        //purge segments first
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToRemoveSegs(df1.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df2.getUuid());
        update2.setToRemoveSegs(df2.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);

        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");

        val executables = getRunningExecutables("default", null);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoLagBehindModelAndNoReadySegs() throws IOException {
        prepareTwoLagBehindModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        dfMgr.appendSegment(df_basic, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        val oldSeg = dfMgr.appendSegment(df_basic_inner, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");
        val executables = getRunningExecutables("default", null);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableAddSegmentHandler);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNotSame(df_basic_inner.getSegments().get(0).getId(), oldSeg.getId());
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoLagBehindModelAndHasReadySegs() throws IOException {
        prepareTwoLagBehindModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var firstSegment = dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update1 = new NDataflowUpdate(df_basic.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update1.setToUpdateSegs(firstSegment);
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        firstSegment = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(firstSegment);
        dfMgr.updateDataflow(update2);

        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "20", "0", "20");
        val executables = getRunningExecutables("default", null);
        //refresh 2 ready segs and rebuild two new segs
        Assert.assertEquals(4, executables.size());
        executables.sort(Comparator.comparing(AbstractExecutable::getJobType));
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(2)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(3)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_OneLagBehindOneOnlineModelAndHasReadySegs() throws IOException {
        prepareOneLagBehindAndOneOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var firstSegment = dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update1 = new NDataflowUpdate(df_basic.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update1.setToUpdateSegs(firstSegment);
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        firstSegment = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        val secondSeg = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        secondSeg.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(firstSegment, secondSeg);
        dfMgr.updateDataflow(update2);

        modelService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "20", "0", "20");

        val executables = getRunningExecutables("default", null);
        executables.sort(Comparator.comparing(AbstractExecutable::getJobType));
        //refresh 2 ready segs in online model and one ready seg in lag behind and rebuild one new seg in lag behind
        Assert.assertEquals(4, executables.size());
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(2)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(3)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    @Test
    public void testGetCubes() {
        Mockito.doReturn(Sets.newHashSet("default")).when(modelService).getAllProjects();
        List<NDataModelResponse> responses = modelService.getCubes("nmodel_full_measure_test", "default");
        Assert.assertEquals(1, responses.size());

        List<NDataModelResponse> responses1 = modelService.getCubes("nmodel_full_measure_test", null);
        Assert.assertEquals(1, responses.size());

        NDataModelResponse response = modelService.getCube("nmodel_full_measure_test", "default");
        Assert.assertNotNull(response);

        NDataModelResponse response1 = modelService.getCube("nmodel_full_measure_test", null);
        Assert.assertNotNull(response1);
    }

    @Test
    public void testAddOldParams() {
        // normal model
        List<NDataModelResponse> modelResponseList = modelService.getModels("nmodel_full_measure_test", "default",
                false, "", null, "last_modify", true);
        Assert.assertEquals(1, modelResponseList.size());
        Assert.assertTrue(Objects.isNull(modelResponseList.get(0).getOldParams()));

        List<NDataModel> models = new ArrayList<>(modelResponseList);
        modelService.addOldParams("default", models);
        NDataModelResponse model = modelResponseList.get(0);
        Assert.assertTrue(Objects.nonNull(model.getOldParams()));
        Assert.assertEquals(100, model.getOldParams().getInputRecordSizeBytes());

        // broken model
        String brokenModelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(brokenModelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);
        NDataModelResponse brokenModelResponse = new NDataModelResponse(brokenModel);
        brokenModelResponse.setBroken(brokenModel.isBroken());
        Assert.assertTrue(Objects.isNull(brokenModelResponse.getOldParams()));

        List<NDataModelResponse> brokenModelResponseList = Lists.newArrayList(brokenModelResponse);
        List<NDataModel> brokenModels = modelService.addOldParams("default", new ArrayList<>(brokenModelResponseList));
        Assert.assertEquals(1, brokenModels.size());
        Assert.assertTrue(Objects.nonNull(brokenModelResponse.getOldParams()));
        Assert.assertEquals(0, brokenModelResponse.getOldParams().getInputRecordSizeBytes());
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
    public void testUpdateModel_CleanRecommendation() throws Exception {
        val modelRequest = prepare();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        modelRequest.setSimplifiedMeasures(
                modelRequest.getSimplifiedMeasures().stream().filter(measure -> measure.getId() != 100001)
                        .sorted(Comparator.comparingInt(SimplifiedMeasure::getId)).collect(Collectors.toList()));
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), "default").updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> copyForWrite.setIndexes(new ArrayList<>()));
            return 0;
        }, "default");
        modelService.updateDataModelSemantic("default", modelRequest);
    }

    @Test
    public void testRemoveRecommendAggIndexDimensionColumn() throws Exception {
        val modelRequest = prepare();
        modelRequest.getSimplifiedDimensions().remove(0);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The dimension TEST_SITES.SITE_NAME is being referenced by aggregation group, "
                + "recommended aggregate index or table index. Please delete this dimension from the above first.");
        modelService.updateDataModelSemantic("default", modelRequest);
    }

    @Test
    public void testRemoveRecommendAggIndexMeasureColumn() throws Exception {
        val modelRequest = prepare();
        modelRequest.setSimplifiedMeasures(
                modelRequest.getSimplifiedMeasures().stream().filter(measure -> measure.getId() != 100005)
                        .sorted(Comparator.comparingInt(SimplifiedMeasure::getId)).collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage("The measure ITEM_COUNT_MAX is referenced by indexes. Please try again after "
                + "deleting it from aggregation group or table index.");
        modelService.updateDataModelSemantic("default", modelRequest);
    }

    @Test
    public void testCheckBeforeModelSave() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel okModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        okModel.setFilterCondition("TEST_KYLIN_FACT.SELLER_ID > 0");
        ModelRequest okModelRequest = new ModelRequest(okModel);
        okModelRequest.setProject(project);
        Mockito.when(semanticService.convertToDataModel(okModelRequest)).thenReturn(okModel);
        modelService.checkBeforeModelSave(okModelRequest);
    }

    @Test
    public void testMassageModelFilterCondition() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager
                .copyForWrite(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        String originSql = "trans_id = 0 and TEST_KYLIN_FACT.order_id < 100 and DEAL_AMOUNT > 123";
        model.setFilterCondition(originSql);
        modelService.massageModelFilterCondition(model);
        Assert.assertEquals(
                "(((TEST_KYLIN_FACT.TRANS_ID = 0) AND (TEST_KYLIN_FACT.ORDER_ID < 100)) AND ((TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) > 123))",
                model.getFilterCondition());
    }

    @Test
    public void testAddTableNameIfNotExist() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        String originSql = "trans_id = 0 and TEST_KYLIN_FACT.order_id < 100";
        String newSql = modelService.addTableNameIfNotExist(originSql, model);
        Assert.assertEquals("((TEST_KYLIN_FACT.TRANS_ID = 0) AND (TEST_KYLIN_FACT.ORDER_ID < 100))", newSql);
        originSql = "trans_id between 1 and 10";
        newSql = modelService.addTableNameIfNotExist(originSql, model);
        Assert.assertEquals("(TEST_KYLIN_FACT.TRANS_ID BETWEEN 1 AND 10)", newSql);

        modelManager.updateDataModel(model.getUuid(), copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.get(0).setFlattenable(JoinTableDesc.NORMALIZED);
            copyForWrite.setJoinTables(joinTables);
        });
        NDataModel updatedModel = modelManager.getDataModelDesc(model.getUuid());

        try {
            originSql = "TEST_ORDER.ORDER_ID > 10";
            modelService.addTableNameIfNotExist(originSql, updatedModel);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals("KE-010011006", e.getErrorCode().getCodeString());
            Assert.assertEquals(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getFILTER_CONDITION_ON_ANTI_FLATTEN_LOOKUP(), "TEST_ORDER"), e.getMessage());
        }

    }

    @Test
    public void testGetCubeWithExactModelName() {
        NCubeDescResponse cube = modelService.getCubeWithExactModelName("ut_inner_join_cube_partial", "default");
        Assert.assertTrue(cube.getDimensions().size() == 13);
        Assert.assertTrue(cube.getMeasures().size() == 11);
        Assert.assertTrue(cube.getAggregationGroups().size() == 2);
        Set<String> derivedCol = Sets.newHashSet();
        for (val dim : cube.getDimensions()) {
            if (dim.getDerived() != null) {
                derivedCol.add(dim.getDerived().get(0));
            }
        }
        Assert.assertTrue(derivedCol.size() == 1);
        Assert.assertTrue(derivedCol.contains("SITE_NAME"));
    }

    @Test
    public void testGetModelDesc() {
        // model1: model with only rule_based_index
        NModelDescResponse model1 = modelService.getModelDesc("ut_inner_join_cube_partial", "default");
        Assert.assertEquals("default", model1.getProject());
        Assert.assertEquals(11, model1.getMeasures().size());
        Assert.assertEquals(2, model1.getAggregationGroups().size());
        Assert.assertNotEquals(0, model1.getCreateTime());
        Assert.assertEquals(24, model1.getDimensions().size());
        Assert.assertSame("DIMENSION", model1.getDimensions().get(3).getNamedColumn().getStatus().name());
        Assert.assertSame("DIMENSION", model1.getDimensions().get(5).getNamedColumn().getStatus().name());

        // model2: model with rule_based_index and table indexes, with overlap between their dimensions
        NModelDescResponse model2 = modelService.getModelDesc("nmodel_basic_inner", "default");
        Assert.assertEquals(31, model2.getDimensions().size());
        Assert.assertSame("DIMENSION", model2.getDimensions().get(0).getNamedColumn().getStatus().name());
        Assert.assertSame("DIMENSION", model2.getDimensions().get(1).getNamedColumn().getStatus().name());
    }

    @Test
    public void testComputedColumnNameCheck_PreProcessBeforeModelSave_ExceptionWhenCCNameIsSameWithColumnInLookupTable() {

        expectedEx.expect(KylinException.class);
        expectedEx.expectMessage(
                "The computed column name \"SITE_ID\" has been used in the current model. Please rename it.");
        String tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "SITE_ID";
        String expression = "nvl(TEST_SITES.SITE_ID)";
        String dataType = "integer";
        ComputedColumnDesc ccDesc = new ComputedColumnDesc();
        ccDesc.setTableIdentity(tableIdentity);
        ccDesc.setColumnName(columnName);
        ccDesc.setExpression(expression);
        ccDesc.setDatatype(dataType);

        String project = "default";
        NDataModelManager dataModelManager = modelService.getDataModelManager("default");
        NDataModel model = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        model.getComputedColumnDescs().add(ccDesc);

        modelService.preProcessBeforeModelSave(model, project);
    }

    @Test
    public void testComputedColumnNameCheck_CheckCC_ExceptionWhenCCNameIsSameWithColumnInLookupTable() {

        expectedEx.expect(KylinException.class);
        expectedEx.expectMessage(
                "The computed column name \"SITE_ID\" has been used in the current model. Please rename it.");
        String tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "SITE_ID";
        String expression = "nvl(TEST_SITES.SITE_ID)";
        String dataType = "integer";
        ComputedColumnDesc ccDesc = new ComputedColumnDesc();
        ccDesc.setTableIdentity(tableIdentity);
        ccDesc.setColumnName(columnName);
        ccDesc.setExpression(expression);
        ccDesc.setDatatype(dataType);

        String project = "default";
        NDataModelManager dataModelManager = modelService.getDataModelManager("default");
        NDataModel model = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        model.getComputedColumnDescs().add(ccDesc);

        modelService.checkComputedColumn(model, project, null);
    }

    private NDataSegment mockSegment() {
        NDataSegment segment = Mockito.mock(NDataSegment.class);
        Map<Long, NDataLayout> layoutMap = Maps.newHashMap();
        layoutMap.put(1L, new NDataLayout());
        layoutMap.put(10001L, new NDataLayout());
        layoutMap.put(10002L, new NDataLayout());
        layoutMap.put(1030001L, new NDataLayout());
        layoutMap.put(1080001L, new NDataLayout());
        layoutMap.put(1040001L, new NDataLayout());
        Mockito.doAnswer(invocationOnMock -> layoutMap).when(segment).getLayoutsMap();
        return segment;
    }

    private List<LayoutEntity> spyLayouts() {
        val id = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val index = indexPlanManager.getIndexPlan(id);
        val layouts = index.getAllLayouts();
        layouts.forEach(l -> {
            if (l.getId() == 1L || l.getId() == 10001L) {
                l.setToBeDeleted(true);
            }
        });
        return layouts;
    }

    @Test
    public void testGetAvailableIndexesCount() throws Exception {
        val id = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val alias = "nmodel_basic_inner";
        val layouts = spyLayouts();
        val segment = mockSegment();
        val dfManager = spyNDataflowManager();
        val indexPlanManager = spyNIndexPlanManager();
        AtomicBoolean f1 = new AtomicBoolean(false);
        AtomicBoolean f2 = new AtomicBoolean(false);
        spy(dfManager, m -> m.getDataflow(id), df -> {
            if (!df.getId().equals(id)) {
                return df;
            }
            NDataflow spyDf = Mockito.spy(df);
            Mockito.doAnswer(invocation -> segment).when(spyDf).getLatestReadySegment();
            return spyDf;
        });
        spy(indexPlanManager, m -> m.getIndexPlan(id), indexPlan -> {
            if (!indexPlan.getId().equals(id)) {
                return indexPlan;
            }
            IndexPlan indexPlan1 = Mockito.spy(indexPlan);
            Mockito.doAnswer(invocationOnMock -> layouts).when(indexPlan1).getAllLayouts();
            return indexPlan1;
        });
        val res = modelService.getModels(alias, getProject(), false, "", null, "last_modify", true);
        Assert.assertEquals(1, res.size());
        Assert.assertEquals(4, res.get(0).getAvailableIndexesCount());
    }

    @Test
    public void testUpdateReponseAcl() {
        List<NDataModel> models = new ArrayList<>();
        models.addAll(modelService.getModels("", "default", false, "", null, "last_modify", true));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        val adminModels = modelService.updateReponseAcl(models, "default");
        for (val model : adminModels) {
            Assert.assertTrue(((NDataModelResponse) model).getAclParams().isVisible());
            Assert.assertEquals(0, ((NDataModelResponse) model).getAclParams().getUnauthorizedTables().size());
            Assert.assertEquals(0, ((NDataModelResponse) model).getAclParams().getUnauthorizedColumns().size());
        }
        val table = NTableMetadataManager.getInstance(getTestConfig(), "default").getTableDesc("DEFAULT.TEST_ENCODING");
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), "default");
        AclTCR acl = new AclTCR();
        AclTCR.Table aclTable = new AclTCR.Table();
        AclTCR.ColumnRow aclColumnRow = new AclTCR.ColumnRow();
        AclTCR.Column aclColumns = new AclTCR.Column();
        Arrays.stream(table.getColumns()).forEach(x -> aclColumns.add(x.getName()));
        aclColumnRow.setColumn(aclColumns);
        aclTable.put("DEFAULT.TEST_ENCODING", aclColumnRow);
        acl.setTable(aclTable);
        manager.updateAclTCR(acl, "user", true);
        PasswordEncoder pwdEncoder = PasswordEncodeFactory.newUserPasswordEncoder();
        val user = new ManagedUser("user", pwdEncoder.encode("pw"), false);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(user, "ANALYST", Constant.ROLE_ANALYST));
        val noAdminModels = modelService.updateReponseAcl(models, "default");
        for (val model : noAdminModels) {
            if (model.getAlias().equals("test_encoding")) {
                Assert.assertTrue(((NDataModelResponse) model).getAclParams().isVisible());
                Assert.assertEquals(0, ((NDataModelResponse) model).getAclParams().getUnauthorizedTables().size());
                Assert.assertEquals(0, ((NDataModelResponse) model).getAclParams().getUnauthorizedColumns().size());
            } else {
                Assert.assertFalse(((NDataModelResponse) model).getAclParams().isVisible());
                Assert.assertTrue(((NDataModelResponse) model).getAclParams().getUnauthorizedTables().size() > 0);
            }
        }

    }

    @Test
    public void testCheckSegmentHole() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        var dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        modelManager.updateDataModel(modelId, model -> {
            model.setManagementType(ManagementType.MODEL_BASED);
        });
        var res = modelService.checkSegHoleIfSegDeleted(modelId, getProject(), new String[0]);
        Assert.assertEquals(0, res.getOverlapSegments().size());
        Assert.assertEquals(0, res.getSegmentHoles().size());

        var df = dataflowManager.getDataflow(modelId);
        val update = new NDataflowUpdate(modelId);
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflow(modelId);
        dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(10L, 100L));
        dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1000L, 10000L));

        val segs = dataflowManager.getDataflow(modelId).getSegments();
        res = modelService.checkSegHoleIfSegDeleted(modelId, getProject(),
                segs.subList(1, 2).stream().map(NDataSegment::getId).toArray(String[]::new));
        Assert.assertEquals(0, res.getOverlapSegments().size());
        Assert.assertEquals(1, res.getSegmentHoles().size());

        var range = new SegmentRange.TimePartitionedSegmentRange(10000L, 20000L);
        res = modelService.checkSegHoleExistIfNewRangeBuild(getProject(), modelId, "20000", "30000");
        Assert.assertEquals(0, res.getOverlapSegments().size());
        Assert.assertEquals(3, res.getSegmentHoles().size());

        res = modelService.checkSegHoleExistIfNewRangeBuild(getProject(), modelId, "1", "10");
        Assert.assertEquals(0, res.getOverlapSegments().size());
        Assert.assertEquals(1, res.getSegmentHoles().size());

        res = modelService.checkSegHoleExistIfNewRangeBuild(getProject(), modelId, "1", "5");
        Assert.assertEquals(0, res.getOverlapSegments().size());
        Assert.assertEquals(2, res.getSegmentHoles().size());
    }

    @Test
    public void testUpdateModelOwner() throws IOException {
        String project = "default";
        String owner = "test";
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";

        // normal case
        Set<String> projectManagementUsers1 = Sets.newHashSet();
        projectManagementUsers1.add("test");
        Mockito.doReturn(projectManagementUsers1).when(accessService).getProjectManagementUsers(project);

        OwnerChangeRequest ownerChangeRequest1 = new OwnerChangeRequest();
        ownerChangeRequest1.setProject(project);
        ownerChangeRequest1.setOwner(owner);

        modelService.updateModelOwner(project, modelId, ownerChangeRequest1);
        var modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        Assert.assertEquals(modelManager.getDataModelDesc(modelId).getOwner(), owner);

        // user not exists
        ownerChangeRequest1.setOwner("nonUser");
        thrown.expectMessage(
                "This user can’t be set as the model’s owner. Please select system admin, project admin or management user.");
        modelService.updateModelOwner(project, modelId, ownerChangeRequest1);

        // empty admin users, throw exception
        Set<String> projectManagementUsers2 = Sets.newHashSet();
        Mockito.doReturn(projectManagementUsers2).when(accessService).getProjectManagementUsers(project);

        OwnerChangeRequest ownerChangeRequest = new OwnerChangeRequest();
        ownerChangeRequest.setProject(project);
        ownerChangeRequest.setOwner(owner);

        thrown.expectMessage("Illegal users!"
                + " Only the system administrator, project administrator role, and management role can be set as the model owner.");
        modelService.updateModelOwner(project, modelId, ownerChangeRequest);
    }

    @Test
    public void testUpdateModelOwnerException() throws IOException {
        String project = "default";
        String owner = "test";

        // can not found model, throw exception
        Set<String> projectManagementUsers3 = Sets.newHashSet();
        Mockito.doReturn(projectManagementUsers3).when(accessService).getProjectManagementUsers(project);

        OwnerChangeRequest ownerChangeRequest3 = new OwnerChangeRequest();
        ownerChangeRequest3.setProject(project);
        ownerChangeRequest3.setOwner(owner);

        String modelId = UUID.randomUUID().toString();
        thrown.expectMessage(
                String.format(Locale.ROOT, "Model %s does not exist or broken in project %s", modelId, project));
        modelService.updateModelOwner(project, modelId, ownerChangeRequest3);

        // test broken model, throw exception
        String brokenModelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(brokenModelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        thrown.expectMessage(
                String.format(Locale.ROOT, "Model %s does not exist or broken in project %s", brokenModelId, project));
        modelService.updateModelOwner(project, brokenModelId, ownerChangeRequest3);
    }

    @Test
    public void testGetCubes0ExistBrokenModel() {
        tableService.unloadTable(getProject(), "DEFAULT.TEST_KYLIN_FACT", false);
        val result = modelService.getCubes0(null, getProject());
        Assert.assertEquals(7, result.size());

        boolean notBrokenModel = result.stream()
                .filter(model -> "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94".equals(model.getUuid()))
                .allMatch(NDataModelResponse::isModelBroken);
        Assert.assertFalse(notBrokenModel);

        boolean brokenModel = result.stream()
                .filter(model -> "82fa7671-a935-45f5-8779-85703601f49a".equals(model.getUuid()))
                .allMatch(NDataModelResponse::isModelBroken);
        Assert.assertTrue(brokenModel);

        int joinTablesSize = result.stream()
                .filter(model -> "cb596712-3a09-46f8-aea1-988b43fe9b6c".equals(model.getUuid())).findFirst().get()
                .getOldParams().getJoinTables().size();
        Assert.assertEquals(1, joinTablesSize);
    }

    @Test
    public void testCheckSegments() {
        CheckSegmentResponse response = modelService.checkSegments("default", "all_fixed_length", "0",
                Long.MAX_VALUE + "");
        Assert.assertEquals(1, response.getSegmentsOverlap().size());
        Assert.assertEquals("11124840-b3e3-43db-bcab-2b78da666d00",
                response.getSegmentsOverlap().get(0).getSegmentId());
        Assert.assertEquals("20171104141833_20171105141833", response.getSegmentsOverlap().get(0).getSegmentName());

        response = modelService.checkSegments("default", "all_fixed_length", "0", "100");
        Assert.assertEquals(0, response.getSegmentsOverlap().size());
    }

    @Test
    public void testCheckSegmentWithBrokenModel() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Failed to get segment information as broken is broken");
        modelService.checkSegments("gc_test", "broken", "0", "100");
    }

    @Test
    public void testConvertSegmentIdWithName_NotExistName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t find the segment by name \"not exist name1,not exist name2\". Please check and try again.");

        modelService.convertSegmentIdWithName("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default", null,
                new String[] { "not exist name1", "not exist name2" });
    }

    @Test
    public void testConvertSegmentIdWithName_ByName() {
        String[] segIds = modelService.convertSegmentIdWithName("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default", null,
                new String[] { "20171104141833_20171105141833" });
        String[] originSegIds = { "11124840-b3e3-43db-bcab-2b78da666d00" };
        Assert.assertTrue(ArrayUtils.isEquals(segIds, originSegIds));
    }

    @Test
    public void testCheckSegmentsExistById() {
        boolean existed = modelService.checkSegmentsExistById("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default",
                new String[] { "11124840-b3e3-43db-bcab-2b78da666d00" }, false);
        Assert.assertTrue(existed);

        existed = modelService.checkSegmentsExistById("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default",
                new String[] { "11124840-b3e3-43db-bcab-2b78da666d00_not" }, false);
        Assert.assertFalse(existed);
    }

    @Test
    public void testCheckSegmentsExistByName() {
        boolean existed = modelService.checkSegmentsExistByName("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default",
                new String[] { "20171104141833_20171105141833" }, false);
        Assert.assertTrue(existed);
        existed = modelService.checkSegmentsExistByName("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default",
                new String[] { "20171104141833_20171105141833_not" }, false);
        Assert.assertFalse(existed);
    }

    @Test
    public void testGetPartitionColumnFormat() {
        String partitionColumnFormat = modelService.getPartitionColumnFormatById("default",
                "82fa7671-a935-45f5-8779-85703601f49a");
        Assert.assertEquals("yyyy-MM-dd", partitionColumnFormat);

        partitionColumnFormat = modelService.getPartitionColumnFormatByAlias("default", "ut_inner_join_cube_partial");
        Assert.assertEquals("yyyy-MM-dd", partitionColumnFormat);

        partitionColumnFormat = modelService.getPartitionColumnFormatById("gc_test",
                "e0e90065-e7c3-49a0-a801-20465ca64799");
        Assert.assertEquals(null, partitionColumnFormat);

        partitionColumnFormat = modelService.getPartitionColumnFormatByAlias("gc_test", "m1");
        Assert.assertEquals(null, partitionColumnFormat);

        // broken model
        String brokenModelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(brokenModelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);
        partitionColumnFormat = modelService.getPartitionColumnFormatByAlias("default", "nmodel_basic_inner");
        Assert.assertEquals(null, partitionColumnFormat);
    }

    @Test
    public void testModelSelectedColumns() {
        NDataModelResponse model = modelService
                .getModels("nmodel_basic", "default", false, "", null, "last_modify", true).get(0);

        Set<String> dimCols = model.getAllNamedColumns().stream()
                .filter(col -> col.getStatus() == NDataModel.ColumnStatus.DIMENSION)
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet());

        Set<String> colsInMeasure = model.getMeasures().stream()
                .flatMap(measure -> measure.getFunction().getColRefs().stream()).filter(Objects::nonNull)
                .map(TblColRef::getIdentity).collect(Collectors.toSet());

        Set<String> expected = new HashSet<>();
        expected.addAll(dimCols);
        expected.addAll(colsInMeasure);

        Assert.assertEquals(expected, model.getAllSelectedColumns().stream()
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet()));
    }

    @Test
    public void testModelSelectedColumns_WithTombCCColumn() {
        NDataModel model = modelService.getModels("nmodel_basic", "default", false, "", null, "last_modify", true)
                .get(0);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        model = modelManager.updateDataModel(model.getId(), copyForWrite -> {
            val col1 = new NDataModel.NamedColumn();
            col1.setId(202);
            col1.setAliasDotColumn("TEST_KYLIN_FACT.CC1");
            col1.setName("CC1");
            col1.setStatus(NDataModel.ColumnStatus.TOMB);

            val col2 = new NDataModel.NamedColumn();
            col2.setId(203);
            col2.setAliasDotColumn("TEST_KYLIN_FACT.CC1");
            col2.setName("CC1");
            copyForWrite.getAllNamedColumns().add(col1);
            copyForWrite.getAllNamedColumns().add(col2);

            try {
                val measure1 = JsonUtil.readValue("{" //
                        + "            \"name\": \"sum_cc\",\n" //
                        + "            \"function\": {\n" //
                        + "                \"expression\": \"SUM\",\n" //
                        + "                \"parameters\": [\n" //
                        + "                    {\n" //
                        + "                        \"type\": \"column\",\n" //
                        + "                        \"value\": \"TEST_KYLIN_FACT.CC1\"\n" //
                        + "                    }\n" //
                        + "                ],\n" //
                        + "                \"returntype\": \"bigint\"\n" //
                        + "            },\n" //
                        + "            \"id\": 100018,\n" //
                        + "            \"tomb\": true" //
                        + "}", NDataModel.Measure.class);
                val measure2 = JsonUtil.readValue("{" //
                        + "            \"name\": \"sum_cc\",\n" //
                        + "            \"function\": {\n" //
                        + "                \"expression\": \"SUM\",\n" //
                        + "                \"parameters\": [\n" //
                        + "                    {\n" //
                        + "                        \"type\": \"column\",\n" //
                        + "                        \"value\": \"TEST_KYLIN_FACT.CC1\"\n" //
                        + "                    }\n" //
                        + "                ],\n" //
                        + "                \"returntype\": \"bigint\"\n" //
                        + "            },\n" //
                        + "            \"id\": 100019" + "}", NDataModel.Measure.class);
                copyForWrite.getAllMeasures().add(measure1);
                copyForWrite.getAllMeasures().add(measure2);

                copyForWrite.getComputedColumnDescs()
                        .add(JsonUtil.readValue(
                                "        {\n" + "            \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                                        + "            \"tableAlias\": \"TEST_KYLIN_FACT\",\n"
                                        + "            \"columnName\": \"CC1\",\n"
                                        + "            \"expression\": \"TEST_KYLIN_FACT.PRICE+1\",\n"
                                        + "            \"datatype\": \"BIGINT\"\n" + "        }",
                                ComputedColumnDesc.class));
            } catch (IOException ignore) {
            }
        });

        Set<String> dimCols = model.getAllNamedColumns().stream()
                .filter(col -> col.getStatus() == NDataModel.ColumnStatus.DIMENSION)
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet());

        Set<String> colsInMeasure = model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .flatMap(measure -> measure.getFunction().getColRefs().stream()).filter(Objects::nonNull)
                .map(TblColRef::getIdentity).collect(Collectors.toSet());

        Set<String> expected = new HashSet<>();
        expected.addAll(dimCols);
        expected.addAll(colsInMeasure);

        Assert.assertEquals(expected, model.getAllSelectedColumns().stream()
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet()));
        Assert.assertEquals(1,
                model.getAllSelectedColumns().stream().filter(col -> col.getName().equals("CC1")).count());
    }

    @Test
    public void testModelResponseJoinSimplified() throws Exception {
        NDataModelResponse modelResponse = modelService
                .getModels("nmodel_basic", "default", false, "", null, "last_modify", true).get(0);
        Assert.assertTrue(CollectionUtils.isNotEmpty(modelResponse.getSimplifiedJoinTableDescs()));

        //1.test SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert
        String responseJson = JsonUtil.writeValueAsString(modelResponse.getJoinTables());
        List<SimplifiedJoinTableDesc> convertedSimplifiedJointables = SCD2SimplificationConvertUtil
                .simplifiedJoinTablesConvert(modelResponse.getJoinTables());

        Assert.assertEquals(JsonUtil.writeValueAsString(convertedSimplifiedJointables),
                JsonUtil.writeValueAsString(modelResponse.getSimplifiedJoinTableDescs()));

        //2.test simplified join json equal origin join
        //clear list
        modelResponse.setJoinTables(null);

        NDataModel nDataModel = JsonUtil.readValue(JsonUtil.writeValueAsString(modelResponse), NDataModel.class);
        String modelJson = JsonUtil.writeValueAsString(nDataModel.getJoinTables());
        Assert.assertEquals(responseJson, modelJson);

        //3. test deep copy model
        Assert.assertEquals(JsonUtil.writeValueAsString(nDataModel),
                JsonUtil.writeValueAsString(semanticService.deepCopyModel(nDataModel)));

    }

    @Test
    public void testConvertToRequest() throws IOException {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic");

        ModelRequest modelRequest = modelService.convertToRequest(originModel);

        String originJsonModel = JsonUtil.writeValueAsString(originModel.getJoinTables());

        String requestJson = JsonUtil.writeValueAsString(
                SCD2SimplificationConvertUtil.convertSimplified2JoinTables(modelRequest.getSimplifiedJoinTableDescs()));

        Assert.assertEquals(originJsonModel, requestJson);

    }

    @Test
    public void testCheckModelDimensionNameAndMeasureName() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);

        List<NDataModel.NamedColumn> namedColumns = modelRequest.getAllNamedColumns().stream()
                .filter(col -> col.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList());

        NDataModel.NamedColumn dimension = new NDataModel.NamedColumn();
        dimension.setId(38);
        dimension.setName("aaa中文 () （） % ? acfz ABNZ 0 8 2 _ -- end");
        dimension.setAliasDotColumn("TEST_CAL_DT.CAL_DT");
        dimension.setStatus(NDataModel.ColumnStatus.DIMENSION);

        namedColumns.add(dimension);
        modelRequest.setSimplifiedDimensions(namedColumns);

        List<SimplifiedMeasure> measures = Lists.newArrayList();
        SimplifiedMeasure measure1 = new SimplifiedMeasure();
        measure1.setName("ssa中文 () kkk?（） % ? dirz AHRZ 2 5 9 _ -- end");
        measure1.setExpression("COUNT_DISTINCT");
        measure1.setReturnType("hllc(10)");
        ParameterResponse parameterResponse = new ParameterResponse("column", "TEST_KYLIN_FACT");
        measure1.setParameterValue(Lists.newArrayList(parameterResponse));
        measures.add(measure1);
        modelRequest.setSimplifiedMeasures(measures);

        modelService.checkModelDimensions(modelRequest);
        modelService.checkModelMeasures(modelRequest);
    }

    @Test
    public void testUpdatePartitionColumn() throws IOException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        modelMgr.updateDataModel(modelId, model -> {
            model.setManagementType(ManagementType.MODEL_BASED);
        });
        modelService.updatePartitionColumn(project, modelId, null, null);
        val runningExecutables = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getRunningExecutables(project, modelId);
        Assert.assertEquals(0, runningExecutables.size());
    }

    @Test
    public void testBuildMultiPartitionSegments() throws Exception {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        val model = dataflow.getModel();
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "usa" });
        buildPartitions.add(new String[] { "Austria" });
        val segmentTimeRequests = Lists.<SegmentTimeRequest> newArrayList();
        segmentTimeRequests.add(new SegmentTimeRequest("1630425600000", "1630512000000"));

        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams(project, modelId, "1633017600000",
                "1633104000000", model.getPartitionDesc(), model.getMultiPartitionDesc(), segmentTimeRequests, true,
                buildPartitions);
        val jobInfo = modelService.incrementBuildSegmentsManually(incrParams);

        Assert.assertEquals(2, jobInfo.getJobs().size());
        Assert.assertEquals(jobInfo.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(2, executables.size());
        val job = executableManager.getJob(jobInfo.getJobs().get(0).getJobId());
        Assert.assertEquals(3, job.getTargetPartitions().size());
        Set<JobBucket> buckets = ExecutableParams.getBuckets(job.getParam(NBatchConstants.P_BUCKETS));
        Assert.assertEquals(45, buckets.size());
        NDataSegment segment = dataflowManager.getDataflow(modelId).getSegment(job.getTargetSegments().get(0));
        Assert.assertEquals(44, segment.getMaxBucketId());

        // build all partition values
        IncrementBuildSegmentParams incrParams2 = new IncrementBuildSegmentParams(project, modelId, "1633104000000",
                "1633190400000", model.getPartitionDesc(), model.getMultiPartitionDesc(), null, true, null)
                        .withBuildAllSubPartitions(true);
        val jobInfo2 = modelService.incrementBuildSegmentsManually(incrParams2);
        Assert.assertEquals(1, jobInfo2.getJobs().size());
        Assert.assertEquals(jobInfo2.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val job2 = executableManager.getJob(jobInfo2.getJobs().get(0).getJobId());
        Assert.assertEquals(4, job2.getTargetPartitions().size()); // usa,un,cn,Austria

        // change multi partition desc will clean all segments
        IncrementBuildSegmentParams incrParams3 = new IncrementBuildSegmentParams(project, modelId, "1633017600000",
                "1633104000000", model.getPartitionDesc(), null, null, true, null);
        val jobInfo3 = modelService.incrementBuildSegmentsManually(incrParams3);
        val newModel = dataflowManager.getDataflow(modelId).getModel();
        Assert.assertEquals(1, jobInfo3.getJobs().size());
        Assert.assertFalse(newModel.isMultiPartitionModel());
    }

    @Test
    public void testRefreshMultiPartitionSegments() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";
        val segmentId = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        val refreshSegmentParams = new RefreshSegmentParams(project, modelId, new String[] { segmentId });
        modelService.refreshSegmentById(refreshSegmentParams);

        val jobs = getRunningExecutables(getProject(), modelId);
        val job = jobs.get(0);
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(2, job.getTargetPartitions().size());
        Set<JobBucket> buckets = ExecutableParams.getBuckets(job.getParam(NBatchConstants.P_BUCKETS));
        Assert.assertEquals(30, buckets.size());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val segment = dataflow.getSegment(job.getTargetSegments().get(0));
        segment.getMultiPartitions().forEach(partition -> {
            Assert.assertEquals(PartitionStatusEnum.REFRESH, partition.getStatus());
        });
    }

    @Test
    public void testMergeMultiPartitionSegments() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);

        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        // first segment
        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        NDataLayout layout1 = generateLayoutForMultiPartition(modelId, dataSegment1.getId(), partitionValues, 1L);

        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        NDataLayout layout2 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues, 1L);

        List<String> partitionValues2 = Lists.newArrayList("usa");
        NDataSegment dataSegment3 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-03-01",
                "2010-04-01", SegmentStatusEnum.READY);
        NDataLayout layout3 = generateLayoutForMultiPartition(modelId, dataSegment3.getId(), partitionValues2, 1L);

        NDataSegment dataSegment4 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-04-01",
                "2010-05-01", SegmentStatusEnum.READY);
        NDataLayout layout4_1 = generateLayoutForMultiPartition(modelId, dataSegment4.getId(), partitionValues2, 1L);
        NDataLayout layout4_2 = generateLayoutForMultiPartition(modelId, dataSegment4.getId(), partitionValues2,
                10001L);

        NDataSegment dataSegment5 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-05-01",
                "2010-06-01", SegmentStatusEnum.READY);
        NDataSegment dataSegment6 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-06-01",
                "2010-07-01", SegmentStatusEnum.READY);

        List<NDataLayout> toAddCuboIds = Lists.newArrayList(layout1, layout2, layout3, layout4_1, layout4_2);
        val segments = Lists.newArrayList(dataSegment1, dataSegment2, dataSegment3, dataSegment4, dataSegment5,
                dataSegment6);

        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToAddOrUpdateLayouts(toAddCuboIds.toArray(new NDataLayout[] {}));
        update2.setToUpdateSegs(segments.toArray(new NDataSegment[] {}));
        dfManager.updateDataflow(update2);

        // empty layout in segment4
        try {
            modelService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment5.getId(), dataSegment6.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "The indexes included in the selected segments are not fully identical. Please build index first and try merging again.",
                    e.getMessage());
        }

        // index is not aligned in segment3, segment4
        try {
            modelService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment3.getId(), dataSegment4.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "The indexes included in the selected segments are not fully identical. Please build index first and try merging again.",
                    e.getMessage());
        }

        // partitions are not aligned in segment2, segment3
        try {
            modelService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment2.getId(), dataSegment3.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "The subpartitions included in the selected segments are not fully aligned. Please build the subpartitions first and try merging again.",
                    e.getMessage());
        }

        // success
        modelService.mergeSegmentsManually(
                new MergeSegmentParams(project, modelId, new String[] { dataSegment1.getId(), dataSegment2.getId() }));
    }

    private NDataSegment generateSegmentForMultiPartition(String modelId, List<String> partitionValues, String start,
            String end, SegmentStatusEnum status) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), getProject());
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        long startTime = SegmentRange.dateToLong(start);
        long endTime = SegmentRange.dateToLong(end);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);
        val df = dfm.getDataflow(modelId);
        val newSegment = dfm.appendSegment(df, segmentRange, status, partitions);
        newSegment.getMultiPartitions().forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.READY);
        });
        return newSegment;
    }

    private NDataLayout generateLayoutForMultiPartition(String modelId, String segmentId, List<String> partitionValues,
            long layoutId) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), getProject());
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());

        val model = modelManager.getDataModelDesc(modelId);
        val df = dfm.getDataflow(modelId);
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        val partitionIds = model.getMultiPartitionDesc().getPartitionIdsByValues(partitions);
        NDataLayout layout = NDataLayout.newDataLayout(df, segmentId, layoutId);
        partitionIds.forEach(id -> {
            layout.getMultiPartition().add(new LayoutPartition(id));
        });
        return layout;
    }

    @Test
    public void testBuildMultiPartitionManually() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "73570f31-05a5-448f-973c-44209830dd01";

        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowManager.updateDataflow(dataflowUpdate);
        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "un" });
        buildPartitions.add(new String[] { "Africa" });
        buildPartitions.add(new String[] { "Austria" });
        val multiPartition1 = modelManager.getDataModelDesc(modelId).getMultiPartitionDesc();
        Assert.assertEquals(3, multiPartition1.getPartitions().size());
        modelService.buildSegmentPartitionByValue(getProject(), modelId, segmentId1, buildPartitions, false, false);
        val multiPartition2 = modelManager.getDataModelDesc(modelId).getMultiPartitionDesc();
        // add two new partitions
        Assert.assertEquals(5, multiPartition2.getPartitions().size());
        val jobs1 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(1, jobs1.size());

        val segmentId2 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        modelService.buildSegmentPartitionByValue(getProject(), modelId, segmentId2, buildPartitions, true, false);
        val jobs2 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(4, jobs2.size());

        val segmentId3 = "d2edf0c5-5eb2-4968-9ad5-09efbf659324";
        try {
            modelService.buildSegmentPartitionByValue(getProject(), modelId, segmentId3, buildPartitions, true, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Can’t add the job. Please ensure that the subpartitions are unique.", e.getMessage());
            Assert.assertEquals(4, getRunningExecutables(getProject(), modelId).size());
        }

        val segmentId4 = "ff839b0b-2c23-4420-b332-0df70e36c343";
        try {
            overwriteSystemProp("kylin.job.max-concurrent-jobs", "1");
            val buildPartitions2 = Lists.<String[]> newArrayList();
            buildPartitions2.add(new String[] { "ASIA" });
            buildPartitions2.add(new String[] { "EUROPE" });
            buildPartitions2.add(new String[] { "MIDDLE EAST" });
            buildPartitions2.add(new String[] { "AMERICA" });
            buildPartitions2.add(new String[] { "MOROCCO" });
            buildPartitions2.add(new String[] { "INDONESIA" });
            modelService.buildSegmentPartitionByValue(getProject(), modelId, segmentId4, buildPartitions2, true, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "Can't submit building jobs, as it exceeds the concurrency limit (5).  Please try submitting fewer jobs at a time.",
                    e.getMessage());
            Assert.assertEquals(4, getRunningExecutables(getProject(), modelId).size());
        }

        modelService.buildSegmentPartitionByValue(getProject(), modelId, segmentId4, null, false, true);
        val jobs4 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(3, jobs4.get(0).getTargetPartitions().size());
    }

    @Test
    public void testRefreshMultiPartitionById() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        val segmentId2 = "ff839b0b-2c23-4420-b332-0df70e36c343";

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowManager.updateDataflow(dataflowUpdate);

        // refresh partition by id
        PartitionsRefreshRequest param1 = new PartitionsRefreshRequest(getProject(), segmentId1,
                Sets.newHashSet(7L, 8L), null, null);
        modelService.refreshSegmentPartition(param1, modelId);

        // refresh partition by value
        val partitionValues = Lists.<String[]> newArrayList(new String[] { "usa" }, new String[] { "un" });
        PartitionsRefreshRequest param2 = new PartitionsRefreshRequest(getProject(), segmentId2, null, partitionValues,
                null);
        modelService.refreshSegmentPartition(param2, modelId);

        // no target partition id in segment
        PartitionsRefreshRequest param3 = new PartitionsRefreshRequest(getProject(), segmentId1, Sets.newHashSet(99L),
                null, null);
        try {
            modelService.refreshSegmentPartition(param3, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(),
                    "Can’t add the job. Please ensure that the operation is valid for the current object.");
        }

        // no target partition value in segment
        partitionValues.add(new String[] { "nodata" });
        PartitionsRefreshRequest param4 = new PartitionsRefreshRequest(getProject(), segmentId1, null, partitionValues,
                null);
        try {
            modelService.refreshSegmentPartition(param4, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(),
                    "Can’t add the job. Please ensure that the operation is valid for the current object.");
        }

        // no target partition value or partition id
        PartitionsRefreshRequest param5 = new PartitionsRefreshRequest(getProject(), segmentId1, null, null, null);
        try {
            modelService.refreshSegmentPartition(param5, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(),
                    "Can’t add the job. Please ensure that the operation is valid for the current object.");
        }
    }

    @Test
    public void testMultiPartitionIndexBuild() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        // different segments with different partitions and layouts
        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        NDataLayout layout1 = generateLayoutForMultiPartition(modelId, dataSegment1.getId(), partitionValues, 1L);

        List<String> partitionValues2 = Lists.newArrayList("usa");
        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        NDataLayout layout2 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues2, 1L);
        NDataLayout layout3 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues2, 100001L);

        List<NDataLayout> toAddCuboIds = Lists.newArrayList(layout1, layout2, layout3);
        val segments = Lists.newArrayList(dataSegment1, dataSegment2);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToAddOrUpdateLayouts(toAddCuboIds.toArray(new NDataLayout[] {}));
        update2.setToUpdateSegs(segments.toArray(new NDataSegment[] {}));
        dfManager.updateDataflow(update2);

        modelService.addIndexesToSegments(project, modelId,
                Lists.newArrayList(dataSegment1.getId(), dataSegment2.getId()), Lists.newArrayList(80001L), false,
                ExecutablePO.DEFAULT_PRIORITY);
        val executables = getRunningExecutables(getProject(), modelId);
        val job = executables.get(0);
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(2, job.getTargetPartitions().size());
        Assert.assertEquals(3, ExecutableParams.getBuckets(job.getParam("buckets")).size());
    }

    @Test
    public void testDeleteMultiPartitions() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        val segmentId2 = "d2edf0c5-5eb2-4968-9ad5-09efbf659324";
        val project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val dfm = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfm.getDataflow(modelId);
        modelManager.getDataModelDesc(modelId);
        NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel model1 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(3, model1.getMultiPartitionDesc().getPartitions().size());
        Assert.assertEquals(2, df.getSegment(segmentId).getAllPartitionIds().size());
        Assert.assertEquals(2, df.getSegment(segmentId).getLayout(1).getMultiPartition().size());

        // just remove partitions in layouts and segment
        modelService.deletePartitions(project, segmentId, modelId, Sets.newHashSet(7L));
        Assert.assertEquals(20128L, dfm.getDataflow(modelId).getSegment(segmentId).getStorageBytesSize());
        Assert.assertEquals(27L, dfm.getDataflow(modelId).getSegment(segmentId).getSegDetails().getTotalRowCount());
        Assert.assertEquals(20L, dfm.getDataflow(modelId).getSegment(segmentId).getSourceCount());

        val model2 = modelManager.getDataModelDesc(modelId);
        val segment2 = dfm.getDataflow(modelId).getSegment(segmentId);
        Assert.assertEquals(3, model2.getMultiPartitionDesc().getPartitions().size());
        Assert.assertEquals(1, segment2.getAllPartitionIds().size());
        Assert.assertEquals(1, segment2.getLayout(1).getMultiPartition().size());

        // remove partitions in all layouts and segments and model
        modelService.deletePartitions(project, null, modelId, Sets.newHashSet(8L, 99L));
        val model3 = modelManager.getDataModelDesc(modelId);
        val segment3 = dfm.getDataflow(modelId).getSegment(segmentId);
        val segment4 = dfm.getDataflow(modelId).getSegment(segmentId2);
        Assert.assertEquals(2, model3.getMultiPartitionDesc().getPartitions().size());
        Assert.assertEquals(0, segment3.getAllPartitionIds().size());
        Assert.assertEquals(0, segment3.getLayout(1).getMultiPartition().size());
        Assert.assertEquals(2, segment4.getAllPartitionIds().size());
        Assert.assertEquals(2, segment4.getLayout(1).getMultiPartition().size());
    }

    @Test
    public void testChangeMultiPartition() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val model = modelManager.getDataModelDesc(modelId);
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val df = dfm.getDataflow(modelId);
        Assert.assertEquals(df.getSegments().size(), 4);
        Assert.assertEquals(df.getStatus(), RealizationStatusEnum.ONLINE);
        // PartitionDesc change. Multi Partition column change or from none to have or from have to none.

        // Not change partition
        modelService.updatePartitionColumn(getProject(), modelId, model.getPartitionDesc(),
                model.getMultiPartitionDesc());
        Assert.assertEquals(df.getSegments().size(), 4);
        Assert.assertEquals(df.getStatus(), RealizationStatusEnum.ONLINE);
        Assert.assertEquals(model.getMultiPartitionDesc().getPartitions().size(), 3);

        // PartitionDesc change
        modelService.updatePartitionColumn(getProject(), modelId, null, model.getMultiPartitionDesc());
        val df1 = dfm.getDataflow(modelId);
        val model1 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(df1.getSegments().getSegments().size(), 0);
        Assert.assertEquals(df1.getStatus(), RealizationStatusEnum.OFFLINE);
        Assert.assertEquals(model1.getMultiPartitionDesc().getPartitions().size(), 0);

        // Multi Partition column change
        dfm.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite(), SegmentStatusEnum.READY);
        dfm.updateDataflowStatus(modelId, RealizationStatusEnum.ONLINE);
        val columns = Lists.<String> newLinkedList();
        columns.add("location");

        modelService.updatePartitionColumn(getProject(), modelId, model.getPartitionDesc(),
                new MultiPartitionDesc(columns));
        val df2 = dfm.getDataflow(modelId);
        Assert.assertEquals(df2.getSegments().size(), 0);
        Assert.assertEquals(df2.getStatus(), RealizationStatusEnum.OFFLINE);

        // Multi Partition column change to none
        dfm.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite(), SegmentStatusEnum.READY);
        dfm.updateDataflowStatus(modelId, RealizationStatusEnum.ONLINE);
        modelService.updatePartitionColumn(getProject(), modelId, model.getPartitionDesc(), null);
        val df3 = dfm.getDataflow(modelId);
        Assert.assertEquals(df3.getSegments().size(), 0);
        Assert.assertEquals(df3.getStatus(), RealizationStatusEnum.OFFLINE);

        // Normal model change to multi partition model
        dfm.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite(), SegmentStatusEnum.READY);
        dfm.updateDataflowStatus(modelId, RealizationStatusEnum.ONLINE);
        modelService.updatePartitionColumn(getProject(), modelId, model.getPartitionDesc(),
                new MultiPartitionDesc(columns));
        val df4 = dfm.getDataflow(modelId);
        Assert.assertEquals(df4.getSegments().size(), 0);
        Assert.assertEquals(df4.getStatus(), RealizationStatusEnum.OFFLINE);
    }

    private void checkPropParameter(ModelConfigRequest request) {
        request.setOverrideProps(null);
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_NULL_VALUE(), "override_props")));
        }
        LinkedHashMap<String, String> prop = new LinkedHashMap<>();
        request.setOverrideProps(prop);
        prop.put("kylin.engine.spark-conf.spark.executor.cores", "1.2");
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.cores")));
        }
        prop.clear();
        prop.put("kylin.engine.spark-conf.spark.executor.instances", "1.2");
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.executor.instances")));
        }
        prop.clear();
        prop.put("kylin.engine.spark-conf.spark.sql.shuffle.partitions", "1.2");
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_INTEGER_FORMAT(), "spark.sql.shuffle.partitions")));
        }
        prop.clear();
        prop.put("kylin.engine.spark-conf.spark.executor.memory", "3");
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MEMORY_SIZE(), "spark.executor.memory")));
        }
        prop.clear();
        prop.put("kylin.cube.aggrgroup.is-base-cuboid-always-valid", "ddd");
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_BOOLEAN_FORMAT(), "is-base-cuboid-always-valid")));
        }
        prop.clear();
        prop.put("kylin.engine.spark-conf.spark.executor.memory", null);
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(String.format(Locale.ROOT,
                    MsgPicker.getMsg().getINVALID_NULL_VALUE(), "kylin.engine.spark-conf.spark.executor.memory")));
        }
    }

    @Test
    public void testCheckModelConfigParameters() {
        ModelConfigRequest request = new ModelConfigRequest();
        request.setAutoMergeEnabled(true);
        request.setAutoMergeTimeRanges(new ArrayList<>());
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(MsgPicker.getMsg().getINVALID_AUTO_MERGE_CONFIG()));
        }
        request.setAutoMergeEnabled(false);
        request.setVolatileRange(new VolatileRange(2, true, null));
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(MsgPicker.getMsg().getINVALID_VOLATILE_RANGE_CONFIG()));
        }
        request.setVolatileRange(null);
        request.setRetentionRange(new RetentionRange(-1, true, null));
        try {
            modelService.checkModelConfigParameters(request);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(MsgPicker.getMsg().getINVALID_RETENTION_RANGE_CONFIG()));
        }
        request.setRetentionRange(null);
        checkPropParameter(request);
    }

    @Test
    public void testBatchUpdateMultiPartition() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val df = dfm.getDataflow(modelId);
        Assert.assertEquals(df.getSegments().size(), 4);
        Assert.assertEquals(df.getStatus(), RealizationStatusEnum.ONLINE);
        // PartitionDesc change. Multi Partition column change or from none to have or from have to none.

        List<String[]> partitionValues = new ArrayList<>();
        partitionValues.add(new String[] { "p1" });
        partitionValues.add(new String[] { "p2" });
        partitionValues.add(new String[] { "p3" });
        var dataModel = modelService.batchUpdateMultiPartition(getProject(), modelId, partitionValues);

        List<List<String>> expectPartitionValues = new ArrayList<>();
        expectPartitionValues.add(Collections.singletonList("p1"));
        expectPartitionValues.add(Collections.singletonList("p2"));
        expectPartitionValues.add(Collections.singletonList("p3"));

        Assert.assertEquals(expectPartitionValues, dataModel.getMultiPartitionDesc().getPartitions().stream()
                .map(MultiPartitionDesc.PartitionInfo::getValues).map(Arrays::asList).collect(Collectors.toList()));

        partitionValues = new ArrayList<>();
        partitionValues.add(new String[] { "p2" });
        partitionValues.add(new String[] { "p1" });
        partitionValues.add(new String[] { "p5" });
        dataModel = modelService.batchUpdateMultiPartition(getProject(), modelId, partitionValues);

        expectPartitionValues = new ArrayList<>();
        expectPartitionValues.add(Collections.singletonList("p1"));
        expectPartitionValues.add(Collections.singletonList("p2"));
        expectPartitionValues.add(Collections.singletonList("p5"));
        Assert.assertEquals(expectPartitionValues, dataModel.getMultiPartitionDesc().getPartitions().stream()
                .map(MultiPartitionDesc.PartitionInfo::getValues).map(Arrays::asList).collect(Collectors.toList()));
    }

    @Test
    public void testBatchUpdateMultiPartitionWithNotExistsModel() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val modelId = "1";

        List<String[]> partitionValues = new ArrayList<>();
        partitionValues.add(new String[] { "p1" });
        partitionValues.add(new String[] { "p2" });
        partitionValues.add(new String[] { "p3" });
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find model named \"1\". Please check and try again.");
        modelService.batchUpdateMultiPartition(getProject(), modelId, partitionValues);
    }

    @Test
    public void testBatchUpdateMultiPartitionWithEmptyPartitionValues() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";

        List<String[]> partitionValues = new ArrayList<>();
        NDataModel dataModel = modelService.batchUpdateMultiPartition(getProject(), modelId, partitionValues);
        Assert.assertEquals(0, dataModel.getMultiPartitionDesc().getPartitions().size());
    }

    private void addAclTable(String tableName, String user, boolean hasColumn) {
        val table = NTableMetadataManager.getInstance(getTestConfig(), "default").getTableDesc(tableName);
        AclTCR acl = new AclTCR();
        AclTCR.Table aclTable = new AclTCR.Table();
        AclTCR.ColumnRow aclColumnRow = new AclTCR.ColumnRow();
        AclTCR.Column aclColumns = new AclTCR.Column();
        if (hasColumn) {
            Arrays.stream(table.getColumns()).forEach(x -> aclColumns.add(x.getName()));
        }
        aclColumnRow.setColumn(aclColumns);
        aclTable.put(tableName, aclColumnRow);
        acl.setTable(aclTable);
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), "default");
        manager.updateAclTCR(acl, "user", true);
    }

    @Test
    public void testCheckModelPermission() {
        List<NDataModel> models = new ArrayList<>();
        models.addAll(modelService.getModels("", "default", false, "", null, "last_modify", true));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        // Admin is allowed to modify model
        modelService.checkModelPermission(getProject(), "b780e4e4-69af-449e-b09f-05c90dfa04b6");

        addAclTable("DEFAULT.TEST_BANK_LOCATION", "user", true);
        PasswordEncoder pwdEncoder = PasswordEncodeFactory.newUserPasswordEncoder();
        val user = new ManagedUser("user", pwdEncoder.encode("pw"), false);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(user, "ANALYST", Constant.ROLE_ANALYST));
        // lack of table
        assertKylinExeption(() -> {
            modelService.checkModelPermission(getProject(), "b780e4e4-69af-449e-b09f-05c90dfa04b6");
        }, "Model is not support to modify");

        addAclTable("DEFAULT.TEST_ENCODING", "user", false);
        // lack of column
        assertKylinExeption(() -> {
            modelService.checkModelPermission(getProject(), "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        }, "Model is not support to modify");

        // model id is invalid
        assertKylinExeption(() -> {
            modelService.checkModelPermission(getProject(), "xxx");
        }, "Can’t find model named \"xxx\". Please check and try again.");

        addAclTable("DEFAULT.TEST_ENCODING", "user", true);
        modelService.checkModelPermission(getProject(), "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
    }

    @Test
    public void testUpdateDataModelWithNotExistModelId() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        try {
            modelManager.updateDataModel("abc", x -> {
            });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("Data model with id 'abc' not found."));
        }
    }

    @Test
    public void changeSecondStorageIfNeeded() throws IOException {
        val models = new ArrayList<>(modelService.listAllModelIdsInProject("default"));
        val model = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, "default");
        SecondStorageUtil.initModelMetaData("default", model);
        Assert.assertTrue(indexPlanManager.getIndexPlan(model).containBaseTableLayout());
        ModelRequest request = new ModelRequest();
        request.setWithSecondStorage(false);
        request.setUuid(model);
        Mockito.doCallRealMethod().when(modelService).changeSecondStorageIfNeeded("default", request);
        modelService.changeSecondStorageIfNeeded("default", request);
        Assert.assertFalse(SecondStorageUtil.isModelEnable("default", model));

        ModelRequest request2 = new ModelRequest();
        request2.setWithSecondStorage(true);
        request2.setUuid(model);
        modelService.changeSecondStorageIfNeeded("default", request2);
        Assert.assertTrue(SecondStorageUtil.isModelEnable("default", model));
    }

    @Test
    public void testGetFusionModel() {
        String project = "streaming_test";
        String modelName = "streaming_test";
        List<NDataModelResponse> models = modelService.getModels(modelName, project, false, null, Lists.newArrayList(),
                null, false, null, null, null, true);

        FusionModelResponse model = (FusionModelResponse) models.get(0);
        Assert.assertEquals(0, model.getAvailableIndexesCount());
        Assert.assertEquals(3, model.getTotalIndexes());
        Assert.assertEquals(5, model.getStreamingIndexes());
        Assert.assertEquals(10, model.getUsage());
        Assert.assertEquals(0, model.getStorage());
        Assert.assertEquals(0, model.getSource());

        String modelName1 = "AUTO_MODEL_P_LINEORDER_1";
        NDataModelResponse model1 = modelService
                .getModels(modelName1, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertEquals(0, model1.getAvailableIndexesCount());
        Assert.assertEquals(1, model1.getTotalIndexes());
        Assert.assertEquals(0, model1.getStorage());
        Assert.assertEquals(0, model1.getSource());

        String modelName2 = "model_streaming";
        DataResult<List<NDataModel>> modelResult2 = modelService.getModels(modelName2, true, project, "ADMIN",
                Lists.newArrayList(), "", 0, 10, "last_modify", true, null, Arrays.asList(ModelAttributeEnum.BATCH,
                        ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID, ModelAttributeEnum.SECOND_STORAGE),
                null, null, true);
        List<NDataModel> models2 = modelResult2.getValue();
        FusionModelResponse model2 = (FusionModelResponse) models2.get(0);

        Assert.assertEquals(14383, model2.getOldParams().getInputRecordCnt());
        Assert.assertEquals(1505415, model2.getOldParams().getInputRecordSizeBytes());
        Assert.assertEquals(396, model2.getOldParams().getSizeKB());
    }

    @Test
    public void testGetBrokenFusionModel() {
        String project = "streaming_test";
        String modelName = "model_streaming_broken";
        val list = modelService.getModels(null, project, false, null, Lists.newArrayList(), null, false, null, null,
                null, true);
        Assert.assertEquals(10, list.size());

        NDataModelResponse model = modelService
                .getModels(modelName, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertTrue(model.isBroken());
        Assert.assertEquals(0, model.getAvailableIndexesCount());
        Assert.assertEquals(0, model.getTotalIndexes());
        Assert.assertEquals(406495, model.getStorage());
        Assert.assertEquals(1369556, model.getSource());
    }

    @Test
    public void testCreateFusionModel() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "streaming_test");
        NDataModel model = modelManager.getDataModelDesc("b05034a8-c037-416b-aa26-9e6b4a41ee40");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.setProject("streaming_test");
        NDataModel result = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertNotEquals(0L, result.getLastModified());
        Assert.assertEquals(result.getUuid(), result.getFusionId());
    }

    @Test
    public void testCheckAllNamedColumns() {
        String project = "streaming_test";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel okModel = modelManager.getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        ModelRequest okModelRequest = new ModelRequest(okModel);
        okModelRequest.setProject(project);
        val model = semanticService.convertToDataModel(okModelRequest);
        Assert.assertEquals(19, model.getAllNamedColumns().size());
        NDataModel batchModel = modelManager.getDataModelDesc("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        ModelRequest batchModelRequest = new ModelRequest(batchModel);
        batchModelRequest.setProject(project);
        val model1 = semanticService.convertToDataModel(batchModelRequest);
        Assert.assertEquals(model.getAllNamedColumns().get(4).getName(), model1.getAllNamedColumns().get(4).getName());
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
