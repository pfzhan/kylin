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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import static org.awaitility.Awaitility.await;

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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
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
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler;
import io.kyligence.kap.engine.spark.job.ExecutableAddSegmentHandler;
import io.kyligence.kap.engine.spark.job.ExecutableMergeOrRefreshHandler;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.cuboid.CuboidStatus;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.model.util.scd2.SCD2SqlConverter;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.InfluxDBQueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.execution.SucceedChainedTestExecutable;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.NCubeDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.response.ParameterResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SimplifiedColumnResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import lombok.val;
import lombok.var;

public class ModelServiceTest extends CSVSourceTestCase {

    private final String MODEL_UT_INNER_JOIN_ID = "82fa7671-a935-45f5-8779-85703601f49a";

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Autowired
    private TableService tableService = Mockito.spy(new TableService());

    @Autowired
    private TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Autowired
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private SegmentHelper segmentHelper = new SegmentHelper();

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private OptRecService optRecService = Mockito.spy(OptRecService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private static String[] timeZones = { "GMT+8", "CST", "PST", "UTC" };

    @Before
    public void setup() {
        super.setup();
        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        Mockito.doReturn(new OptRecLayoutsResponse()).when(optRecService).getOptRecLayoutsResponse(Mockito.anyString(),
                Mockito.anyString());
        modelService.setSemanticUpdater(semanticService);
        modelService.setSegmentHelper(segmentHelper);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        InfluxDBQueryHistoryDAO influxDBQueryHistoryDAO = Mockito.mock(InfluxDBQueryHistoryDAO.class);
        Mockito.doReturn(influxDBQueryHistoryDAO).when(modelService).getQueryHistoryDao();
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);

        SchedulerEventBusFactory.getInstance(getTestConfig()).register(modelBrokenListener);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(modelBrokenListener);
        SchedulerEventBusFactory.restart();
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
        Assert.assertEquals(6, models7.size());

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

        val models = modelService.getModels(df.getModelAlias(), getProject(), true, "", null, "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, models.get(0).getStatus());
    }

    @Test
    @Ignore
    public void testGetModelsWithRecommendationCount() {
        val models = modelService.getModels("nmodel_basic", "default", true, "", null, "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(0, models.get(0).getRecommendationsCount());

        val modelId1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelId2 = "741ca86a-1f13-46da-a59f-95fb68615e3a";

        val recommendation1 = new OptimizeRecommendation();
        recommendation1.setUuid(modelId1);
        recommendation1.setProject("default");

        val ccRecommendation1 = new CCRecommendationItem();
        val ccRecommendation2 = new CCRecommendationItem();

        recommendation1.setCcRecommendations(Lists.newArrayList(ccRecommendation1, ccRecommendation2));

        val recommendationManager = Mockito.spy(OptimizeRecommendationManager.getInstance(getTestConfig(), "default"));
        Mockito.doReturn(recommendation1).when(recommendationManager).getOptimizeRecommendation(modelId1);

        val recommendation2 = new OptimizeRecommendation();
        recommendation2.setUuid(modelId2);

        recommendation2.setMeasureRecommendations(
                Lists.newArrayList(new MeasureRecommendationItem(), new MeasureRecommendationItem()));
        recommendation2.setDimensionRecommendations(Lists.newArrayList(new DimensionRecommendationItem()));
        val layoutRecommendation1 = new LayoutRecommendationItem();
        val layoutEntity = new LayoutEntity();
        layoutEntity.setId(10001L);
        layoutRecommendation1.setLayout(layoutEntity);
        layoutRecommendation1.setAggIndex(true);

        val layoutRecommendation2 = new LayoutRecommendationItem();
        layoutRecommendation2.setLayout(layoutEntity);
        layoutRecommendation2.setAggIndex(true);
        recommendation2.setLayoutRecommendations(Lists.newArrayList(layoutRecommendation1, layoutRecommendation2));

        Mockito.doReturn(recommendation2).when(recommendationManager).getOptimizeRecommendation(modelId2);
        Mockito.doReturn(recommendationManager).when(modelService).getOptRecommendationManager("default");

        val allModels = modelService.getModels("", "default", false, "", null, "recommendations_count", true);
        Assert.assertEquals(5, allModels.get(0).getRecommendationsCount());
        Assert.assertEquals(2, allModels.get(1).getRecommendationsCount());
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
        Assert.assertEquals(6, models.size());
        Assert.assertEquals("nmodel_basic_inner", models.get(0).getAlias());
        models = modelService.getModels("", "default", false, "", null, "usage", false);
        Assert.assertEquals("nmodel_basic_inner", models.get(models.size() - 1).getAlias());
        models = modelService.getModels("", "default", false, "", null, "storage", true);
        Assert.assertEquals("nmodel_basic", models.get(0).getAlias());
        models = modelService.getModels("", "default", false, "", null, "storage", false);
        Assert.assertEquals("nmodel_basic", models.get(models.size() - 1).getAlias());

        models = modelService.getModels("", "default", false, "", null, "expansionrate", true);
        Assert.assertEquals("nmodel_basic_inner", models.get(0).getAlias());
        models = modelService.getModels("", "default", false, "", null, "expansionrate", false);
        Assert.assertEquals("nmodel_basic_inner", models.get(4).getAlias());

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
        Assert.assertTrue(indices.getIndices().stream().allMatch(
                index -> index.getDimensions().stream().anyMatch(d -> d.contains(contentSegDimension.toUpperCase()))));

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
    public void testGetModelRelations() {
        List<NSpanningTreeForWeb> relations = modelService.getModelRelations("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default");
        Assert.assertEquals(1, relations.size());
        NSpanningTreeForWeb st = relations.get(0);
        IndexEntity root = st.getIndexEntity(1000000L);

        Assert.assertEquals(5, st.getCuboidCount());
        Assert.assertEquals(5, st.getAllIndexEntities().size());
        Assert.assertEquals(1, st.getRootIndexEntities().size());
        Assert.assertEquals(1, st.getLayouts(root).size());
        Assert.assertEquals(1, st.getChildrenByIndexPlan(root).size());
        Assert.assertEquals(st.getNodesMap().get(30000L).getParent().getIndexEntity(), root);
        Assert.assertTrue(st.isValid(0L));

        relations = modelService.getModelRelations("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        Assert.assertEquals(1, relations.size());

        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        indePlanManager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copyForWrite -> {
            val rule = new NRuleBasedIndex();
            rule.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            rule.setMeasures(Lists.newArrayList(100001, 100002));
            try {
                val aggGroup = JsonUtil.readValue("{\n" + "        \"includes\": [1, 2, 3],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [],\n"
                        + "          \"dim_cap\": 1\n" + "        }\n" + "      }", NAggregationGroup.class);
                rule.setAggregationGroups(Lists.newArrayList(aggGroup));
                copyForWrite.setRuleBasedIndex(rule);
            } catch (IOException ignore) {
            }
        });
        relations = modelService.getModelRelations("741ca86a-1f13-46da-a59f-95fb68615e3a", "default");
        Assert.assertEquals(1, relations.size());
    }

    @Test
    public void testGetSimplifiedModelRelations() {
        List<NSpanningTreeForWeb> relations = modelService.getModelRelations("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(1, relations.get(0).getRoots().size());
        Assert.assertEquals(5, relations.get(0).getNodesMap().size());
    }

    @Test
    public void testGetSimplifiedModelRelations_NoReadySegment() {
        val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dfMgr.appendSegment(dfMgr.getDataflow(MODEL_UT_INNER_JOIN_ID),
                new SegmentRange.TimePartitionedSegmentRange(10L, 200L));
        List<NSpanningTreeForWeb> relations = modelService.getModelRelations(MODEL_UT_INNER_JOIN_ID, "default");
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(1, relations.get(0).getRoots().size());
        Assert.assertEquals(CuboidStatus.EMPTY, relations.get(0).getRoots().get(0).getCuboid().getStatus());
        Assert.assertEquals(0L, relations.get(0).getRoots().get(0).getCuboid().getStorageSize());

    }

    @Test
    public void testDropModelExceptionName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.dropModel("nmodel_basic2222", "default");
    }

    @Test
    public void testDropModelPass() throws NoSuchFieldException, IllegalAccessException {
        String modelId = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94";
        String project = "default";
        JobManager jobManager = JobManager.getInstance(getTestConfig(), project);
        val jobId = jobManager.addFullIndexJob(modelId, "admin");
        Assert.assertTrue(jobId == null);
        AtomicBoolean clean = new AtomicBoolean(false);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val recommendationManager = spyOptimizeRecommendationManager();
            Mockito.doAnswer(invocation -> {
                String id = invocation.getArgument(0);
                if (modelId.equals(id)) {
                    clean.set(true);
                }
                return null;
            }).when(recommendationManager).dropOptimizeRecommendation(Mockito.anyString());
            modelService.dropModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
            return null;
        }, "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding", "default", true, "", null,
                "last_modify", true);
        Assert.assertTrue(CollectionUtils.isEmpty(models));
        Assert.assertTrue(clean.get());
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
        thrown.expectMessage("Model [test_encoding] is table oriented, can not purge the model");
        modelService.purgeModelManually("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default");
    }

    @Test
    public void testGetAffectedSegmentsResponse_FullBuildAndEmptyModel() {

        List<NDataSegment> segments = modelService.getSegmentsByRange("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(segments.size() == 1);
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        dfMgr.updateDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setSegments(new Segments());
        });
        RefreshAffectedSegmentsResponse response = modelService.getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.TEST_KYLIN_FACT", "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(response.getByteSize() == 0L);
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
        Assert.assertTrue(response.getAffectedStart().equals("0"));
        Assert.assertTrue(response.getAffectedEnd().equals("30"));
    }

    @Test
    public void testPurgeModelExceptionName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.purgeModelManually("nmodel_basic2222", "default");
    }

    @Test
    public void testCloneModelException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model alias [nmodel_basic_inner] are duplicated!");
        modelService.cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner", "default");
    }

    @Test
    public void testCloneModelExceptionName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic2222' not found");
        modelService.cloneModel("nmodel_basic2222", "nmodel_basic_inner222", "default");
    }

    @Test
    public void testCloneModel() {
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));
        modelService.cloneModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "test_encoding_new", "default");
        List<NDataModelResponse> models = modelService.getModels("test_encoding_new", "default", true, "", null,
                "last_modify", true);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(randomUser, models.get(0).getOwner());

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
            NRuleBasedIndex newRuleBasedCuboid = request.convertToRuleBasedIndex();
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
        Assert.assertTrue(models.get(0).getAlias().equals("new_name"));
    }

    @Test
    public void testRenameModelException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
        modelService.renameDataModel("default", "nmodel_basic222", "new_name");
    }

    @Test
    public void testRenameModelException2() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model alias [nmodel_basic_inner] are duplicated");
        modelService.renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "nmodel_basic_inner");
    }

    @Test
    public void testUpdateDataModelStatus() {
        modelService.updateDataModelStatus("cb596712-3a09-46f8-aea1-988b43fe9b6c", "default", "OFFLINE");
        List<NDataModelResponse> models = modelService.getModels("nmodel_full_measure_test", "default", true, "", null,
                "last_modify", true);
        Assert.assertTrue(models.get(0).getUuid().equals("cb596712-3a09-46f8-aea1-988b43fe9b6c")
                && models.get(0).getStatus().equals(ModelStatusToDisplayEnum.OFFLINE));
    }

    @Test
    public void testUpdateDataModelStatus_ModelNotExist_Exception() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Data Model with name 'nmodel_basic222' not found");
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
        Assert.assertTrue(models.size() == 0);
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
        List<String> sqls = Lists.newArrayList("select order_id, count(*) from test_order group by order_id limit 1");
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelService.couldAnsweredByExistedModel(getProject(), sqls);
        Assert.assertTrue(result);
    }

    @Test
    public void testMultipleModelContextSelectedTheSameModel() {
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
        val response = modelService.suggestModel(getProject(), sqls, true);
        Assert.assertEquals(1, response.getOriginModels().size());
        Assert.assertEquals(0, response.getNewModels().size());
        Assert.assertEquals(3, response.getOriginModels().get(0).getSqls().size());
        val recommendationResponse = response.getOriginModels().get(0).getRecommendationResponse();
        Assert.assertEquals(0, recommendationResponse.getDimensionRecommendations().size());
        Assert.assertEquals(0, recommendationResponse.getMeasureRecommendations().size());
        Assert.assertEquals(0, recommendationResponse.getCcRecommendations().size());
        Assert.assertEquals(0, recommendationResponse.getIndexRecommendations().size());

        val response2 = modelService.suggestModel(getProject(), sqls.subList(0, 2), true);
        Assert.assertEquals(1, response2.getOriginModels().size());
        Assert.assertEquals(0, response2.getNewModels().size());
        Assert.assertEquals(2, response2.getOriginModels().get(0).getSqls().size());
        val recommendationResponse2 = response2.getOriginModels().get(0).getRecommendationResponse();
        Assert.assertEquals(0, recommendationResponse2.getDimensionRecommendations().size());
        Assert.assertEquals(0, recommendationResponse2.getMeasureRecommendations().size());
        Assert.assertEquals(0, recommendationResponse2.getCcRecommendations().size());
        Assert.assertEquals(0, recommendationResponse2.getIndexRecommendations().size());
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
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        dataflowManager.refreshSegment(df, segmentRange);

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(MsgPicker.getMsg().getSEGMENT_LOCKED(), dataSegment.getId()));

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
        thrown.expectMessage("Can not find the Segments by ids [not_exist_01]");
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

        modelService.deleteSegmentById(modelId, project, new String[] { "ef783e4d-e35f-4bd9-8afd-efd64336f04d" },
                false);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId);

        Assert.assertTrue(CollectionUtils.isEmpty(dataflow.getSegments()));
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getToBeDeletedIndexes()));
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
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getToBeDeletedIndexes()));
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
        thrown.expectMessage("Model [nmodel_basic_inner] is table oriented, can not remove segments manually!");
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment.getId() }, false);
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
            modelService.mergeSegmentsManually(dfId, "default",
                    new String[] { dataSegment1.getId(), dataSegment3.getId() });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Merging segments must not have gaps between " + dataSegment1.getId() + " and "
                    + dataSegment3.getId() + ".", e.getMessage());
        }

        modelService.mergeSegmentsManually(dfId, "default",
                new String[] { dataSegment1.getId(), dataSegment2.getId(), dataSegment3.getId() });
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
            modelService.mergeSegmentsManually(dfId, "default", new String[] { dataSegment2.getId() });
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals("Can not remove or refresh or merge segment [" + dataSegment2.getId()
                    + "], because the segment is LOCKED.", e.getMessage());
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

        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dfManager.updateDataflow(update);

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(
                MsgPicker.getMsg().getSEGMENT_STATUS(SegmentStatusEnumToDisplay.LOADING.name()), dataSegment1.getId()));
        modelService.mergeSegmentsManually(dfId, "default",
                new String[] { dataSegment1.getId(), dataSegment2.getId() });

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
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //refresh normally
        modelService.refreshSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment2.getId() });
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(MsgPicker.getMsg().getSEGMENT_LOCKED(), dataSegment2.getId()));
        //refresh exception
        modelService.refreshSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { dataSegment2.getId() });
    }

    @Test
    public void testRefreshSegmentById_isNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can not find the Segments by ids [not_exist_01]");
        //refresh exception
        modelService.refreshSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { "not_exist_01" });
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
        thrown.expectMessage("Model alias [nmodel_basic] are duplicated");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setLastModified(0L);
        modelRequest.setProject("default");
        NDataModel result = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertNotEquals(0L, result.getLastModified());
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
        thrown.expectMessage("Can not create model manually in SQL acceleration project!");
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
    public void testCreateModelAndBuildManully() throws Exception {
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
    public void testCreateModel_SelfJoinementBuildIncr() throws Exception {
        val modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_self_join_increment.json"),
                ModelRequest.class);
        modelRequest.setProject("default");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        thrown.expect(LookupTableException.class);
        thrown.expectMessage("The dimension table of this model has been set as a fact table by another model");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel_IncrementBuildFactTableConflictIncrement() throws Exception {
        setupPushdownEnv();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().forEach(modelManager::dropModel);
        var modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table1.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table2.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        thrown.expect(LookupTableException.class);
        thrown.expectMessage("The fact table of this model has been set as a dimension table by another model");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel_IncrementBuildFactTableConflictFull() throws Exception {
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
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table2.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        thrown.expect(LookupTableException.class);
        thrown.expectMessage("The fact table of this model has been set as a dimension table by another model");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel_IncrementBuildLookupTableConflictIncrement() throws Exception {
        setupPushdownEnv();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().forEach(modelManager::dropModel);
        var modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table2.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table1.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        thrown.expect(LookupTableException.class);
        thrown.expectMessage("The dimension table of this model has been set as a fact table by another model");
        modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    @Test
    public void testCreateModel_FullBuildLookupTableConflictIncrement() throws Exception {
        setupPushdownEnv();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().forEach(modelManager::dropModel);
        var modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table2.json"),
                ModelRequest.class);
        addModelInfo(modelRequest);
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/model_join_increment_fact_table1.json"),
                ModelRequest.class);
        modelRequest.setProject("default");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.setPartitionDesc(null);
        thrown.expect(LookupTableException.class);
        thrown.expectMessage("The dimension table of this model has been set as a fact table by another model");
        modelService.createModel(modelRequest.getProject(), modelRequest);
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
        modelService.createModel(modelRequest.getProject(), modelRequest);
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
    public void testCreateModelNonEquiJoin() throws Exception {

        val newModel = createNonEquiJoinModel("default", "non_equi_join");
        String sql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(newModel.getJoinTables().get(0).getJoin(),
                genNonEquiJoinCond());
        Assert.assertEquals(sql,
                "select * from  \"DEFAULT\".\"TEST_KYLIN_FACT\" AS \"TEST_KYLIN_FACT\" LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" AS \"TEST_ORDER\" ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\" AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\">=\"TEST_ORDER\".\"TEST_EXTENDED_COLUMN\") AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\"<\"TEST_ORDER\".\"BUYER_ID\")");

        Assert.assertTrue(newModel.getJoinTables().get(0).getJoin().isNonEquiJoin());
    }

    @Test
    @Ignore
    public void testModelNonEquiJoinBrokenRepair() throws Exception {
        /* 1.create scd2 model
         * 2.turn off scd2 configuration
         * 3.unload fact table , model become broken
         * 4.reload the fact table, model should be offline when model is scd2 and scd2 is turned off
         */

        BiFunction<String, String, NDataModel> getModel = ((project, modelId) -> NDataModelManager
                .getInstance(getTestConfig(), project).getDataModelDesc(modelId));
        Function<NDataModel, ModelStatusToDisplayEnum> getModelDisplayStatus = (modelId) -> {
            long inconsistentSegmentCount = NDataflowManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), modelId.getProject()).getDataflow(modelId.getId())
                    .getSegments(SegmentStatusEnum.WARNING).size();
            return modelService.convertModelStatusToDisplay(modelId, modelId.getProject(), inconsistentSegmentCount);
        };
        String projectName = "default";
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");

        Function<String, ModelRequest> modelRequestSupplier = (modelId) -> {
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    projectName);
            NDataModel model = modelManager.getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
            model.setPartitionDesc(null);
            model.setManagementType(ManagementType.MODEL_BASED);
            ModelRequest modelRequest = new ModelRequest(model);
            modelRequest.setProject(projectName);
            modelRequest.setUuid(null);
            modelRequest.setLastModified(0L);
            return modelRequest;
        };
        ModelRequest scd2ModelRequest = modelRequestSupplier.apply("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        scd2ModelRequest.setAlias("non_equi_model");
        scd2ModelRequest.getSimplifiedJoinTableDescs().get(0).getSimplifiedJoinDesc()
                .setSimplifiedNonEquiJoinConditions(genNonEquiJoinCond());

        ModelRequest normalModelRequest = modelRequestSupplier.apply("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        normalModelRequest.setAlias("equi_model");

        val scd2Model = modelService.createModel(scd2ModelRequest.getProject(), scd2ModelRequest);
        val normalModel = modelService.createModel(normalModelRequest.getProject(), normalModelRequest);

        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
        });

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(getModelDisplayStatus.apply(scd2Model), ModelStatusToDisplayEnum.ONLINE);
            Assert.assertEquals(getModelDisplayStatus.apply(normalModel), ModelStatusToDisplayEnum.ONLINE);
        });

        Assert.assertTrue(SCD2CondChecker.INSTANCE.isScd2Model(scd2Model));

        //online -> broken
        tableService.unloadTable(projectName, "DEFAULT.TEST_KYLIN_FACT", false);

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(getModelDisplayStatus.apply(getModel.apply(projectName, scd2Model.getUuid())),
                    ModelStatusToDisplayEnum.BROKEN);
            Assert.assertEquals(getModelDisplayStatus.apply(getModel.apply(projectName, normalModel.getUuid())),
                    ModelStatusToDisplayEnum.BROKEN);
        });

        //broken -> repair
        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, projectName);

        NDataflowManager.getInstance(getTestConfig(), projectName).getDataflow(scd2Model.getId());
        await().atMost(60 * 2, TimeUnit.SECONDS).untilAsserted(() -> {
            //normal model should be online
            Assert.assertEquals(getModelDisplayStatus.apply(getModel.apply(projectName, normalModel.getUuid())),
                    ModelStatusToDisplayEnum.ONLINE);
            Assert.assertEquals(getModelDisplayStatus.apply(getModel.apply(projectName, scd2Model.getUuid())),
                    ModelStatusToDisplayEnum.OFFLINE);
        });

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

    @Test
    public void testChangeTableAlias_ClearRecommendation() throws Exception {
        val modelRequest = prepare();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        AtomicBoolean clean = new AtomicBoolean(false);
        val manager = spyOptimizeRecommendationManager();
        Mockito.doAnswer(x -> {
            String id = x.getArgument(0);
            if (modelId.equals(id)) {
                clean.set(true);
            }
            return null;
        }).when(manager).handleTableAliasModify(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        val oldAlias = modelRequest.getJoinTables().get(0).getAlias();
        modelRequest.getJoinTables().get(0).setAlias(oldAlias + "_1");
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), "default").updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, "default");
        modelService.updateDataModelSemantic("default", modelRequest);

        Assert.assertTrue(clean.get());
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
        thrown.expectMessageInTransaction("Table oriented model [nmodel_basic] can not build segments manually!");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT")
                        && ccException.getConflictingModel().equals("nmodel_basic_inner")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.DEAL_AMOUNT is already used in model nmodel_basic_inner,"
                                        + " you should apply the same expression as ' TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT ' here,"
                                        + " or use a different computed column name.");

            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();

        List<NDataModelResponse> dataModelDescs = modelService.getModels("nmodel_basic", "default", true, null, null,
                "", false);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        field.setAccessible(true);
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
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("columnName");
        field.setAccessible(true);
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

                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR)
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
                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME)
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getConflictingModel().equals("nmodel_cc_test")
                        && "UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY)".equals(ccException.getAdvise())
                        && ccException.getBadCC().equals("BUYER_ACCOUNT.COUNTRY_UPPER")
                        && ccException.getMessage().equals(
                                "Column name for computed column BUYER_ACCOUNT.COUNTRY_UPPER is already used in model nmodel_cc_test, you should apply the same expression as ' UPPER(BUYER_ACCOUNT.ACCOUNT_COUNTRY) ' here, or use a different computed column name.");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR)
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_COUNTRY_ABBR is already used in model nmodel_basic, you should apply the same expression as ' SUBSTR(SELLER_ACCOUNT.ACCOUNT_COUNTRY,0,1) ' here, or use a different computed column name.");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression as ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_EXPR_DIFF_NAME)
                        && ccException.getAdvise().equals("LEFTJOIN_BUYER_COUNTRY_ABBR")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR_2")
                        && ccException.getMessage().equals(
                                "Expression SUBSTR(BUYER_ACCOUNT.ACCOUNT_COUNTRY,0,1) in computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR_2 is already defined by computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR from model nmodel_basic, you should use the same column name: ' LEFTJOIN_BUYER_COUNTRY_ABBR ' .");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression as ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
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
            Assert.assertEquals("The computed column's name:[LOCAL] is a sql keyword, please choose another name.",
                    e.getMessage());
        }

        try {
            // CALCITE
            ModelService.checkCCName("MSCK");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("The computed column's name:[MSCK] is a sql keyword, please choose another name.",
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise() == null && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME")
                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic, you should apply the same expression like ' CONCAT(SELLER_ACCOUNT.ACCOUNT_ID, SELLER_COUNTRY.NAME) ' here, or use a different computed column name.");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SAME_NAME_DIFF_EXPR)
                        && ccException.getAdvise().equals("CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)")
                        && ccException.getConflictingModel().equals("nmodel_basic")
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME")

                        && ccException.getMessage().equals(
                                "Column name for computed column TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME is already used in model nmodel_basic,"
                                        + " you should apply the same expression as ' CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME) ' here,"
                                        + " or use a different computed column name.");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, at least two computed columns share the same column name: DEAL_AMOUNT, please use different column name");
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
    public void testSameNameOnDifferentAliasTableInOneModel() throws IOException {

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
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, at least two computed columns share the same column name: DEAL_AMOUNT, please use different column name");
            }
        });

        Serializer<NDataModel> serializer = modelService.getDataModelManager("default").getDataModelSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/cc_test/default/model_desc/nmodel_cc_test.json").toPath(),
                Charset.defaultCharset()), "\n");

        String str = "\"computed_columns\": [";
        int i = contents.indexOf(str) + str.length();
        String oneMoreCC = "  {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_ACCOUNT\",\n"
                + "      \"tableAlias\": \"BUYER_ACCOUNT\",\n" + "      \"columnName\": \"DEAL_AMOUNT\",\n"
                + "      \"expression\": \"BUYER_ACCOUNT.ACCOUNT_ID\",\n" + "      \"datatype\": \"bigint\",\n"
                + "      \"comment\": \"bla bla\"\n" + "    },";
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, computed column TEST_KYLIN_FACT.DEAL_AMOUNT share same expression as TEST_KYLIN_FACT.DEAL_AMOUNT_2, please remove one");
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
                return ccException.getCauseType().equals(BadModelException.CauseType.SELF_CONFLICT)
                        && ccException.getAdvise() == null && ccException.getConflictingModel() == null
                        && ccException.getBadCC().equals("TEST_KYLIN_FACT.DEAL_AMOUNT")
                        && ccException.getMessage().equals(
                                "In current model, computed column TEST_KYLIN_FACT.DEAL_AMOUNT share same expression as TEST_KYLIN_FACT.DEAL_AMOUNT_2, please remove one");
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
            String oneMoreCC = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                    + "      \"columnName\": \"DEAL_AMOUNT_2\",\n"
                    + "      \"expression\": \"TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT\",\n"
                    + "      \"datatype\": \"decimal\",\n" + "      \"comment\": \"bla bla bla\"\n" + "    },";
            contents = contents.substring(0, i) + oneMoreCC + contents.substring(i);

            InputStream bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
            NDataModel deserialized = serializer.deserialize(new DataInputStream(bais));
            modelService.getDataModelManager("default").createDataModelDesc(deserialized, "ADMIN");
            //TODO modelService.updateModelToResourceStore(deserialized, "default");
        } catch (BadModelException e) {
            modelService.getTableManager("default").resetProjectSpecificTableDesc();
            TableDesc aDefault = modelService.getTableManager("default").getTableDesc("DEFAULT.TEST_KYLIN_FACT");
            Collection<String> allColumnNames = Collections2.transform(Arrays.asList(aDefault.getColumns()),
                    new Function<ColumnDesc, String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable ColumnDesc columnDesc) {
                            return columnDesc.getName();
                        }
                    });
            Assert.assertTrue(!allColumnNames.contains("DEAL_AMOUNT_2"));
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
                "1577811661000", "1609430400000");

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
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1577808000000", pattern),
                dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1609430400000", pattern),
                dataflow.getSegments().get(0).getSegRange().getEnd());
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

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
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
        modelService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertTrue(dataflow.getSegments().get(0).getSegRange().isInfinite());
        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        thrown.expectInTransaction(KylinException.class);
        thrown.expectMessageInTransaction(String.format(
                MsgPicker.getMsg().getSEGMENT_STATUS(SegmentStatusEnumToDisplay.LOADING.name()),
                dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().get(0).getId()));
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
            Assert.assertEquals("Add Job failed due to processing time conflict", exception.getCause().getMessage());
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
            Assert.assertTrue(StringUtils.contains(ex.getMessage(), "Dimension name 'CAL_DT1' already exists."));
        }

        // invalid dimension name
        dimension.setName("CAL_DT1@!");
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertTrue(StringUtils.contains(ex.getMessage(),
                    "Invalid dimension name 'CAL_DT1@!', only supports Chinese or English characters, numbers, spaces and symbol(_ -()%?). 100 characters at maximum."));
        }

        StringBuilder name = new StringBuilder();
        for (int i = 0; i < 101; ++i)
            name.append('a');
        dimension.setName(name.toString());
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception ex) {
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertTrue(StringUtils.contains(ex.getMessage(), "100 characters at maximum."));
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
                    "Invalid measure name 'illegal_measure_name@!', only supports Chinese or English characters, numbers, spaces and symbol(_ -()%?). 100 characters at maximum."));
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
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "Measure name 'count_1' already exists."));
        }

        // duplicate measure definitions
        measure2.setName("count_2");

        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
        } catch (Exception e) {
            Assert.assertEquals(KylinException.class, e.getClass());
            Assert.assertTrue(StringUtils.contains(e.getMessage(),
                    "The measure definition is the same as the measure 'count_2'. So this measure cannot be created."));
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
                    "Duplicate join condition 'TEST_ACCOUNT.ACCOUNT_ID' and 'TEST_KYLIN_FACT.SELLER_ID'."));
        }
    }

    @Test
    public void testBuildIndexManually_TableOriented_exception() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        thrown.expect(KylinException.class);
        thrown.expectMessage("Table oriented model [all_fixed_length] can not build indices manually!");
        modelService.buildIndicesManually(modelId, project);
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
        val response = modelService.buildIndicesManually(modelId, project);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);

    }

    @Test
    public void testBuildIndexManuallyWithoutLayout() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelService.buildIndicesManually(modelId, project);
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
        val response = modelService.buildIndicesManually(modelId, project);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_SEGMENT, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(0, executables.size());

    }

    //test refreshSegments:all Online model, all lag beghind model, One Online One lag behind model
    //first test exception
    @Test
    public void testRefreshSegments_AffectedSegmentRangeChanged_Exception() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Ready segments range has changed, can not refresh, please try again.");
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
        thrown.expectMessage("Can not refresh, some segments is building within the range you want to refresh!");
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
        thrown.expectMessage("Can not refresh, some segments is building within the range you want to refresh!");
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
        modelService.addOldParams(models);
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
        List<NDataModel> brokenModels = modelService.addOldParams(new ArrayList<>(brokenModelResponseList));
        Assert.assertEquals(1, brokenModels.size());
        Assert.assertTrue(Objects.nonNull(brokenModelResponse.getOldParams()));
        Assert.assertEquals(0, brokenModelResponse.getOldParams().getInputRecordSizeBytes());
    }

    @Test
    public void testModelBroken_CleanRecommendation() throws NoSuchFieldException, IllegalAccessException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val recomManger = OptimizeRecommendationManager.getInstance(getTestConfig(), "default");
        val opt = new OptimizeRecommendation();
        opt.setUuid(modelId);
        opt.setLayoutRecommendations(
                Lists.newArrayList(new LayoutRecommendationItem(), new LayoutRecommendationItem()));
        recomManger.save(opt);

        Assert.assertEquals(2, recomManger.getOptimizeRecommendation(modelId).getLayoutRecommendations().size());
        tableService.unloadTable(getProject(), "DEFAULT.TEST_KYLIN_FACT", false);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(modelId);
        Assert.assertTrue(model.isBroken());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0, recomManger.getOptimizeRecommendation(modelId).getLayoutRecommendations().size());
        });
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
        AtomicBoolean clean = new AtomicBoolean(false);
        val manager = spyOptimizeRecommendationManager();
        Mockito.doAnswer(invocation -> {
            String id = invocation.getArgument(0);
            if (modelId.equals(id)) {
                clean.set(true);
            }
            return null;
        }).when(manager).cleanInEffective(Mockito.anyString());
        modelRequest.setSimplifiedMeasures(
                modelRequest.getSimplifiedMeasures().stream().filter(measure -> measure.getId() != 100001)
                        .sorted(Comparator.comparingInt(SimplifiedMeasure::getId)).collect(Collectors.toList()));
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), "default").updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, "default");
        modelService.updateDataModelSemantic("default", modelRequest);

        Assert.assertTrue(clean.get());
    }

    @Test
    public void testRemoveRecommendAggIndexDimensionColumn() throws Exception {
        val modelRequest = prepare();
        modelRequest.getSimplifiedDimensions().remove(0);
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(
                "table index 20000000000 contains invalid column 0. table index can only contain columns added to model.");
        modelService.updateDataModelSemantic("default", modelRequest);
    }

    @Test
    public void testRemoveRecommendAggIndexMeasureColumn() throws Exception {
        val modelRequest = prepare();
        modelRequest.setSimplifiedMeasures(
                modelRequest.getSimplifiedMeasures().stream().filter(measure -> measure.getId() != 100005)
                        .sorted(Comparator.comparingInt(SimplifiedMeasure::getId)).collect(Collectors.toList()));
        thrown.expect(KylinException.class);
        thrown.expectMessage("agg group still contains measure(s) ITEM_COUNT_MAX");
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
        NModelDescResponse model = modelService.getModelDesc("ut_inner_join_cube_partial", "default");
        Assert.assertTrue(model.getProject().equals("default"));
        Assert.assertTrue(model.getName().equals("ut_inner_join_cube_partial"));
        Assert.assertTrue(model.getVersion().equals("4.0.0.0"));
        Assert.assertTrue(model.getMeasures().size() == 11);
        Assert.assertTrue(model.getAggregationGroups().size() == 2);
        Assert.assertNotEquals(0, model.getCreateTime());
    }

    @Test
    public void testComputedColumnNameCheck_PreProcessBeforeModelSave_ExceptionWhenCCNameIsSameWithColumnInLookupTable() {

        expectedEx.expect(KylinException.class);
        expectedEx.expectMessage(
                "In this model, computed column name [SITE_ID] has been used, please rename your computed column.");
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
                "In this model, computed column name [SITE_ID] has been used, please rename your computed column.");
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
        thrown.expectMessage("Illegal users!"
                + " Only the system administrator, project administrator role, and management role can be set as the model owner.");
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
        thrown.expectMessage(String.format("Model %s does not exist or broken in project %s", modelId, project));
        modelService.updateModelOwner(project, modelId, ownerChangeRequest3);

        // test broken model, throw exception
        String brokenModelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(brokenModelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        thrown.expectMessage(String.format("Model %s does not exist or broken in project %s", brokenModelId, project));
        modelService.updateModelOwner(project, brokenModelId, ownerChangeRequest3);
    }

    @Test
    public void testGetCubes0ExistBrokenModel() {
        tableService.unloadTable(getProject(), "DEFAULT.TEST_KYLIN_FACT", false);
        val result = modelService.getCubes0(null, getProject());
        Assert.assertEquals(6, result.size());

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

}
