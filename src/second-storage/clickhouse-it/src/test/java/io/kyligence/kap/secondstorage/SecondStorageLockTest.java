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

package io.kyligence.kap.secondstorage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseCleanJobParam;
import io.kyligence.kap.clickhouse.job.ClickHouseIndexCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableSet;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.JobService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.SegmentHelper;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.EnableScheduler;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import io.kyligence.kap.util.ExecAndComp;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*", "javax.script.*"})
@PrepareForTest({SpringContext.class})
public class SecondStorageLockTest implements JobWaiter {
    private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    private final String userName = "ADMIN";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions",
                    "spark.sql.broadcastTimeout", "900")
    );

    public EnableTestUser enableTestUser = new EnableTestUser();

    public EnableScheduler enableScheduler = new EnableScheduler("table_index_incremental", "src/test/resources/ut_meta");

    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(enableScheduler);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @InjectMocks
    private SecondStorageService secondStorageService = Mockito.spy(new SecondStorageService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Mock
    private SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();

    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final ModelSemanticHelper modelSemanticHelper = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final ModelBuildService modelBuildService = Mockito.spy(ModelBuildService.class);

    @Mock
    private final SegmentHelper segmentHelper = Mockito.spy(new SegmentHelper());

    private EmbeddedHttpServer _httpServer = null;
    protected IndexDataConstructor indexDataConstructor;
    private final SparkSession ss = sharedSpark.getSpark();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class)).thenAnswer((Answer<SecondStorageUpdater>) invocation -> secondStorageService);

        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageService.setAclEvaluate(aclEvaluate);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "semanticUpdater", modelSemanticHelper);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "segmentHelper", segmentHelper);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);

        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());

        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());

        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @Test
    public void testIncrementBuildLockedLayout() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testIncrementBuildLockedLayout(1, clickhouse1);
        }
    }

    @Test
    public void testStatic() {
        JobParam jobParam = new JobParam();
        jobParam.setSecondStorageDeleteLayoutIds(null);
        assertNull(jobParam.getSecondStorageDeleteLayoutIds());

        SecondStorageCleanJobBuildParams params = new SecondStorageCleanJobBuildParams(null, jobParam, null);
        params.setSecondStorageDeleteLayoutIds(jobParam.getSecondStorageDeleteLayoutIds());
        assertNull(params.getSecondStorageDeleteLayoutIds());
    }

    private void testIncrementBuildLockedLayout(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        final String catalog = "default";

        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());


            Set<Long> existTablePlanLayoutIds = new HashSet<>();
            Set<Long> existTableDataLayoutIds = new HashSet<>();

            val layout01 = updateIndex("TRANS_ID");

            buildIncrementalLoadQuery(); // build table index
            checkHttpServer(); // check http server
            ClickHouseUtils.triggerClickHouseJob(getDataFlow(), getConfig()); //load into clickhouse
            existTablePlanLayoutIds.add(layout01);
            existTableDataLayoutIds.add(layout01);

            checkSecondStorageBaseMetadata(true, clickhouse.length, replica);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet()), layout01);

            String sql1 = "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02' limit 1";
            OLAPContext.clearThreadLocalContexts();
            ExecAndComp.queryModel(getProject(), sql1);
            assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
            assertTrue(OLAPContext.getNativeRealizations().stream().findFirst().isPresent());
            assertEquals(OLAPContext.getNativeRealizations().stream().findFirst().get().getLayoutId().longValue(), layout01);

            val layout02 = updateIndex("LSTG_SITE_ID");
            // build new index segment
            buildSegmentAndLoadCH(layout02);

            // removed old index
            indexPlanService.removeIndexes(getProject(), modelId, ImmutableSet.of(layout01));
            getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
            assertTrue(getNExecutableManager().getAllExecutables().stream().anyMatch(ClickHouseIndexCleanJob.class::isInstance));

            // check load second storage button is enable
            assertTrue(checkNDataSegmentResponse());

            val layout03 = updateIndex("IS_EFFECTUAL");

            // build new index segment
            buildSegmentAndLoadCH(layout03);

            removeIndexesFromSegments(getDataFlow().getSegments().getFirstSegment().getId(), layout02);
            removeIndexesFromSegments(getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()), layout02);

            OLAPContext.clearThreadLocalContexts();
            String sql2 = "select LSTG_SITE_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02' limit 1";
            QueryContext.current().setRetrySecondStorage(true);
            ExecAndComp.queryModel(getProject(), sql2);
            assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
            assertTrue(OLAPContext.getNativeRealizations().stream().findFirst().isPresent());

            val layout04 = updateIndex("LEAF_CATEG_ID");

            // build new index segment
            buildSegmentAndLoadCH(layout04);

            getDataFlow().getSegments().forEach(segment -> {
                List<JobInfoResponse.JobInfo> jobInfos = modelBuildService.refreshSegmentById(new RefreshSegmentParams(getProject(), modelId,
                        new String[]{segment.getId()}));

                jobInfos.forEach(j -> waitJobFinish(getProject(), j.getJobId()));
            });

//            OLAPContext.clearThreadLocalContexts();
//            ExecAndComp.queryModel(getProject(), "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-04' limit 1");
//            assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
//            assertTrue(OLAPContext.getNativeRealizations().stream().findFirst().isPresent());
//            assertEquals(OLAPContext.getNativeRealizations().stream().findFirst().get().getLayoutId().longValue(), layout03);

            SecondStorageUtil.cleanSegments(getProject(), modelId + "1", null, null);
            SecondStorageUtil.cleanSegments(getProject(), modelId, getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet()), ImmutableSet.of(layout04));

            getTableFlow().cleanTableData(null);
            getTablePlan().cleanTable(null);

            NExecutableManager.getInstance(getConfig(), getProject()).getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
            waitJobFinish(getProject(), secondStorageService.triggerIndexClean(getProject(), modelId, ImmutableSet.of(layout04)));
            val param = ClickHouseCleanJobParam.builder()
                    .modelId(modelId)
                    .jobId("")
                    .submitter("")
                    .df(getDataFlow())
                    .project(getProject())
                    .needDeleteLayoutIds(ImmutableSet.of(layout03))
                    .segments(ImmutableSet.of(getDataFlow().getSegments().getFirstSegment()))
                    .build();
            ClickHouseIndexCleanJob clean = new ClickHouseIndexCleanJob(param);

            secondStorageService.changeModelSecondStorageState(getProject(), modelId, false);
            return true;
        });
    }

    public String getProject() {
        return "table_index_incremental";
    }

    private void buildIncrementalLoadQuery() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        indexDataConstructor.buildIndex(dfName, timeRange, indexes, true);
    }


    @SneakyThrows
    private void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        assertTrue(content.length() > 0);
    }


    private void checkSecondStorageBaseMetadata(boolean isIncremental, int clickhouseNodeSize, int replicaSize) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(modelId);
        // check TableFlow
        TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(plan);
        TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(flow);

        Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                .filter(SecondStorageUtil::isBaseTableIndex).collect(Collectors.toSet());
        Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());

        for (LayoutEntity layoutEntity : allLayouts) {
            TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(tableEntity);
            TableData data = flow.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(data);
            Assert.assertEquals(isIncremental ? PartitionType.INCREMENTAL : PartitionType.FULL, data.getPartitionType());
            Assert.assertEquals(dsMgr.getDataflow(modelId).getQueryableSegments().size(),
                    data.getPartitions().size() / replicaSize);
            TablePartition partition = data.getPartitions().get(0);
            int shards = Math.min(clickhouseNodeSize / replicaSize, tableEntity.getShardNumbers());
            Assert.assertEquals(shards, partition.getShardNodes().size());
            Assert.assertEquals(shards, partition.getSizeInNode().size());
            Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
        }
    }

    private void checkSecondStorageMetadata(Set<Long> tablePlanLayoutIds, Set<Long> tableDataLayoutIds) {
        Set<Long> existTablePlanLayoutId = getTablePlan().getTableMetas().stream().map(TableEntity::getLayoutID).collect(Collectors.toSet());
        assertEquals(existTablePlanLayoutId.size(), tablePlanLayoutIds.size());
        assertTrue(existTablePlanLayoutId.containsAll(tablePlanLayoutIds));

        Set<Long> existTableDataLayoutId = getTableFlow().getTableDataList().stream().map(TableData::getLayoutID).collect(Collectors.toSet());
        assertEquals(existTableDataLayoutId.size(), tableDataLayoutIds.size());
        assertTrue(existTableDataLayoutId.containsAll(tableDataLayoutIds));
    }

    private void checkSecondStorageSegmentMetadata(Set<String> segmentIds, long layoutId) {
        assertTrue(getTableFlow().getEntity(layoutId).isPresent());
        Set<String> existSegmentIds = getTableFlow().getEntity(layoutId).get().getAllSegments();
        assertEquals(existSegmentIds.size(), segmentIds.size());
        assertTrue(existSegmentIds.containsAll(segmentIds));
    }

    private void mergeSegment(List<String> segmentIds) {
        JobInfoResponse.JobInfo jobInfo = modelBuildService.mergeSegmentsManually(new MergeSegmentParams(getProject(), modelId,
                segmentIds.toArray(new String[]{})));

        waitJobFinish(getProject(), jobInfo.getJobId());
    }

    @Data
    public static class BuildBaseIndexUT {
        @JsonProperty("base_table_index")
        public IndexInfo tableIndex;


        @Data
        public static class IndexInfo {
            @JsonProperty("layout_id")
            private long layoutId;
        }
    }

    private KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    private TableFlow getTableFlow() {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private TablePlan getTablePlan() {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId);
    }

    private NDataflow getDataFlow() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(modelId);
    }

    private NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private NDataModel getNDataModel() {
        return getNDataModelManager().getDataModelDesc(modelId);
    }

    private NExecutableManager getNExecutableManager() {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private Set<String> getAllSegmentIds() {
        return getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
    }

    private String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }

    private static String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ModelRequest getChangedModelRequest(String columnName) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        val partitionDesc = model.getPartitionDesc();
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        request.setPartitionDesc(model.getPartitionDesc());
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private NDataModel.NamedColumn getNamedColumn(ColumnDesc columnDesc) {
        NDataModel.NamedColumn transIdColumn = new NDataModel.NamedColumn();
        transIdColumn.setId(Integer.parseInt(columnDesc.getId()));
        transIdColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        transIdColumn.setName(columnDesc.getTable().getName() + "_" + columnDesc.getName());
        transIdColumn.setAliasDotColumn(columnDesc.getTable().getName() + "." + columnDesc.getName());
        return transIdColumn;
    }

    private void buildSegmentAndLoadCH(long layoutId) throws InterruptedException {
        // build new index segment
        for (NDataSegment segment : getDataFlow().getSegments()) {
            indexDataConstructor.buildSegment(modelId, segment, ImmutableSet.of(getDataFlow().getIndexPlan().getLayoutEntity(layoutId)), true, null);
        }

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
        ClickHouseUtils.triggerClickHouseJob(getDataFlow(), getConfig());
    }

    private long updateIndex(String columnName) throws IOException {
        val indexResponse = modelService.updateDataModelSemantic(getProject(), getChangedModelRequest(columnName));
        val layoutId = JsonUtil.readValue(JsonUtil.writeValueAsString(indexResponse), BuildBaseIndexUT.class).tableIndex.layoutId;

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
        Optional<AbstractExecutable> job = getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).findFirst();
        assertFalse(job.isPresent());

        return layoutId;
    }

    private boolean checkNDataSegmentResponse() {
        NDataSegmentResponse res = new NDataSegmentResponse(getDataFlow(), getDataFlow().getFirstSegment());
        return res.isHasBaseTableIndexData();
    }

    private void removeIndexesFromSegments(String segmentId, long indexId) {
        removeIndexesFromSegments(ImmutableList.of(segmentId), indexId);
    }

    private void removeIndexesFromSegments(List<String> segmentIds, long indexId) {
        modelService.removeIndexesFromSegments(getProject(), modelId, segmentIds, ImmutableList.of(indexId));
    }

    private void setQuerySession(String catalog, String jdbcUrl, String driverClassName) {
        System.setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", jdbcUrl);
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver", driverClassName);
    }
}
