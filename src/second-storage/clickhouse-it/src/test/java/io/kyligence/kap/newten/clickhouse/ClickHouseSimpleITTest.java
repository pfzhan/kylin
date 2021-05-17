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
package io.kyligence.kap.newten.clickhouse;

import com.google.common.collect.ImmutableList;
import com.clearspring.analytics.util.Preconditions;
import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.secondstorage.SchemaCache$;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown2$;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCScan;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static org.awaitility.Awaitility.await;

@Slf4j
public class ClickHouseSimpleITTest extends NLocalWithSparkSessionTest {
    public final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    public final String userName = "ADMIN";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();


    /**
     * According to JUnit's mechanism, the super class's method will be hidden by the child class for the same
     * Method signature. So we use {@link #beforeClass()} to hide {@link NLocalWithSparkSessionTest#beforeClass()}
     */
    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.ensureSparkConf();
        ClickHouseUtils.InjectNewPushDownRule(sparkConf);
        NLocalWithSparkSessionTest.beforeClass();
        Assert.assertTrue(SparderEnv.getSparkSession()
                .sessionState().optimizer().preCBORules().contains(V2ScanRelationPushDown2$.MODULE$));
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }
    private EmbeddedHttpServer _httpServer = null;
    @Rule
    public TestName testName = new TestName();

    protected boolean needHttpServer() {
        return true;
    }
    protected void doSetup() throws Exception {

    }
    protected void prepareMeta() throws IOException {
        this.createTestMetadata("src/test/resources/ut_meta");
        Assert.assertTrue(tempMetadataDirectory.exists());
        final File testProjectMetaDir = new File(tempMetadataDirectory.getPath() + "/metadata/" + getProject());
        final String message = String.format(Locale.ROOT,
                "%s's meta (%s) doesn't exist, please check!",
                getProject(), testProjectMetaDir.getCanonicalPath());
        Assert.assertTrue(message, testProjectMetaDir.exists());
    }

    /**
     * It will hidden  {@link NLocalWithSparkSessionTest#setUp()} }
     */
    @Before
    public void setUp() throws Exception {
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        prepareMeta();
        ExecutableUtils.initJobFactory();

        doSetup();

        if (needHttpServer()) {
            _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());
        }

        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        SecurityContextHolder.getContext().setAuthentication(authentication);
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @After
    public void tearDown() throws Exception {
        if (_httpServer != null) {
            _httpServer.stopServer();
            _httpServer = null;
        }
        ClickHouseConfigLoader.clean();
        NDefaultScheduler.destroyInstance();
        ResourceStore.clearCache();
        FileUtils.deleteDirectory(new File("../clickhouse-it/metastore_db"));
        SchemaCache$.MODULE$.reset();
        super.tearDown();
    }

    @Override
    public String getProject() {
        String project;
        if (testName.getMethodName().toLowerCase(Locale.ROOT).contains("incremental")) {
            project = "table_index_incremental";
        } else {
            project = "table_index";
        }
        return project;
    }

    @Test
    public void testSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShard", false, clickhouse);
        }
    }

    @Test
    public void testSingleShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardDoubleReplica", false, 2, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testTwoShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardDoubleReplica", false, 2,
                    clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testTwoShards() throws Exception {
        // TODO: make sure splitting data into two shards
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShards", false, clickhouse1, clickhouse2);
        }
    }


    protected void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        Assert.assertTrue(content.length() > 0);
    }

    @Test
    public void testIncrementalSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalSingleShard", true, clickhouse);
        }
    }

    @Test
    public void testIncrementalTwoShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShard", true, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testIncrementalTwoShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardDoubleReplica", true, 2,
                    clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testIncrementalCleanSegment() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalCleanSegment", true, clickhouse);
            triggerSegmentClean();
            val manager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
            Preconditions.checkState(manager.isPresent());
            val tablePartitions = Objects.requireNonNull(manager.get().get(cubeName).orElse(null))
                    .getTableDataList().get(0).getPartitions();
            Assert.assertEquals(0, tablePartitions.size());
        }
    }

    @Test
    public void testIncrementalCleanModel() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalCleanModel", true, clickhouse);
            val request = new ModelEnableRequest();
            request.setModel(cubeName);
            request.setEnabled(false);
            request.setProject(getProject());
            val jobInfo = secondStorageEndpoint.enableStorage(request);
            Assert.assertEquals(1, jobInfo.getData().getJobs().size());
            waitJobFinish(jobInfo.getData().getJobs().get(0).getJobId());
            val tablePlanManager = SecondStorageUtil.tablePlanManager(KylinConfig.getInstanceFromEnv(), getProject());
            val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), getProject());
            Preconditions.checkState(tableFlowManager.isPresent() && tablePlanManager.isPresent() && nodeGroupManager.isPresent());
            Assert.assertEquals(0, tablePlanManager.get().listAll().size());
            Assert.assertEquals(0, tableFlowManager.get().listAll().size());
            Assert.assertEquals(1, nodeGroupManager.get().listAll().size());
        }
    }

    @Test
    public void testIncrementalCleanProject() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalCleanProject", true, clickhouse);
            val request = new ProjectEnableRequest();
            request.setProject(getProject());
            request.setEnabled(false);
            request.setNewNodes(null);
            val jobInfo = secondStorageEndpoint.enableProjectStorage(request);
            Assert.assertEquals(1, jobInfo.getData().getJobs().size());
            waitJobFinish(jobInfo.getData().getJobs().get(0).getJobId());
            val manager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), getProject());
            Preconditions.checkState(manager.isPresent() && nodeGroupManager.isPresent());
            Assert.assertEquals(0, manager.get().listAll().size());
            Assert.assertEquals(0, nodeGroupManager.get().listAll().size());
        }
    }

    private JobParam triggerSegmentClean() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfManager = NDataflowManager.getInstance(config, getProject());
        val df = dfManager.getDataflow(cubeName);
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler segmentCleanJobHandler = new SecondStorageSegmentCleanJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.segmentCleanParam(getProject(), cubeName, userName, segments.stream().map(NDataSegment::getId).collect(Collectors.toSet()));
        segmentCleanJobHandler.handle(jobParam);
        waitJobFinish(jobParam.getJobId());
        return jobParam;
    }


    protected void buildIncrementalLoadQuery() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = cubeName;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);

        val timeRange1 = new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-01-02");
        val indexes = df.getIndexPlan().getAllLayouts().stream().filter(layoutEntity -> layoutEntity.getId() == 20000000001L)
                .collect(Collectors.toSet());
        buildCuboid(dfName, timeRange1, indexes, true);
        val timeRange2 = new SegmentRange.TimePartitionedSegmentRange("2012-01-02", "2012-01-03");
        buildCuboid(dfName, timeRange2, indexes, true);
    }

    protected void buildFullLoadQuery() throws Exception {
        fullBuildCube(cubeName, getProject());
    }

    protected void mergeSegments(List<String> segIds) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(cubeName);
        val jobManager = JobManager.getInstance(config, getProject());
        long start = Long.MAX_VALUE;
        long end = -1;
        for (String id : segIds) {
            val segment = df.getSegment(id);
            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();
            if (segmentStart < start)
                start = segmentStart;
            if (segmentEnd > end)
                end = segmentEnd;
        }

        val mergeSeg = dfMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(start, end), true);
        val jobParam = new JobParam(mergeSeg, cubeName, userName);
        val jobId = jobManager.mergeSegmentJob(jobParam);
        waitJobFinish(jobId);
    }

    private void waitJobFinish(String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, getProject());
        DefaultChainedExecutable job = (DefaultChainedExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        val firstErrorMsg = job.getTasks().stream()
                .filter(abstractExecutable -> abstractExecutable.getStatus() == ExecutableState.ERROR)
                .findFirst()
                .map(task -> executableManager.getOutputFromHDFSByJobId(job.getId(), task.getId(), Integer.MAX_VALUE).getVerboseMsg())
                .orElse("Unknown Error");
        Assert.assertEquals(firstErrorMsg,
                ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
    }

    protected void refreshSegment(String segId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(cubeName);
        val jobManager = JobManager.getInstance(config, getProject());
        NDataSegment newSeg = dfMgr.refreshSegment(df, df.getSegment(segId).getSegRange());
        val jobParam = new JobParam(newSeg, df.getModel().getId(), userName);
        val jobId = jobManager.refreshSegmentJob(jobParam);
        waitJobFinish(jobId);
    }

    protected String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }


    private String simulateJobMangerAddJob(JobParam jobParam, AbstractJobHandler handler) {
        ExecutableUtil.computeParams(jobParam);
        handler.handle(jobParam);
        return jobParam.getJobId();
    }

    protected void build_load_query(String catalog, boolean incremental, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        build_load_query(catalog, incremental, 1, clickhouse);
    }

    private JobParam triggerClickHouseJob(NDataflow df, KylinConfig config) {
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam =
                SecondStorageJobParamUtil.of(getProject(), cubeName, userName, segments.stream().map(NDataSegment::getId));
        String jobId = simulateJobMangerAddJob(jobParam, localHandler);
        waitJobFinish(jobId);
        return jobParam;
    }

    protected void build_load_query(String catalog, boolean incremental, int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllNames(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), cubeName, true);
            // build table index
            if (incremental) {
                buildIncrementalLoadQuery();
            } else {
                buildFullLoadQuery();
            }

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

            // check http server
            checkHttpServer();

            //load into clickhouse
            //load into clickhouse
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);
            val segments = new HashSet<>(df.getQueryableSegments());
            triggerClickHouseJob(df, config);

            // test refresh segment
            val needRefresh = dsMgr.getDataflow(cubeName).getSegments().get(0);
            refreshSegment(needRefresh.getId());

            // test merge segment
            if (incremental) {
                mergeSegments(dsMgr.getDataflow(cubeName).getQueryableSegments().stream()
                        .map(NDataSegment::getId).collect(Collectors.toList()));
            }


            // check TableFlow
            TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(plan);
            TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(flow);

            Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                    .filter(layout -> SecondStorageUtil.isBaseIndex(layout.getId())).collect(Collectors.toSet());
            Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());
            for (LayoutEntity layoutEntity : allLayouts) {
                TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(tableEntity);
                TableData data = flow.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(data);
                Assert.assertEquals(incremental ? PartitionType.INCREMENTAL : PartitionType.FULL, data.getPartitionType());
                Assert.assertEquals(dsMgr.getDataflow(cubeName).getQueryableSegments().size(), data.getPartitions().size() / replica);
                TablePartition partition = data.getPartitions().get(0);
                int shards = Math.min(clickhouse.length / replica, tableEntity.getShardNumbers());
                Assert.assertEquals(shards, partition.getShardNodes().size());
                Assert.assertEquals(shards, partition.getSizeInNode().size());
                Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
            }

            // check
            overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            ss.sessionState().conf().setConfString(
                    "spark.sql.catalog." + catalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            ss.sessionState().conf().setConfString(
                    "spark.sql.catalog." + catalog + ".url",
                    clickhouse[0].getJdbcUrl());
            ss.sessionState().conf().setConfString(
                    "spark.sql.catalog." + catalog + ".driver",
                    clickhouse[0].getDriverClassName());

            // check ClickHouse
            checkQueryResult(incremental, clickhouse, replica);
            return true;
        });
    }

    private void checkQueryResult(boolean incremental, JdbcDatabaseContainer<?>[] clickhouse, int replica) throws Exception {
        Dataset<Row> dataset =
                NExecAndComp.queryCubeAndSkipCompute(getProject(), "select PRICE from TEST_KYLIN_FACT group by PRICE");
        Assert.assertTrue(ClickHouseUtils.findShardJDBCTable(dataset.queryExecution().optimizedPlan()));

            // check Aggregate push-down
            Dataset<Row> groupPlan =
                    NExecAndComp.queryCubeAndSkipCompute(getProject(), "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE");
            ShardJDBCScan shardJDBCScan = ClickHouseUtils.findShardScan(groupPlan.queryExecution().optimizedPlan());
            Assert.assertEquals(clickhouse.length/replica, shardJDBCScan.relation().parts().length);
            /* See
              1. src/examples/test_case_data/localmeta/metadata/table_index/table/DEFAULT.TEST_KYLIN_FACT.json
              2. src/examples/test_case_data/localmeta/metadata/table_index_incremental/table/DEFAULT.TEST_KYLIN_FACT.json
             * PRICE  <=>  9, hence its column name in ck is c9
            */
            List<String> expected = ImmutableList.of("c9");
            ClickHouseUtils.checkGroupBy(shardJDBCScan, expected);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        if (incremental) {
            val result = SparderEnv.getSparkSession()
                    .sql("select * from TEST_KYLIN_FACT where CAL_DT >= '2012-01-01' and CAL_DT < '2012-01-03'");
            result.createOrReplaceTempView("TEST_KYLIN_FACT");
        }
        query.add(Pair.newPair("query_table_index1", "select PRICE from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index2", "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index3", "select max(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index4", "select min(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index5", "select count(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index6", "select count(distinct PRICE) from TEST_KYLIN_FACT group by PRICE"));

        query.add(Pair.newPair("query_table_index7", "select sum(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index8", "select max(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index9", "select min(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index10", "select count(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index11", "select count(distinct PRICE) from TEST_KYLIN_FACT"));

        query.add(Pair.newPair("query_table_index12", "select sum(PRICE),sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index13", "select max(PRICE),max(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index14", "select min(PRICE),min(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index15", "select count(PRICE),count(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index16", "select count(distinct PRICE),count(distinct ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index17", "select min(PRICE) from TEST_KYLIN_FACT where ORDER_ID=2 group by PRICE "));

        query.add(Pair.newPair("query_agg_index1", "select sum(ORDER_ID) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_agg_index2", "select sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));

        query.add(Pair.newPair("query_agg_inner_col_index1",
                "select \n"
                        + "  sum(ORDER_ID + 1), \n"
                        + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n"
                        + "  ) from TEST_KYLIN_FACT \n"
                        + "group by \n"
                        + "  LSTG_FORMAT_NAME\n"));
        query.add(Pair.newPair("query_agg_inner_col_index2",
                "select \n"
                        + "  sum(ORDER_ID + 1), \n"
                        + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n"
                        + "  ) \n"
                        + "from \n"
                        + "  (\n"
                        + "    select \n"
                        + "      a1.ORDER_ID - 10 as ORDER_ID, \n"
                        + "      a1.LSTG_FORMAT_NAME\n"
                        + "    from \n"
                        + "      TEST_KYLIN_FACT a1\n"
                        + "  ) \n"
                        + "where \n"
                        + "  order_id > 10 \n"
                        + "group by \n"
                        + "  LSTG_FORMAT_NAME\n"));

        NExecAndComp.execAndCompareNew(query, getProject(), NExecAndComp.CompareLevel.SAME, "left", null);
    }
}
