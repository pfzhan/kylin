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

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.columnMapping;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
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
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.clickhouse.parser.ShowDatabasesParser;
import io.kyligence.kap.clickhouse.tool.ClickHouseSanityCheckTool;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.JobService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLockOperateRequest;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.SecondStorageMetadataRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseSimpleITTest extends NLocalWithSparkSessionTest implements JobWaiter {
    public final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    public final String userName = "ADMIN";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private SecondStorageService secondStorageService = new SecondStorageService();
    private ModelService modelService = Mockito.mock(ModelService.class);
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
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().optimizer().preCBORules()
                .contains(V2ScanRelationPushDown2$.MODULE$));
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
        final String message = String.format(Locale.ROOT, "%s's meta (%s) doesn't exist, please check!", getProject(),
                testProjectMetaDir.getCanonicalPath());
        Assert.assertTrue(message, testProjectMetaDir.exists());
    }

    /**
     * It will hidden  {@link NLocalWithSparkSessionTest#setUp()} }
     */
    @Before
    public void setUp() throws Exception {
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);

        secondStorageService.setAclEvaluate(aclEvaluate);
        secondStorageService.setJobService(jobService);
        secondStorageService.setModelService(modelService);
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);
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
        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @After
    public void tearDown() throws Exception {
        if (_httpServer != null) {
            _httpServer.stopServer();
            _httpServer = null;
        }
        QueryContext.reset();
        ClickHouseConfigLoader.clean();
        NDefaultScheduler.destroyInstance();
        ResourceStore.clearCache();
        FileUtils.deleteDirectory(new File("../clickhouse-it/metastore_db"));
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
            EnvelopeResponse response = secondStorageEndpoint.tableSync(getProject());
            Assertions.assertEquals(response.getCode(), "000");
        }
    }

    @Test
    public void testSingleShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testTwoShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2, clickhouse3,
                    clickhouse4);
        }
    }

    @Test
    public void testAddSecondStorageResponseWithHA() throws Exception {
        secondStorageEndpoint.setModelService(modelService);
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);

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

            val tableFlowManager = SecondStorage.tableFlowManager(config, getProject());
            val tableFlow = tableFlowManager.get(df.getId()).orElse(null);
            String id = tableFlow.getTableDataList().get(0).getPartitions().get(0).getSegmentId();
            segmentResponse1.setId(id);
            segmentResponse2.setId(id);
            segmentResponse3.setId(id);

            mockSegments.add(segmentResponse1);
            mockSegments.add(segmentResponse3);
            mockSegments.add(segmentResponse2);

            //mockito.when(SecondStorageUtil)
            ModelService ms = new ModelService();

            ms.addSecondStorageResponse(cubeName, getProject(), mockSegments, df);
            Assert.assertEquals(2, (mockSegments.get(0)).getSecondStorageNodes().get("pair0").size());

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

    @Test
    public void testIncrementalTwoShardDoubleReplicaHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardDoubleReplica", true, false, 2, null, null, clickhouse1, clickhouse2,
                    clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testIncrementalTwoShardDoubleReplicaRows() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            final String queryCatalog = "testIncrementalTwoShardDoubleReplicaRows";
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

            JdbcDatabaseContainer<?>[] clickhouse = new JdbcDatabaseContainer[] { clickhouse1, clickhouse2, clickhouse3,
                    clickhouse4 };

            Map<String, List<Node>> clusterNode = new HashMap<>();
            int pairNum = clickhouse.length / 2;
            IntStream.range(0, pairNum).forEach(idx -> clusterNode.put("pair" + idx, new ArrayList<>()));
            ClusterInfo cluster = new ClusterInfo().setKeepAliveTimeout("600000").setSocketTimeout("600000")
                    .setCluster(clusterNode);
            int i = 0;
            for (JdbcDatabaseContainer<?> jdbcDatabaseContainer : clickhouse) {
                Node node = new Node();
                node.setName(String.format(Locale.ROOT, "node%02d", i == 2 ? 5 : i));
                URI uri = URI.create(jdbcDatabaseContainer.getJdbcUrl().replace("jdbc:", ""));
                node.setIp(uri.getHost());
                node.setPort(uri.getPort());
                node.setUser("default");
                clusterNode.get("pair" + i % pairNum).add(node);
                i += 1;
            }
            File file = File.createTempFile("clickhouse", ".yaml");
            ClickHouseConfigLoader.getConfigYaml().dump(
                    JsonUtil.readValue(JsonUtil.writeValueAsString(cluster), Map.class),
                    new PrintWriter(file, Charset.defaultCharset().name()));
            Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
            Unsafe.setProperty(SecondStorageConstants.NODE_REPLICA, String.valueOf(2));
            SecondStorage.init(true);

            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(4, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), cubeName, true);
            /// build table index
            buildIncrementalLoadQuery();

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            if ("table_index_incremental".equals(getProject())) {
                Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            } else if ("table_index".equals(getProject())) {
                Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            }

            // check http server
            checkHttpServer();

            //load into clickhouse
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);
            triggerClickHouseJob(df, config);

            List<Integer> rowsList = getHAModelRowCount(getProject(), cubeName,
                    df.getIndexPlan().getBaseTableLayoutId(), new JdbcDatabaseContainer[] { clickhouse1, clickhouse3 });

            Assert.assertEquals(1, rowsList.stream().distinct().count());

            rowsList = getHAModelRowCount(getProject(), cubeName, df.getIndexPlan().getBaseTableLayoutId(),
                    new JdbcDatabaseContainer[] { clickhouse2, clickhouse4 });

            Assert.assertEquals(1, rowsList.stream().distinct().count());
        }
    }

    @Test
    public void testTwoShardDoubleReplicaRows2() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            final String catalog = "testTwoShardDoubleReplicaRows2";
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, catalog);

            ClickHouseUtils.internalConfigClickHouse(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 }, 2);

            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), cubeName, true);
            /// build table index
            buildFullLoadQuery();

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            if ("table_index_incremental".equals(getProject())) {
                Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            } else if ("table_index".equals(getProject())) {
                Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            }

            // check http server
            checkHttpServer();

            //load into clickhouse
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);
            triggerClickHouseJob(df, config);

            Map<String, List<Node>> cluster = ClickHouseConfigLoader.getInstance().getCluster().getCluster();

            for (String pair : cluster.keySet()) {
                Map<String, Map<String, Boolean>> nodeStatusMap = new HashMap<>();
                Map<String, Boolean> nodeStatus = Maps.newHashMap();
                cluster.get(pair).forEach(n -> nodeStatus.put(n.getName(), true));
                nodeStatusMap.put(pair, nodeStatus);
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
            }

            // check
            overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", clickhouse1.getJdbcUrl());
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver",
                    clickhouse2.getDriverClassName());

            String sql1 = "select order_id from TEST_KYLIN_FACT ";
            ExecAndComp.queryModel(getProject(), sql1);
            Assert.assertTrue(
                    OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));

            clickhouse1.stop();
            clickhouse2.stop();

            ExecAndComp.queryModel(getProject(), sql1);
            Assert.assertTrue(
                    OLAPContext.getNativeRealizations().stream().noneMatch(NativeQueryRealization::isSecondStorage));
        }
    }

    @Test
    public void testshard2Replica2AndDifferentGroupNodeDown() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("test2shard2ReplicaAndDifferentGroupNodeDown",
                    false,
                    false,
                    2,
                    () ->{
                        clickhouse1.stop();
                        clickhouse2.stop();
                        return null;
                    },
                    () -> {
                        String sql = "select order_id from TEST_KYLIN_FACT ";
                        OLAPContext.clearThreadLocalContexts();
                        ExecAndComp.queryModel(getProject(), sql);
                        Assert.assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));

                        return null;
                    },
                    clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testQueryWithClickHouseHADownSameGroup() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testQueryWithClickHouseHASuccess",
                    false,
                    false,
                    2,
                    () ->{
                        Map<String, List<Node>> cluster = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
                        int index = 0;
                        Map<String, Map<String, Boolean>> nodeStatusMap = new HashMap<>();
                        for (String pair : cluster.keySet()) {
                            Map<String, Boolean> nodeStatus = com.google.common.collect.Maps.newHashMap();
                            for (Node n : cluster.get(pair)) {
                                nodeStatus.put(n.getName(), index == 0 || index == 3);
                                index++;
                            }
                            nodeStatusMap.put(pair, nodeStatus);
                        }
                        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                        return null;
                    },
                    () -> {
                        String sql = "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE";
                        ExecAndComp.queryModel(getProject(), sql);
                        Assert.assertTrue(OLAPContext.getNativeRealizations().stream().anyMatch(NativeQueryRealization::isSecondStorage));

                        Map<String, List<Node>> cluster = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
                        Map<String, Map<String, Boolean>> nodeStatusMap = new HashMap<>();
                        for (String pair : cluster.keySet()) {
                            Map<String, Boolean> nodeStatus = com.google.common.collect.Maps.newHashMap();
                            for (Node n : cluster.get(pair)) {
                                nodeStatus.put(n.getName(), true);
                            }
                            nodeStatusMap.put(pair, nodeStatus);
                        }
                        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                        return null;
                    },
                    clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testQueryWithClickHouseHADownOneShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testQueryWithClickHouseHASuccess",
                    false,
                    false,
                    2,
                    () ->{
                        Map<String, List<Node>> cluster = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
                        int index = 0;
                        Map<String, Map<String, Boolean>> nodeStatusMap = new HashMap<>();
                        for (String pair : cluster.keySet()) {
                            Map<String, Boolean> nodeStatus = Maps.newHashMap();
                            for (Node n : cluster.get(pair)) {
                                nodeStatus.put(n.getName(), index == 0);

                            }
                            index++;
                            nodeStatusMap.put(pair, nodeStatus);
                        }
                        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                        return null;
                    },
                    () -> {
                        String sql = "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE";
                        ExecAndComp.queryModel(getProject(), sql);
                        Assert.assertTrue(OLAPContext.getNativeRealizations().stream().noneMatch(NativeQueryRealization::isSecondStorage));

                        Map<String, List<Node>> cluster = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
                        Map<String, Map<String, Boolean>> nodeStatusMap = new HashMap<>();
                        for (String pair : cluster.keySet()) {
                            Map<String, Boolean> nodeStatus = com.google.common.collect.Maps.newHashMap();
                            for (Node n : cluster.get(pair)) {
                                nodeStatus.put(n.getName(), true);
                            }
                            nodeStatusMap.put(pair, nodeStatus);
                        }
                        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                        return null;
                    },
                    clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @SneakyThrows
    protected void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        Assert.assertTrue(content.length() > 0);
    }

    @Test
    public void testIncrementalSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalSingleShard", true, clickhouse, clickhouse2);

            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve("node01"))) {
                ShowDatabases showDatabases = new ShowDatabases();
                List<String> databases = clickHouse.query(showDatabases.toSql(), ShowDatabasesParser.SHOW_DATABASES);
                databases = databases.stream()
                        .filter(database -> !database.equals("default") && !database.equals("system"))
                        .collect(Collectors.toList());
                Assertions.assertTrue(new HashSet<>(databases).contains("UT_table_index_incremental"));
                List<String> tables = clickHouse.query(
                        ShowTables.createShowTables("UT_table_index_incremental").toSql(),
                        ShowDatabasesParser.SHOW_DATABASES);
                Assertions
                        .assertTrue(new HashSet<>(tables).contains("acfde546_2cc9_4eec_bc92_e3bd46d4e2ee_20000000001"));
            }
            EnvelopeResponse response = secondStorageEndpoint.tableSync(getProject());
            Assertions.assertEquals(response.getCode(), "000");
            SecondStorageMetadataRequest request = new SecondStorageMetadataRequest();
            request.setProject(getProject());
            response = secondStorageEndpoint.sizeInNode(request);
            Assertions.assertEquals(response.getCode(), "000");
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
            build_load_query("testIncrementalTwoShardDoubleReplica", true, 2, null, clickhouse1, clickhouse2,
                    clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testIncrementalSingleShardTripleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();) {
            build_load_query("testIncrementalSingleShardTripleReplica", true, 3, () -> {
                clickhouse3.stop();
                clickhouse1.stop();
                return null;
            }, clickhouse1, clickhouse2, clickhouse3);
        }
    }

    @Test
    public void testIncrementalCleanSegment() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalCleanSegment", true, clickhouse);
            val jobParam = triggerSegmentClean();
            waitJobFinish(jobParam.getJobId());
            val manager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
            Preconditions.checkState(manager.isPresent());
            val tablePartitions = Objects.requireNonNull(manager.get().get(cubeName).orElse(null)).getTableDataList()
                    .get(0).getPartitions();
            Assert.assertEquals(1, tablePartitions.size());
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
            Preconditions.checkState(
                    tableFlowManager.isPresent() && tablePlanManager.isPresent() && nodeGroupManager.isPresent());
            Assert.assertEquals(0, tablePlanManager.get().listAll().size());
            Assert.assertEquals(0, tableFlowManager.get().listAll().size());
            Assert.assertEquals(1, nodeGroupManager.get().listAll().size());
        }
    }

    @Ignore
    @Test
    public void testIncrementalCleanModel2() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            changeProjectSecondStorageState("testIncrementalSingleShard", true, 1, clickhouse1, clickhouse2);
            build_load_query("testIncrementalSingleShard", true, clickhouse1);
            build_load_query("testSingleShard", false, clickhouse2);

            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve("node01"))) {
                ShowDatabases showDatabases = new ShowDatabases();
                List<String> databases = clickHouse.query(showDatabases.toSql(), ShowDatabasesParser.SHOW_DATABASES);
                for (String database : databases) {
                    List<String> tables = clickHouse.query(ShowTables.createShowTables(database).toSql(),
                            ShowDatabasesParser.SHOW_DATABASES);
                    System.out.println(tables);
                }
                //                CreateDatabase createDatabase = CreateDatabase.createDatabase("ut_db_sync");
                //                Assertions.assertTrue(clickHouse.apply(createDatabase.toSql()));
                //
                //                String createTableSql = "CREATE TABLE ut_db_sync\n"
                //                        + ".ut_table_sync\n"
                //                        + "(`c0` Nullable(Int64),`c1` Nullable(Int32),`c7` Nullable(Int32),`c10` Date,`c15` Nullable(Int64),`c16` Nullable(Int32)) \n"
                //                        + "ENGINE = MergeTree() \n"
                //                        + "PARTITION BY `c10` ORDER BY tuple()";
                //                Assertions.assertTrue(clickHouse.apply(createTableSql));
            }
            //            Assertions.assertEquals(secondStorageEndpoint.tableSync(getProject()).getCode(), "000");
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve("node02"))) {
                //                ShowCreateTable showCreateTable = new ShowCreateTable(TableIdentifier.table("ut_db_sync", "ut_table_sync"));
                //                String aa = clickHouse.query(showCreateTable.toSql(), ShowCreateQueryParser.SHOW_CREATE).get(0);
                //                System.out.println(aa);
                ShowDatabases showDatabases = new ShowDatabases();
                List<String> databases = clickHouse.query(showDatabases.toSql(), ShowDatabasesParser.SHOW_DATABASES);
                for (String database : databases) {
                    List<String> tables = clickHouse.query(ShowTables.createShowTables(database).toSql(),
                            ShowDatabasesParser.SHOW_DATABASES);
                    System.out.println(tables);
                }

            }
        }
    }

    @Test
    public void testLockOperateWithHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2);

            val lockOperateRequest6 = new ProjectLockOperateRequest();
            lockOperateRequest6.setProject(getProject());
            lockOperateRequest6.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
            lockOperateRequest6.setOperateType(LockOperateTypeEnum.LOCK.name());
            EnvelopeResponse envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest6);
            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()), getProject());

            val lockOperateRequest7 = new ProjectLockOperateRequest();
            lockOperateRequest7.setProject(getProject());
            lockOperateRequest7.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name()));
            lockOperateRequest7.setOperateType(LockOperateTypeEnum.UNLOCK.name());
            envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest7);
            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Arrays.asList(LockTypeEnum.QUERY.name()), getProject());
        }
    }

    @Test
    public void testReblanceWithHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2);

            val lockOperateRequest6 = new ProjectLockOperateRequest();
            lockOperateRequest6.setProject(getProject());
            lockOperateRequest6.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
            lockOperateRequest6.setOperateType(LockOperateTypeEnum.LOCK.name());
            EnvelopeResponse envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest6);
            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()), getProject());

            val lockOperateRequest7 = new ProjectLockOperateRequest();
            lockOperateRequest7.setProject(getProject());
            lockOperateRequest7.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name()));
            lockOperateRequest7.setOperateType(LockOperateTypeEnum.UNLOCK.name());
            envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest7);
            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Arrays.asList(LockTypeEnum.QUERY.name()), getProject());

            secondStorageService.sizeInNode(getProject());

        }
    }

    @Test
    public void testLockOperate() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShard", false, 1, null, clickhouse1);

            val lockOperateRequest1 = new ProjectLockOperateRequest();

            lockOperateRequest1.setProject(getProject());
            lockOperateRequest1.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
            lockOperateRequest1.setOperateType(LockOperateTypeEnum.LOCK.name());

            EnvelopeResponse<Void> envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest1);

            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()), getProject());

            val lockOperateRequest2 = new ProjectLockOperateRequest();

            lockOperateRequest2.setProject(getProject());
            lockOperateRequest2.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
            lockOperateRequest2.setOperateType(LockOperateTypeEnum.LOCK.name());

            Assert.assertThrows(KylinException.class, () -> secondStorageEndpoint.lockOperate(lockOperateRequest2));

            val lockOperateRequest3 = new ProjectLockOperateRequest();

            lockOperateRequest3.setProject(getProject());
            lockOperateRequest3.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
            lockOperateRequest3.setOperateType(LockOperateTypeEnum.UNLOCK.name());

            envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest3);

            Assertions.assertEquals(envelopeResponse.getCode(), "000");

            val lockOperateRequest4 = new ProjectLockOperateRequest();

            lockOperateRequest4.setProject(getProject());
            lockOperateRequest4.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
            lockOperateRequest4.setOperateType(LockOperateTypeEnum.LOCK.name());

            envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest4);
            ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                    Collections.singletonList(LockTypeEnum.ALL.name()), getProject());

            val lockOperateRequest5 = new ProjectLockOperateRequest();

            lockOperateRequest5.setProject(getProject());
            lockOperateRequest5.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
            lockOperateRequest5.setOperateType(LockOperateTypeEnum.LOCK.name());

            Assert.assertThrows(KylinException.class, () -> secondStorageEndpoint.lockOperate(lockOperateRequest5));

            val lockOperateRequest6 = new ProjectLockOperateRequest();

            lockOperateRequest6.setProject(getProject());
            lockOperateRequest6.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
            lockOperateRequest6.setOperateType(LockOperateTypeEnum.UNLOCK.name());

            envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest6);

            Assertions.assertEquals(envelopeResponse.getCode(), "000");
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

    @Test
    public void testRecoverProject() throws Exception {
        secondStorageEndpoint.setModelService(modelService);

        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testRecoverProject", false, clickhouse);
            val request = new RecoverRequest();
            request.setProject(getProject());
            val response = secondStorageEndpoint.recoverProject(request);
            Assert.assertEquals(1, response.getData().getSubmittedModels().size());
            Assert.assertEquals(0, response.getData().getFailedModels().size());
        }
    }

    @Test
    public void testCheckUtil() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse }, 1, "testCheckUtil", () -> {
                ClickHouseSanityCheckTool.execute(new String[] { "1" });
                return null;
            });
        }
    }

    @Test
    public void testHAJobPaused() throws Exception {
        final String modelId = cubeName;
        final String project = getProject(); //"table_index";

        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 }, 2, "testHAJobPaused", () -> {
                new IndexDataConstructor(project).buildDataflow(modelId);
                val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val dataflow = dataflowManager.getDataflow(modelId);
                val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList());

                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                val jobId = triggerClickHouseLoadJob(project, modelId, userName, segs);

                await().atMost(30, TimeUnit.SECONDS).until(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
                });

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    executableManager.pauseJob(jobId);
                    return null;
                }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);

                waitJobEnd(project, jobId);

                NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
                await().atMost(30, TimeUnit.SECONDS)
                        .until(() -> scheduler.getContext().getRunningJobs().values().size() == 0);

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    executableManager.resumeJob(jobId);
                    return null;
                }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
                await().atMost(30, TimeUnit.SECONDS).until(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
                });
                waitJobFinish(project, jobId);

                List<Integer> rowsList = getHAModelRowCount(project, modelId);

                for (Integer rows : rowsList) {
                    Assert.assertEquals(10000, rows.intValue());
                }
                return null;
            });
        }
    }

    private JobParam triggerSegmentClean() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfManager = NDataflowManager.getInstance(config, getProject());
        val df = dfManager.getDataflow(cubeName);
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler segmentCleanJobHandler = new SecondStorageSegmentCleanJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.segmentCleanParam(getProject(), cubeName, userName,
                segments.stream().map(NDataSegment::getId).collect(Collectors.toSet()));
        segmentCleanJobHandler.handle(jobParam);
        waitJobFinish(jobParam.getJobId());
        return jobParam;
    }

    private void triggerSegmentClean(List<String> segments, String modelId, boolean isAllowFailed) {
        val request = new StorageRequest();
        request.setProject(getProject());
        request.setModel(modelId);
        request.setSegmentIds(segments);
        secondStorageEndpoint.cleanStorage(request, segments);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobFinish(job.get().getId(), isAllowFailed);
    }

    protected void buildIncrementalLoadQuery() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = cubeName;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);

        val timeRange1 = new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-01-02");
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        indexDataConstructor.buildIndex(dfName, timeRange1, indexes, true);
        val timeRange2 = new SegmentRange.TimePartitionedSegmentRange("2012-01-02", "2012-01-03");
        indexDataConstructor.buildIndex(dfName, timeRange2, indexes, true);
    }

    protected void buildFullLoadQuery() throws Exception {
        fullBuild(cubeName);
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

    private void waitJobFinish(String jobId, boolean isAllowFailed) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, getProject());
        DefaultChainedExecutable job = (DefaultChainedExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        if (!isAllowFailed) {
            val firstErrorMsg = IndexDataConstructor.firstFailedJobErrorMessage(executableManager, job);
            Assert.assertEquals(firstErrorMsg, ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
        }
    }

    private void waitJobFinish(String jobId) {
        waitJobFinish(jobId, false);
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

    protected void build_load_query(String catalog, boolean incremental, JdbcDatabaseContainer<?>... clickhouse)
            throws Exception {
        build_load_query(catalog, incremental, 1, null, clickhouse);
    }

    private JobParam triggerClickHouseJob(NDataflow df, KylinConfig config) {
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), cubeName, userName,
                segments.stream().map(NDataSegment::getId));
        String jobId = simulateJobMangerAddJob(jobParam, localHandler);
        waitJobFinish(jobId);
        return jobParam;
    }

    private void changeProjectSecondStorageState(String catalog, boolean incremental, int replica,
            JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            return true;
        });
    }

    protected void build_load_query(String catalog, boolean incremental, int replica, Callable<Void> beforeQuery, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        build_load_query(catalog, incremental, true, replica, beforeQuery, null, clickhouse);
    }

    protected void build_load_query(String catalog, boolean incremental, boolean isMergeSegment, int replica, Callable<Void> beforeQuery, Callable<Void> checkQuery, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), cubeName, true);
            // build table index
            if (incremental) {
                buildIncrementalLoadQuery();
            } else {
                buildFullLoadQuery();
            }

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            if ("table_index_incremental".equals(getProject())) {
                Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            } else if ("table_index".equals(getProject())) {
                Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            }

            // check http server
            checkHttpServer();

            //load into clickhouse
            //load into clickhouse
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);
            triggerClickHouseJob(df, config);

            // test refresh segment
            val needRefresh = dsMgr.getDataflow(cubeName).getSegments().get(0);
            refreshSegment(needRefresh.getId());

            // test merge segment
            if (incremental && isMergeSegment) {
                mergeSegments(dsMgr.getDataflow(cubeName).getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList()));
            }

            // check TableFlow
            TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(plan);
            TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(flow);

            Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                    .filter(SecondStorageUtil::isBaseTableIndex).collect(Collectors.toSet());
            Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());
            for (LayoutEntity layoutEntity : allLayouts) {
                TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(tableEntity);
                TableData data = flow.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(data);
                Assert.assertEquals(incremental ? PartitionType.INCREMENTAL : PartitionType.FULL,
                        data.getPartitionType());
                Assert.assertEquals(dsMgr.getDataflow(cubeName).getQueryableSegments().size(),
                        data.getPartitions().size() / replica);
                TablePartition partition = data.getPartitions().get(0);
                int shards = Math.min(clickhouse.length / replica, tableEntity.getShardNumbers());
                Assert.assertEquals(shards, partition.getShardNodes().size());
                Assert.assertEquals(shards, partition.getSizeInNode().size());
                Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
            }

            // check
            overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", clickhouse[0].getJdbcUrl());
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver",
                    clickhouse[0].getDriverClassName());
            // check ClickHouse
            if (beforeQuery != null) beforeQuery.call();

            if (checkQuery != null) {
                checkQuery.call();
            } else {
                checkQueryResult(incremental, clickhouse, replica);
            }

            return true;
        });
    }

    private void checkQueryResult(boolean incremental, JdbcDatabaseContainer<?>[] clickhouse, int replica)
            throws Exception {
        Dataset<Row> dataset = ExecAndComp.queryModelWithoutCompute(getProject(),
                "select PRICE from TEST_KYLIN_FACT group by PRICE");
        Assert.assertTrue(ClickHouseUtils.findShardJDBCTable(dataset.queryExecution().optimizedPlan()));
        QueryContext.reset();
        // check Aggregate push-down
        Dataset<Row> groupPlan = ExecAndComp.queryModelWithoutCompute(getProject(),
                "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE");
        ShardJDBCScan shardJDBCScan = ClickHouseUtils.findShardScan(groupPlan.queryExecution().optimizedPlan());
        Assert.assertEquals(clickhouse.length / replica, shardJDBCScan.relation().parts().length);
        List<String> expected = ImmutableList.of(columnMapping.get("PRICE"));
        ClickHouseUtils.checkGroupBy(shardJDBCScan, expected);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        Pair<String, String> pair = null;
        if (incremental) {
            val result = SparderEnv.getSparkSession()
                    .sql("select * from TEST_KYLIN_FACT where CAL_DT >= '2012-01-01' and CAL_DT < '2012-01-03'");
            result.createOrReplaceTempView("TEST_KYLIN_FACT1");
            pair = new Pair<>("TEST_KYLIN_FACT", "TEST_KYLIN_FACT1");
        }
        query.add(Pair.newPair("query_table_index1", "select PRICE from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index2", "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index3", "select max(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index4", "select min(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index5", "select count(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(
                Pair.newPair("query_table_index6", "select count(distinct PRICE) from TEST_KYLIN_FACT group by PRICE"));

        query.add(Pair.newPair("query_table_index7", "select sum(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index8", "select max(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index9", "select min(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index10", "select count(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index11", "select count(distinct PRICE) from TEST_KYLIN_FACT"));

        query.add(Pair.newPair("query_table_index12",
                "select sum(PRICE),sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index13",
                "select max(PRICE),max(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index14",
                "select min(PRICE),min(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index15",
                "select count(PRICE),count(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index16",
                "select count(distinct PRICE),count(distinct ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index17",
                "select min(PRICE) from TEST_KYLIN_FACT where ORDER_ID=2 group by PRICE "));

        query.add(Pair.newPair("query_agg_index1", "select sum(ORDER_ID) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_agg_index2",
                "select sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));

        query.add(Pair.newPair("query_agg_inner_col_index1",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n"
                        + "  ) from TEST_KYLIN_FACT \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));
        query.add(Pair.newPair("query_agg_inner_col_index2",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n" + "  ) \n"
                        + "from \n" + "  (\n" + "    select \n" + "      a1.ORDER_ID - 10 as ORDER_ID, \n"
                        + "      a1.LSTG_FORMAT_NAME\n" + "    from \n" + "      TEST_KYLIN_FACT a1\n" + "  ) \n"
                        + "where \n" + "  order_id > 10 \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left", null, pair);
    }

    public List<Integer> getHAModelRowCount(String project, String modelId) throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, 20000000001L);
        return SecondStorageNodeHelper.getAllNames().stream().map(SecondStorageNodeHelper::resolve).map(url -> {
            try (ClickHouse clickHouse = new ClickHouse(url)) {
                List<Integer> count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`",
                        rs -> {
                            try {
                                return rs.getInt(1);
                            } catch (SQLException e) {
                                return ExceptionUtils.rethrow(e);
                            }
                        });
                Assert.assertFalse(count.isEmpty());
                return count.get(0);
            } catch (Exception e) {
                return ExceptionUtils.rethrow(e);
            }
        }).collect(Collectors.toList());
    }

    public List<Integer> getHAModelRowCount(String project, String modelId, long layoutId,
            JdbcDatabaseContainer<?>[] clickhouse) throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, layoutId);

        return Arrays.stream(clickhouse).map(JdbcDatabaseContainer::getJdbcUrl).map(url -> {
            try (ClickHouse clickHouse = new ClickHouse(url)) {
                List<Integer> count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`",
                        rs -> {
                            try {
                                return rs.getInt(1);
                            } catch (SQLException e) {
                                return ExceptionUtils.rethrow(e);
                            }
                        });
                Assert.assertFalse(count.isEmpty());
                return count.get(0);
            } catch (Exception e) {
                return ExceptionUtils.rethrow(e);
            }
        }).collect(Collectors.toList());
    }
}
