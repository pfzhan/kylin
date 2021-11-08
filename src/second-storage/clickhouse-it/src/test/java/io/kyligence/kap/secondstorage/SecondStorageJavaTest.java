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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import io.kyligence.kap.clickhouse.database.ClickHouseOperator;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.job.Engine;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import static org.apache.kylin.common.exception.JobErrorCode.SECOND_STORAGE_JOB_EXISTS;
import org.apache.kylin.common.exception.KylinException;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_NODE_NOT_AVAILABLE;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Collections;
import java.util.stream.Collectors;

public class SecondStorageJavaTest implements JobWaiter {
    private static final String modelName = "test_table_index";
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );

    public EnableTestUser enableTestUser = new EnableTestUser();
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);

    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1,
            project, Collections.singletonList(modelId), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();
    private AclEvaluate aclEvaluate = Mockito.mock(AclEvaluate.class);

    private final SparkSession sparkSession = sharedSpark.getSpark();

    @Before
    public void setUp() {
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageService.setAclEvaluate(aclEvaluate);
    }

    @Test
    public void testModelUpdate() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        secondStorageService.onUpdate(project, modelId);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(manager.getAllExecutables().stream().anyMatch(ClickHouseModelCleanJob.class::isInstance));
        secondStorageService.disableModelSecondStorage(project, modelId);
        int jobNum = manager.getAllExecutables().size();
        secondStorageService.onUpdate(project, modelId);
        Assert.assertEquals(jobNum, manager.getAllExecutables().size());
    }

    @Test
    public void testCleanSegment() throws Exception {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(tableFlowManager.isPresent());
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        int segmentNum = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(1, segmentNum);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val segmentResponse = new NDataSegmentResponse(dataflow, dataflow.getFirstSegment());
        Assert.assertTrue(segmentResponse.isHasBaseTableIndexData());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        request.setSegmentIds(segs);
        secondStorageEndpoint.cleanStorage(request, segs);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobFinish(project, job.get().getId());
        int partitionNum = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(0, partitionNum);
    }

    @Test
    public void testDoubleTriggerSegmentLoad() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        try {
            triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        } catch (KylinException e) {
            Assert.assertEquals(FAILED_CREATE_JOB.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testRecoverModelNotEnableSecondStorage() {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName);
        val jobId = secondStorageService.disableModelSecondStorage(project, modelId);
        waitJobFinish(project, jobId);
        secondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test
    public void testRecoverModelWhenHasLoadTask() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName);
        try {
            secondStorageEndpoint.recoverModel(request);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_JOB_EXISTS.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCleanSegmentWhenHasLoadTask() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        try {
            secondStorageEndpoint.cleanStorage(request, segs);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_JOB_EXISTS.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test(expected = KylinException.class)
    public void testRecoverModelNotExist() {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName + "123");
        secondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test
    public void testModelCleanJobWithoutSegments() {
        val jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getJob(jobId);
        Assert.assertTrue(job.getDataRangeStart() < job.getDataRangeEnd());
    }

    @Test
    public void testEnableModelWithoutBaseLayout() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() ->{
            NIndexPlanManager manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.updateIndexPlan(modelId, copy -> copy.removeLayouts(Sets.newHashSet(copy.getBaseTableLayout().getId()), true, true));
            return null;
        }, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val segmentResponse = new NDataSegmentResponse(dataflow, dataflow.getFirstSegment());
        Assert.assertFalse(segmentResponse.isHasBaseTableIndexData());
        secondStorageService.onUpdate(project, modelId);

        secondStorageService.disableModelSecondStorage(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        secondStorageService.onUpdate(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, modelId));
    }

    @Test
    public void testEnableProjectNodeNotAvailable() {
        try {
            secondStorageService.changeProjectSecondStorageState("table_index_incremental", SecondStorageNodeHelper.getAllNames(), true);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_NODE_NOT_AVAILABLE.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testResetStorage() {
        Assert.assertTrue(SecondStorageUtil.isProjectEnable(project));
        secondStorageEndpoint.resetStorage();
        Assert.assertFalse(SecondStorageUtil.isProjectEnable(project));
    }

    @Test
    public void testQueryWithClickHouseSuccess() throws Exception {
        final String queryCatalog = "testQueryWithClickHouseSuccess";
        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        secondStorageEndpoint.refreshConf();
        Mockito.verify(aclEvaluate).checkIsGlobalAdmin();
        secondStorageService.sizeInNode(project);

        //build
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

        // check
        test.checkHttpServer();
        test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

        JdbcDatabaseContainer<?> clickhouse1 = clickHouseClassRule.getClickhouse(0);
        sparkSession.sessionState().conf().setConfString(
                "spark.sql.catalog." + queryCatalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        sparkSession.sessionState().conf().setConfString(
                "spark.sql.catalog." + queryCatalog + ".url",
                clickhouse1.getJdbcUrl());
        sparkSession.sessionState().conf().setConfString(
                "spark.sql.catalog." + queryCatalog + ".driver",
                clickhouse1.getDriverClassName());

        String sql = "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE";
        NExecAndComp.queryWithKapWithMeta(project, "left", Pair.newPair("query_table_index1", sql), null);
        Assert.assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
    }

    @Test
    public void testClickHouseOperator() throws Exception {
        val jdbcUrl = SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0));
        ClickHouseOperator operator = new ClickHouseOperator(SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0)));
        val databases = operator.listDatabases();
        Assert.assertEquals(2, databases.size());
        ClickHouse clickHouse = new ClickHouse(jdbcUrl);
        clickHouse.apply("CREATE TABLE test(a int) engine=Memory()");
        val tables = operator.listTables("default");
        Assert.assertEquals(1, tables.size());
        operator.dropTable("default", "test");
        val remainingTables = operator.listTables("default");
        Assert.assertEquals(0, remainingTables.size());
        operator.close();
        clickHouse.close();
    }

    @Test
    public void testSchedulerService() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val jdbcUrl = SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0));
        ClickHouse clickHouse = new ClickHouse(jdbcUrl);
        ClickHouseOperator operator = new ClickHouseOperator(SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0)));
        val render = new ClickHouseRender();
        val fakeJobId = RandomUtil.randomUUIDStr();
        val tempTable = fakeJobId + "@" + "test_temp";
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply(ClickHouseCreateTable.createCKTable(database, tempTable)
                .columns(new ColumnWithType("i1", "Int32"))
                .columns(new ColumnWithType("i2", "Nullable(Int64)"))
                .engine(Engine.DEFAULT).toSql(render));
        secondStorageScheduleService.secondStorageTempTableCleanTask();
        val tables = operator.listTables(database);
        Assert.assertFalse(tables.contains(tempTable));
    }

    @Test
    public void testCleanModelWhenTableNotExists() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val table = NameUtil.getTable(modelId, 20000000001L);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP TABLE " + database + "." + table);
        val jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        waitJobFinish(project, jobId);
    }

    @Test
    public void testCleanModelWhenDatabaseNotExists() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP DATABASE " + database);
        val jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        waitJobFinish(project, jobId);
    }
}
