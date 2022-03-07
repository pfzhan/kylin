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

package io.kyligence.kap.rest.service.task;

import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.AccelerateRuleUtil;
import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.rest.service.NUserGroupService;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(TimeZoneTestRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore("javax.management.*")
public class QueryHistoryTaskSchedulerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String DATAFLOW = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String LAYOUT1 = "20000000001";
    private static final String LAYOUT2 = "1000001";
    private static final Long QUERY_TIME = 1586760398338L;

    private QueryHistoryTaskScheduler qhAccelerateScheduler;

    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
        createTestMetadata();
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenReturn(PowerMockito.mock(PermissionFactory.class));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenReturn(PowerMockito.mock(PermissionGrantingStrategy.class));
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryTaskScheduler(PROJECT));
        ReflectionTestUtils.setField(qhAccelerateScheduler, "userGroupService", userGroupService);
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryAcc() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistories()).thenReturn(null);

        Mockito.when(qhAccelerateScheduler.accelerateRuleUtil.findMatchedCandidate(Mockito.anyString(),
                Mockito.anyList(), Mockito.anyMap(), Mockito.anyList())).thenReturn(queryHistories());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryIdOffset());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();

        // after update id offset
        Assert.assertEquals(8, idOffsetManager.get().getQueryHistoryIdOffset());

    }

    @Test
    public void testQueryAccResetOffset() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.spy(RDBMSQueryHistoryDAO.getInstance());
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(Lists.newArrayList());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        QueryHistoryIdOffset queryHistoryIdOffset = idOffsetManager.get();
        queryHistoryIdOffset.setQueryHistoryIdOffset(999L);
        queryHistoryIdOffset.setQueryHistoryStatMetaUpdateIdOffset(999L);
        idOffsetManager.save(queryHistoryIdOffset);
        Assert.assertEquals(999L, idOffsetManager.get().getQueryHistoryIdOffset());
        // run update
        QueryHistoryTaskScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();
        // after auto reset offset
        Assert.assertEquals(0L, idOffsetManager.get().getQueryHistoryIdOffset());
    }

    @Test
    public void testQueryAccNotResetOffset() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.spy(RDBMSQueryHistoryDAO.getInstance());
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(Lists.newArrayList());
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.getQueryHistoryMaxId(Mockito.anyString())).thenReturn(12L);
        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        QueryHistoryIdOffset queryHistoryIdOffset = idOffsetManager.get();
        queryHistoryIdOffset.setQueryHistoryIdOffset(9L);
        queryHistoryIdOffset.setQueryHistoryStatMetaUpdateIdOffset(9L);
        idOffsetManager.save(queryHistoryIdOffset);
        Assert.assertEquals(9L, idOffsetManager.get().getQueryHistoryIdOffset());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();
        // after auto reset offset
        Assert.assertEquals(9L, idOffsetManager.get().getQueryHistoryIdOffset());
    }

    @Test
    public void testUpdateMetadata() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistories()).thenReturn(null);

        // before update dataflow usage, layout usage and last query time
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(DATAFLOW);
        Assert.assertEquals(3, dataflow.getQueryHitCount());
        Assert.assertNull(dataflow.getLayoutHitCount().get(20000000001L));
        Assert.assertNull(dataflow.getLayoutHitCount().get(1000001L));
        Assert.assertEquals(0L, dataflow.getLastQueryTime());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryMetaUpdateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryMetaUpdateRunner();
        queryHistoryAccelerateRunner.run();

        // after update dataflow usage, layout usage and last query time
        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).getDataflow(DATAFLOW);
        Assert.assertEquals(6, dataflow.getQueryHitCount());
        Assert.assertEquals(2, dataflow.getLayoutHitCount().get(20000000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1, dataflow.getLayoutHitCount().get(1000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1586760398338L, dataflow.getLastQueryTime());

        // after update id offset
        Assert.assertEquals(8, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());
    }

    @Test
    public void testUpdateMetadataWithStringRealization() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistoriesWithStringRealization())
                .thenReturn(null);

        // before update dataflow usage, layout usage and last query time
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(DATAFLOW);
        Assert.assertEquals(3, dataflow.getQueryHitCount());
        Assert.assertNull(dataflow.getLayoutHitCount().get(20000000001L));
        Assert.assertNull(dataflow.getLayoutHitCount().get(1000001L));
        Assert.assertEquals(0L, dataflow.getLastQueryTime());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryMetaUpdateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryMetaUpdateRunner();
        queryHistoryAccelerateRunner.run();

        // after update dataflow usage, layout usage and last query time
        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).getDataflow(DATAFLOW);
        Assert.assertEquals(6, dataflow.getQueryHitCount());
        Assert.assertEquals(2, dataflow.getLayoutHitCount().get(20000000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1, dataflow.getLayoutHitCount().get(1000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1586760398338L, dataflow.getLastQueryTime());

        // after update id offset
        Assert.assertEquals(8, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());
    }

    @Test
    public void testNotUpdateMetadataForUserTriggered() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistories()).thenReturn(null);
        Mockito.when(qhAccelerateScheduler.accelerateRuleUtil.findMatchedCandidate(Mockito.anyString(),
                Mockito.anyList(), Mockito.anyMap(), Mockito.anyList())).thenReturn(queryHistories());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryIdOffset());

        // update sync-acceleration-task to running, then check auto-run will fail
        AsyncTaskManager manager = AsyncTaskManager.getInstance(getTestConfig(), PROJECT);
        AsyncAccelerationTask asyncTask = (AsyncAccelerationTask) manager.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        asyncTask.setAlreadyRunning(true);
        manager.save(asyncTask);
        AsyncAccelerationTask update = (AsyncAccelerationTask) manager.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Assert.assertTrue(update.isAlreadyRunning());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false);
        queryHistoryAccelerateRunner.run();

        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryIdOffset());
    }

    @Test
    public void testBatchUpdate() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.queryQueryHistoriesByIdOffset(Mockito.anyLong(),
                Mockito.anyInt(), Mockito.anyString())).thenReturn(queryHistories()).thenReturn(queryHistories());

        // run update
        QueryHistoryTaskScheduler.QueryHistoryMetaUpdateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryMetaUpdateRunner();
        queryHistoryAccelerateRunner.run();

        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(8, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());

        queryHistoryAccelerateRunner.run();
        Assert.assertEquals(16, idOffsetManager.get().getQueryHistoryStatMetaUpdateIdOffset());
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("select * from sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);
        queryHistory1.setEngineType("CONSTANTS");
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("select * from sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);
        queryHistory2.setEngineType("HIVE");
        queryHistory2.setId(2);

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("select * from sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);
        queryHistory3.setEngineType("NATIVE");
        queryHistory3.setId(3);

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("select * from sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);
        queryHistory4.setEngineType("HIVE");
        queryHistory4.setId(4);

        QueryHistory queryHistory5 = new QueryHistory();
        queryHistory5.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory5.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory5.setDuration(1000L);
        queryHistory5.setQueryTime(QUERY_TIME);
        queryHistory5.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo5 = new QueryHistoryInfo();
        queryHistoryInfo5.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT1, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory5.setQueryHistoryInfo(queryHistoryInfo5);
        queryHistory5.setId(5);

        QueryHistory queryHistory6 = new QueryHistory();
        queryHistory6.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory6.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory6.setDuration(1000L);
        queryHistory6.setQueryTime(QUERY_TIME);
        queryHistory6.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo6 = new QueryHistoryInfo();
        queryHistoryInfo6.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT1, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory6.setQueryHistoryInfo(queryHistoryInfo6);
        queryHistory6.setId(6);

        QueryHistory queryHistory7 = new QueryHistory();
        queryHistory7.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory7.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory7.setDuration(1000L);
        queryHistory7.setQueryTime(QUERY_TIME);
        queryHistory7.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo7 = new QueryHistoryInfo();
        queryHistoryInfo7.setRealizationMetrics(Lists.newArrayList(
                new QueryMetrics.RealizationMetrics(LAYOUT2, "Table Index", DATAFLOW, Lists.newArrayList())));
        queryHistory7.setQueryHistoryInfo(queryHistoryInfo7);
        queryHistory7.setId(7);

        QueryHistory queryHistory8 = new QueryHistory();
        queryHistory8.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory8.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory8.setDuration(1000L);
        queryHistory8.setQueryTime(QUERY_TIME);
        queryHistory8.setEngineType("NATIVE");
        QueryHistoryInfo queryHistoryInfo8 = new QueryHistoryInfo();
        queryHistoryInfo8.setRealizationMetrics(
                Lists.newArrayList(new QueryMetrics.RealizationMetrics(null, null, DATAFLOW, Lists.newArrayList())));
        queryHistory8.setQueryHistoryInfo(queryHistoryInfo8);
        queryHistory8.setId(8);

        List<QueryHistory> histories = Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4,
                queryHistory5, queryHistory6, queryHistory7, queryHistory8);

        for (QueryHistory history : histories) {
            history.setId(startOffset + history.getId());
        }
        startOffset += histories.size();

        return histories;
    }

    private List<QueryHistory> queryHistoriesWithStringRealization() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("select * from sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);
        queryHistory1.setEngineType("CONSTANTS");
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("select * from sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);
        queryHistory2.setEngineType("HIVE");
        queryHistory2.setId(2);

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("select * from sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);
        queryHistory3.setEngineType("NATIVE");
        queryHistory3.setId(3);

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("select * from sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);
        queryHistory4.setEngineType("HIVE");
        queryHistory4.setId(4);

        QueryHistory queryHistory5 = new QueryHistory();
        queryHistory5.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory5.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory5.setDuration(1000L);
        queryHistory5.setQueryTime(QUERY_TIME);
        queryHistory5.setEngineType("NATIVE");
        queryHistory5.setQueryRealizations(DATAFLOW + "#" + LAYOUT1 + "#Table Index#[]");
        queryHistory5.setId(5);

        QueryHistory queryHistory6 = new QueryHistory();
        queryHistory6.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory6.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory6.setDuration(1000L);
        queryHistory6.setQueryTime(QUERY_TIME);
        queryHistory6.setEngineType("NATIVE");
        queryHistory6.setQueryRealizations(DATAFLOW + "#" + LAYOUT1 + "#Table Index#[]");
        queryHistory6.setId(6);

        QueryHistory queryHistory7 = new QueryHistory();
        queryHistory7.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory7.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory7.setDuration(1000L);
        queryHistory7.setQueryTime(QUERY_TIME);
        queryHistory7.setEngineType("NATIVE");
        queryHistory7.setQueryRealizations(DATAFLOW + "#" + LAYOUT2 + "#Agg Index");
        queryHistory7.setId(7);

        QueryHistory queryHistory8 = new QueryHistory();
        queryHistory8.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory8.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory8.setDuration(1000L);
        queryHistory8.setQueryTime(QUERY_TIME);
        queryHistory8.setEngineType("NATIVE");
        queryHistory8.setQueryRealizations(DATAFLOW + "#null" + "#null");
        queryHistory8.setId(8);

        List<QueryHistory> histories = Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4,
                queryHistory5, queryHistory6, queryHistory7, queryHistory8);

        for (QueryHistory history : histories) {
            history.setId(startOffset + history.getId());
        }
        startOffset += histories.size();

        return histories;
    }

    int startOffset = 0;

}