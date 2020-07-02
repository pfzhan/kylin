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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.AccelerateRuleUtil;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.query.AccelerateRatio;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.rest.service.RawRecService;

@RunWith(TimeZoneTestRunner.class)
public class QueryHistoryAccelerateSchedulerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String DATAFLOW = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String LAYOUT1 = "20000000001";
    private static final String LAYOUT2 = "1000001";
    private static final Long QUERY_TIME = 1586760398338L;

    @InjectMocks
    private QueryHistoryAccelerateScheduler qhAccelerateScheduler;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryAccelerateScheduler(PROJECT));
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateMetadata() {
        qhAccelerateScheduler.queryHistoryDAO = Mockito.mock(RDBMSQueryHistoryDAO.class);
        qhAccelerateScheduler.accelerateRuleUtil = Mockito.mock(AccelerateRuleUtil.class);
        qhAccelerateScheduler.rawRecommendation = Mockito.mock(RawRecService.class);
        Mockito.when(qhAccelerateScheduler.queryHistoryDAO.getQueryHistoriesById(Mockito.anyLong(), Mockito.anyInt(),
                Mockito.anyString())).thenReturn(queryHistories()).thenReturn(null);
        Mockito.when(qhAccelerateScheduler.accelerateRuleUtil.findMatchedCandidate(Mockito.anyString(),
                Mockito.anyList(), Mockito.anyList())).thenReturn(queryHistories());

        // before update dataflow usage, layout usage and last query time
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(DATAFLOW);
        Assert.assertEquals(3, dataflow.getQueryHitCount());
        Assert.assertNull(dataflow.getLayoutHitCount().get(20000000001L));
        Assert.assertNull(dataflow.getLayoutHitCount().get(1000001L));
        Assert.assertEquals(0L, dataflow.getLastQueryTime());

        // before update accelerate ratio
        AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertNull(accelerateRatioManager.get());

        // before update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, idOffsetManager.get().getQueryHistoryIdOffset());

        // run update
        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner();
        queryHistoryAccelerateRunner.run();

        // after update dataflow usage, layout usage and last query time
        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).getDataflow(DATAFLOW);
        Assert.assertEquals(6, dataflow.getQueryHitCount());
        Assert.assertEquals(2, dataflow.getLayoutHitCount().get(20000000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1, dataflow.getLayoutHitCount().get(1000001L).getDateFrequency()
                .get(TimeUtil.getDayStart(QUERY_TIME)).intValue());
        Assert.assertEquals(1586760398338L, dataflow.getLastQueryTime());

        // after update accelerate ratio
        AccelerateRatio ratio = accelerateRatioManager.get();
        Assert.assertEquals(4, ratio.getNumOfQueryHitIndex());
        Assert.assertEquals(7, ratio.getOverallQueryNum());

        // after update id offset
        Assert.assertEquals(7, idOffsetManager.get().getQueryHistoryIdOffset());
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
        queryHistory5.setQueryRealizations(DATAFLOW + "#" + LAYOUT1 + "#Table Index");
        queryHistory5.setId(5);

        QueryHistory queryHistory6 = new QueryHistory();
        queryHistory6.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\n" + "FROM \"KYLIN_SALES\"");
        queryHistory6.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory6.setDuration(1000L);
        queryHistory6.setQueryTime(QUERY_TIME);
        queryHistory6.setEngineType("NATIVE");
        queryHistory6.setQueryRealizations(DATAFLOW + "#" + LAYOUT1 + "#Table Index");
        queryHistory6.setId(6);

        QueryHistory queryHistory7 = new QueryHistory();
        queryHistory7.setSqlPattern("SELECT count(*) FROM \"KYLIN_SALES\" group by LSTG_FORMAT_NAME");
        queryHistory7.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory7.setDuration(1000L);
        queryHistory7.setQueryTime(QUERY_TIME);
        queryHistory7.setEngineType("NATIVE");
        queryHistory7.setQueryRealizations(DATAFLOW + "#" + LAYOUT2 + "#Agg Index");
        queryHistory7.setId(7);

        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4, queryHistory5,
                queryHistory6, queryHistory7);
    }

}