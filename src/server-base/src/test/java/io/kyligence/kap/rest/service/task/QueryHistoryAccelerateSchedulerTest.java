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
import io.kyligence.kap.metadata.favorite.AccelerateRuleUtil;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.query.AccelerateRatio;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.rest.service.RawRecService;

@RunWith(TimeZoneTestRunner.class)
public class QueryHistoryAccelerateSchedulerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

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

        QueryHistoryAccelerateScheduler.QueryHistoryAccelerateRunner queryHistoryAccelerateRunner = //
                qhAccelerateScheduler.new QueryHistoryAccelerateRunner();
        queryHistoryAccelerateRunner.run();

        // test update accelerate ratio
        AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        AccelerateRatio accelerateRatio = accelerateRatioManager.get();
        Assert.assertEquals(1, accelerateRatio.getNumOfQueryHitIndex());
        Assert.assertEquals(4, accelerateRatio.getOverallQueryNum());

        // test update id offset
        QueryHistoryIdOffsetManager idOffsetManager = QueryHistoryIdOffsetManager.getInstance(getTestConfig(), PROJECT);
        QueryHistoryIdOffset queryHistoryIdOffset = idOffsetManager.get();
        Assert.assertEquals(0, queryHistoryIdOffset.getQueryHistoryIdOffset());
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("select * from sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);
        queryHistory1.setEngineType("CONSTANTS");

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("select * from sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);
        queryHistory2.setEngineType("HIVE");

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("select * from sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);
        queryHistory3.setEngineType("NATIVE");

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("select * from sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);
        queryHistory4.setEngineType("HIVE");

        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4);
    }

}