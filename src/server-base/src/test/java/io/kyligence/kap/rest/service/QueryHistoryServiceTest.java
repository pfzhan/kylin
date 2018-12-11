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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import org.apache.kylin.rest.msg.MsgPicker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

public class QueryHistoryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @BeforeClass
    public static void setUpBeforeClass() {
        staticCreateTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetFilteredQueryHistories() throws InvocationTargetException, IllegalAccessException {
        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom(0);
        request.setStartTimeTo(Long.MAX_VALUE);
        request.setLatencyFrom(0);
        request.setLatencyTo(Integer.MAX_VALUE);

        // mock query history
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select * from test_table_1");
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select * from test_table_2");

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory1, queryHistory2)).when(queryHistoryDAO).getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao(PROJECT);

        HashMap<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        int size = (int) result.get("size");

        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals(queryHistory1.getSql(), queryHistories.get(0).getSql());
        Assert.assertEquals(queryHistory2.getSql(), queryHistories.get(1).getSql());
        Assert.assertEquals(10, size);
    }

    @Test
    @Ignore("do not throw exception")
    public void testCheckMetricTypeError() {
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "");

        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);

        try {
            queryHistoryService.getQueryHistories(request, 10, 0);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalStateException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getNOT_SET_INFLUXDB(), ex.getMessage());
        }
    }
}
