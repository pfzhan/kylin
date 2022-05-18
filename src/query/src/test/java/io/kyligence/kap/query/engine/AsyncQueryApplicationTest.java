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

package io.kyligence.kap.query.engine;

import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.QueryParams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.query.QueryMetricsContext;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;

@RunWith(MockitoJUnitRunner.class)
public class AsyncQueryApplicationTest {

    private AsyncQueryApplication asyncQueryApplication;

    @Before
    public void setUp() throws Exception {
        asyncQueryApplication = spy(new AsyncQueryApplication());
    }

    @Test
    public void testDoExecute() throws Exception {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class);
                MockedStatic<RDBMSQueryHistoryDAO> rdbmsQueryHistoryDAOMockedStatic = mockStatic(
                        RDBMSQueryHistoryDAO.class);
                MockedStatic<QueryMetricsContext> queryMetricsContextMockedStatic = mockStatic(
                        QueryMetricsContext.class);
                MockedConstruction<QueryRoutingEngine> queryRoutingEngineMockedConstruction = mockConstruction(
                        QueryRoutingEngine.class)) {
            doReturn("{\"queryId\": \"query_uuid1\"}").when(asyncQueryApplication).getParam(P_QUERY_CONTEXT);
            doReturn("{}").when(asyncQueryApplication).getParam(P_QUERY_PARAMS);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(mock(KylinConfig.class));
            rdbmsQueryHistoryDAOMockedStatic.when(RDBMSQueryHistoryDAO::getInstance)
                    .thenReturn(mock(RDBMSQueryHistoryDAO.class));
            queryMetricsContextMockedStatic.when(() -> QueryMetricsContext.collect(any()))
                    .thenReturn(mock(QueryMetricsContext.class));

            asyncQueryApplication.doExecute();
        }
    }

    @Test
    public void testDoExecuteWithException() throws Exception {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class);
             MockedStatic<RDBMSQueryHistoryDAO> rdbmsQueryHistoryDAOMockedStatic = mockStatic(
                     RDBMSQueryHistoryDAO.class);
             MockedStatic<QueryMetricsContext> queryMetricsContextMockedStatic = mockStatic(
                     QueryMetricsContext.class);
             MockedConstruction<QueryRoutingEngine> queryRoutingEngineMockedConstruction = mockConstruction(
                     QueryRoutingEngine.class)) {
            doReturn("{\"queryId\": \"query_uuid1\"}").when(asyncQueryApplication).getParam(P_QUERY_CONTEXT);
            doReturn("xxx").when(asyncQueryApplication).getParam(P_QUERY_PARAMS);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(mock(KylinConfig.class));
            rdbmsQueryHistoryDAOMockedStatic.when(RDBMSQueryHistoryDAO::getInstance)
                    .thenReturn(mock(RDBMSQueryHistoryDAO.class));
            queryMetricsContextMockedStatic.when(() -> QueryMetricsContext.collect(any()))
                    .thenReturn(mock(QueryMetricsContext.class));

            asyncQueryApplication.doExecute();
        }
    }

    @Test
    public void testConstructQueryHistorySqlText() {
        try (MockedStatic<QueryContext> queryContextMockedStatic = mockStatic(QueryContext.class)) {
            QueryContext.Metrics metrics = mock(QueryContext.Metrics.class);
            queryContextMockedStatic.when(QueryContext::currentMetrics).thenReturn(metrics);
            when(metrics.getCorrectedSql()).thenReturn("select col1 from table1");
            QueryParams queryParams = mock(QueryParams.class);
            when(queryParams.isPrepareStatementWithParams()).thenReturn(true);
            PrepareSqlStateParam prepareSqlStateParam = new PrepareSqlStateParam("java.lang.Integer", "1001");
            when(queryParams.getParams()).thenReturn(new PrepareSqlStateParam[] { prepareSqlStateParam });

            String result = (String) ReflectionTestUtils.invokeMethod(asyncQueryApplication,
                    "constructQueryHistorySqlText", queryParams, "-- comment\nselect col1 from table1");
            assertEquals(
                    "{\"sql\":\"-- comment\\nselect col1 from table1\",\"normalized_sql\":\"select col1 from table1\",\"params\":[{\"pos\":1,\"java_type\":\"java.lang.Integer\",\"data_type\":\"INTEGER\",\"value\":\"1001\"}]}",
                    result);
        }
    }
}
