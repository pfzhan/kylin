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

package org.apache.kylin.rest.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.service.KapQueryService;
import lombok.extern.slf4j.Slf4j;

@Ignore
@Slf4j
public class FavoriteQueryTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    private List<QueryHistory> queryHistories = new ArrayList<>();
    private CountDownLatch countDown;

    @InjectMocks
    private KapQueryService queryService = Mockito.spy(new KapQueryService());
    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(queryService, "aclEvaluate", aclEvaluate);
        System.setProperty("kylin.query.cache-enabled", "false");

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        AppConfig appConfig = Mockito.spy(new AppConfig());
        ClusterManager clusterManager = new DefaultClusterManager(7070);

        ReflectionTestUtils.setField(queryService, "clusterManager", clusterManager);

        Mockito.when(appConfig.getPort()).thenReturn(7070);
        ReflectionTestUtils.setField(queryService, "appConfig", appConfig);
    }

    @After
    public void destroy() {
        System.clearProperty("kylin.query.cache-enabled");
    }

    private void mockQueryException(QueryService queryService, String sql, Throwable throwable) throws Exception {
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        Mockito.when(queryExec.executeQuery(sql)).thenThrow(throwable);
        Mockito.when(queryService.newQueryExec(PROJECT)).thenReturn(queryExec);
    }

    @Test
    public void testSyntaxErrorQuery() throws Exception {
        countDown = new CountDownLatch(1);
        SQLException exception = new SQLException(
                new SqlValidatorException("sql syntax error", new RuntimeException()));
        String sql1 = "select * from test_kylin_fact";
        mockQueryException(queryService, sql1, exception);
        SQLRequest request = new SQLRequest();
        request.setProject(PROJECT);
        request.setSql(sql1);
        queryService.doQueryWithCache(request, false);

        countDown.await(1000, TimeUnit.MILLISECONDS);

        countDown = new CountDownLatch(1);
        exception = new SQLException(new NoRealizationFoundException("time out", new RuntimeException()));
        String sql2 = "select * from test_account";
        mockQueryException(queryService, sql2, exception);
        request = new SQLRequest();
        request.setProject(PROJECT);
        request.setSql("select * from test_account");
        Mockito.when(queryService.tryPushDownSelectQuery(request, null, exception, false))
                .thenThrow(new KylinTimeoutException("time out"));
        queryService.doQueryWithCache(request, false);

        countDown.await(1000, TimeUnit.MILLISECONDS);
    }

}
