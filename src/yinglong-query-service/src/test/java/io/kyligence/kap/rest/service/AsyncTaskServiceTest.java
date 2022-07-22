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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDaoTest;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ServiceTestBase.SpringConfig.class)
@WebAppConfiguration(value = "src/main/resources")
@TestPropertySource(properties = {"spring.cloud.nacos.discovery.enabled = false"})
@TestPropertySource(properties = {"spring.session.store-type = NONE"})
@ActiveProfiles({ "testing", "test" })
public class AsyncTaskServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @Autowired
    private QueryHistoryService queryHistoryService;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        RDBMSQueryHistoryDAO.getInstance().deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testDownloadQueryHistoriesTimeout() throws Exception {
        // prepare query history to RDBMS
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.deleteAllQueryHistory();
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311572000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311632000L, 1L, true, PROJECT, true));
        queryHistoryDAO.insert(RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311692000L, 1L, true, PROJECT, false));
        QueryMetrics queryMetrics = RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311752000L, 1L, true, PROJECT, false);
        queryMetrics.setSql("select * from PRT LIMIT 500");
        queryMetrics.setQueryStatus("FAILED");
        queryMetrics.getQueryHistoryInfo().setQueryMsg("From line 1, column 15 to line 1, column 17: Object 'PRT' not found while executing SQL: " + "\"select * from PRT LIMIT 500\"");
        queryHistoryDAO.insert(queryMetrics);

        // prepare request and response
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0]);
                TimeUnit.SECONDS.sleep(1);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class));

        queryHistoryService.downloadQueryHistories(request, response, ZoneOffset.ofHours(8), 8, false);
        assertEquals("\uFEFFStart Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n"
                        + "2020-01-29 23:29:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select * from PRT LIMIT 500\",CONSTANTS,FAILED,,ADMIN,\"From line 1, column 15 to line 1, column 17: Object 'PRT' not found while executing SQL: \"\"select * from PRT LIMIT 500\"\"\"\n"
                        + "2020-01-29 23:28:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",CONSTANTS,SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:27:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n"
                        + "2020-01-29 23:26:12 GMT+8,1ms,6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1,\"select LSTG_FORMAT_NAME from KYLIN_SALES LIMIT 500\",\"[Deleted Model,Deleted Model]\",SUCCEEDED,,ADMIN,\n",
                baos.toString(StandardCharsets.UTF_8.name()));
        
        overwriteSystemProp("kylin.query.query-history-download-timeout-seconds", "3s");
        try {
            queryHistoryService.downloadQueryHistories(request, response, ZoneOffset.ofHours(8), 8, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TimeoutException);
        }
    }
}