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
package org.apache.kylin.rest.util;

import static org.apache.spark.deploy.history.HistoryServerBuilder.createHistoryServer;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.history.HistoryServer;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.util.SparkHistoryUIUtil;

public class SparkHistoryUIUtilTest extends NLocalFileMetadataTestCase {
    private MockHttpServletRequest request = new MockHttpServletRequest() {
    };
    private MockHttpServletResponse response = new MockHttpServletResponse();
    private HistoryServer historyServer;
    private SparkHistoryUIUtil sparkHistoryUIUtil;

    @Test
    public void testProxy() throws Exception {
        createTestMetadata();
        FileUtils.forceMkdir(new File("/tmp/spark-events"));
        historyServer = createHistoryServer(new SparkConf());
        sparkHistoryUIUtil = new SparkHistoryUIUtil();
        Field field = sparkHistoryUIUtil.getClass().getDeclaredField("historyServer");
        field.setAccessible(true);
        field.set(sparkHistoryUIUtil, historyServer);
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE);
        request.setMethod("GET");
        sparkHistoryUIUtil.proxy(request, response);
        response.getContentType();
        assert response.getContentAsString().contains("href=\"" + SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE + "/");
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE + "/history/app001");
        sparkHistoryUIUtil.proxy(request, response);
        assert SparkHistoryUIUtil.getHistoryTrackerUrl("app-0001")
                .equals(SparkHistoryUIUtil.PROXY_LOCATION_BASE + "/history/app-0001");

    }

    @Test
    public void test3xxProxy() throws Exception {
        HttpComponentsClientHttpRequestFactory mockFactory = Mockito.mock(HttpComponentsClientHttpRequestFactory.class);
        Field field = SparkUIUtil.class.getDeclaredField("factory");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, mockFactory);
        ClientHttpResponse mockClientHttpResponse = Mockito.mock(ClientHttpResponse.class);
        Mockito.when(mockClientHttpResponse.getStatusCode()).thenReturn(HttpStatus.FOUND);
        Mockito.when(mockClientHttpResponse.getHeaders()).thenReturn(new HttpHeaders());

        ClientHttpRequest mockRequest = Mockito.mock(ClientHttpRequest.class);
        Mockito.when(mockRequest.getHeaders()).thenReturn(new HttpHeaders());
        Mockito.when(mockRequest.execute()).thenReturn(mockClientHttpResponse);
        Mockito.when(mockFactory.createRequest(Mockito.any(), Mockito.any())).thenReturn(mockRequest);
        request.setRequestURI(SparkHistoryUIUtil.KYLIN_HISTORY_UI_BASE);
        request.setMethod("GET");
        response.setWriterAccessAllowed(true);
        SparkUIUtil.resendSparkUIRequest(request, response, "http://localhost:18080", "history_server", "proxy");
        Mockito.verify(mockRequest, Mockito.times(6)).execute();

    }
}
