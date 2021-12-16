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
package io.kyligence.kap.streaming.util;

import com.google.common.collect.Lists;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RestSupportTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static boolean assertMeet = false;
    static CountDownLatch latch;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRestSupport() {
        val config = getTestConfig();
        val rest = new RestSupport(config);
        val baseUrl = ReflectionUtils.getField(rest, "baseUrl");
        Assert.assertEquals("http://127.0.0.1:7070/kylin/api", baseUrl);
    }

    @Test
    public void testHttpPost() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPost("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testHttpPut() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPut("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testHttpGet() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new NormalModeHandler(request));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpGet("status"), null);
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("false", restResp.getData());
                    Assert.assertEquals("", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertFalse(maintenanceMode);

                    val checkMaintenanceMode = rest.checkMaintenceMode();
                    Assert.assertFalse(checkMaintenanceMode);
                    rest.close();
                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testMaintenanceMode() {
        List<Integer> ports = Lists.newArrayList(50000, 51000, 52000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    val request = new StreamingJobUpdateRequest();
                    server.createContext("/test", new MaintenanceModeHandler(request, 10));
                    server.start();

                    val rest = new RestSupport("http://localhost:" + port + "/test/");
                    val restResp = rest.execute(rest.createHttpPost("status"), new StreamingJobUpdateRequest());
                    Assert.assertNotNull(restResp);
                    Assert.assertEquals("0", restResp.getCode());
                    Assert.assertEquals("true", restResp.getData());
                    Assert.assertEquals("maintenance", restResp.getMsg());

                    val maintenanceMode = rest.isMaintenanceMode();
                    Assert.assertTrue(maintenanceMode);
                    rest.close();

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testCheckMaintenanceMode() {
        val rest = Mockito.mock(RestSupport.class);
        Mockito.when(rest.isMaintenanceMode()).thenReturn(false);
        Assert.assertFalse(rest.checkMaintenceMode());
    }

    @Test
    public void testIsMaintenanceMode() {
        val rest = Mockito.mock(RestSupport.class);
        Mockito.when(rest.isMaintenanceMode()).thenReturn(false);
        Assert.assertFalse(rest.isMaintenanceMode());
    }

    static class NormalModeHandler implements HttpHandler {
        Object req;

        public NormalModeHandler(Object req) {
            this.req = req;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(req))) {
                    assertMeet = true;
                }

            } finally {

                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                httpExchange.getResponseBody()
                        .write(JsonUtil.writeValueAsString(new RestResponse<String>("0", "false", "")).getBytes());
                httpExchange.close();
                latch.countDown();
            }
        }
    }

    static class MaintenanceModeHandler implements HttpHandler {
        Object req;
        int mtmCnt;

        public MaintenanceModeHandler(Object req, int mtmCnt) {
            this.req = req;
            this.mtmCnt = mtmCnt;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(req))) {
                    assertMeet = true;
                }
            } finally {
                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                if (mtmCnt > 0) {
                    httpExchange.getResponseBody().write(JsonUtil
                            .writeValueAsString(new RestResponse<String>("0", "true", "maintenance")).getBytes());
                    mtmCnt--;
                } else {
                    httpExchange.getResponseBody().write(JsonUtil
                            .writeValueAsString(new RestResponse<String>("0", "false", "maintenance")).getBytes());
                }
                httpExchange.close();
                latch.countDown();
            }
        }
    }
}
