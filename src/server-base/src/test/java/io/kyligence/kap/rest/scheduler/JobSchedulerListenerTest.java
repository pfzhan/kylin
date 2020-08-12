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

package io.kyligence.kap.rest.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.kyligence.kap.common.scheduler.JobFinishedNotifier;

public class JobSchedulerListenerTest {
    static CountDownLatch latch;

    static JobFinishedNotifier.JobInfo modelInfo = new JobFinishedNotifier.JobInfo(
            "test_project",
            "9f85e8a0-3971-4012-b0e7-70763c471a01",
            Sets.newHashSet("061e2862-7a41-4516-977b-28045fcc57fe"),
            Sets.newHashSet(1L),
            0L,
            Long.MAX_VALUE,
            1000L,
            "SUCCEED");

    static boolean assertMeet = false;

    @Test
    public void testPostJobInfoSucceed() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        for (int port: ports) {
            try {
                latch = new CountDownLatch(1);
                KylinConfig config = Mockito.mock(KylinConfig.class);
                KylinConfig.setKylinConfigThreadLocal(config);
                Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server.createContext("/test", new ModelHandler());
                server.start();

                JobSchedulerListener.postJobInfo(modelInfo);

                latch.await(10, TimeUnit.SECONDS);
                server.stop(0);
            } catch (InterruptedException e) {
                Assert.fail();
            } catch (IOException e) {
                continue;
            }
            if (assertMeet) {
                break;
            }
        }
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testPostJobInfoTimeout() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    KylinConfig config = Mockito.mock(KylinConfig.class);
                    KylinConfig.setKylinConfigThreadLocal(config);
                    Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    server.createContext("/test", new TimeoutHandler());
                    server.start();

                    JobSchedulerListener.postJobInfo(modelInfo);

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

    static class ModelHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(modelInfo))) {
                    assertMeet = true;
                }
            } finally {
                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                httpExchange.close();
                latch.countDown();
            }
        }
    }

    static class TimeoutHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
                latch.countDown();
        }
    }
}
