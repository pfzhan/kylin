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

package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.var;

public class JobSchedulerListenerTest extends NLocalFileMetadataTestCase {
    static CountDownLatch latch;
    static JobSyncListener.JobInfo modelInfo = new JobSyncListener.JobInfo(
            "f26641d7-2094-473b-972a-4e1cebe55091", "test_project", "9f85e8a0-3971-4012-b0e7-70763c471a01",
            Sets.newHashSet("061e2862-7a41-4516-977b-28045fcc57fe"), Sets.newHashSet(1L), 1000L, "SUCCEED",
            "INDEX_BUILD", new ArrayList<>(), new ArrayList<>(), 1626135824000L, 1626144908000L, null);
    static boolean assertMeet = false;

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testPostJobInfoSucceed() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        for (int port : ports) {
            try {
                latch = new CountDownLatch(1);
                KylinConfig config = Mockito.mock(KylinConfig.class);
                KylinConfig.setKylinConfigThreadLocal(config);
                Mockito.when(config.getJobFinishedNotifierUsername()).thenReturn("ADMIN");
                Mockito.when(config.getJobFinishedNotifierPassword()).thenReturn("KYLIN");
                Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server.createContext("/test", new ModelHandler());
                server.start();

                JobSyncListener.postJobInfo(modelInfo);

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

                    JobSyncListener.postJobInfo(modelInfo);

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
    public void testExtractInfo() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "SUCCEED";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d00");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        Set<Long> partitionIds = null;
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertEquals(jobId, jobInfo.getJobId());
        Assert.assertEquals(project, jobInfo.getProject());
        Assert.assertEquals(subject, jobInfo.getModelId());
        Assert.assertEquals(duration, jobInfo.getDuration());
        Assert.assertEquals(jobState, jobInfo.getState());
        Assert.assertEquals(jobType, jobInfo.getJobType());
        Assert.assertTrue(jobInfo.getSegmentIds().containsAll(segIds));
        Assert.assertEquals(segIds.size(), jobInfo.getSegmentIds().size());
        JobSyncListener.SegRange segRange = jobInfo.getSegRanges().get(0);
        Assert.assertEquals("11124840-b3e3-43db-bcab-2b78da666d00", segRange.getSegmentId());
        Assert.assertEquals(1309891513770L, segRange.getStart());
        Assert.assertEquals(1509891513770L, segRange.getEnd());
        Assert.assertTrue(jobInfo.getIndexIds().containsAll(layoutIds));
        Assert.assertEquals(layoutIds.size(), jobInfo.getIndexIds().size());
    }

    @Test
    public void testExtractInfoMultiPartition() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        long duration = 1000L;
        String jobState = "SUCCEED";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("0db919f3-1359-496c-aab5-b6f3951adc0e");
        segIds.add("ff839b0b-2c23-4420-b332-0df70e36c343");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        Set<Long> partitionIds = new HashSet<>();
        partitionIds.add(7L);
        partitionIds.add(8L);
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, partitionIds, 0L, null, "", true, startTime, endTime, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertTrue(jobInfo.getSegmentIds().containsAll(segIds));
        Assert.assertEquals(segIds.size(), jobInfo.getSegmentIds().size());
        Assert.assertEquals(jobInfo.getSegmentPartitionInfoList().size(), 2);
        Assert.assertEquals(jobInfo.getSegmentPartitionInfoList().get(0).getSegmentId(),
                "0db919f3-1359-496c-aab5-b6f3951adc0e");
        var partitionInfos = jobInfo.getSegmentPartitionInfoList().get(0).getPartitionInfo();
        Assert.assertEquals(partitionInfos.get(0).getPartitionId(), 7);
        Assert.assertEquals(partitionInfos.get(1).getPartitionId(), 8);

        Assert.assertEquals(jobInfo.getSegmentPartitionInfoList().get(1).getSegmentId(),
                "ff839b0b-2c23-4420-b332-0df70e36c343");
    }

    @Test
    public void testRecordPrometheusMetric() {
        JobSyncListener jobSyncListener = Mockito.spy(JobSyncListener.class);
        JobFinishedNotifier notifier = Mockito.mock(JobFinishedNotifier.class);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        Mockito.when(notifier.getJobType()).thenReturn(JobTypeEnum.STREAMING_BUILD.toString());
        Mockito.when(notifier.getProject()).thenReturn("project");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "false");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.ERROR);
        Collection<Meter> meters1 = meterRegistry.getMeters();
        Assert.assertEquals(0, meters1.size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "true");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "", ExecutableState.ERROR);
        Collection<Meter> meters2 = meterRegistry.getMeters();
        Assert.assertEquals(1, meters2.size());

        Mockito.when(notifier.getJobType()).thenReturn(JobTypeEnum.INDEX_BUILD.toString());
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.ERROR);
        Collection<Meter> meters3 = meterRegistry.find(PrometheusMetrics.MODEL_BUILD_DURATION.getValue()).meters();
        Collection<Meter> meters4 = meterRegistry.find(PrometheusMetrics.JOB_MINUTES.getValue()).meters();
        Assert.assertEquals(1, meters3.size());
        Assert.assertEquals(2, meters4.size());

        Mockito.when(notifier.getJobType()).thenReturn("TEST");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.SUCCEED);
    }

    static class ModelHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                assert "Basic QURNSU46S1lMSU4="
                        .equals(httpExchange.getRequestHeaders().get(HttpHeaders.AUTHORIZATION).get(0));
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
        public void handle(HttpExchange httpExchange) {
            latch.countDown();
        }
    }
}
