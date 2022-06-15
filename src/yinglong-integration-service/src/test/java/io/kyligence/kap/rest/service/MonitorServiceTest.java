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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kyligence.kap.common.state.StateSwitchConstant;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.state.QueryShareStateManager;
import io.kyligence.kap.rest.request.AlertMessageRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.metrics.service.JobStatusMonitorMetric;
import io.kyligence.kap.common.metrics.service.MonitorDao;
import io.kyligence.kap.common.metrics.service.QueryMonitorMetric;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.MockClusterManager;
import io.kyligence.kap.rest.response.ClusterStatusResponse;
import lombok.val;

public class MonitorServiceTest extends SourceTestCase {

    @InjectMocks
    private MonitorService monitorService = Mockito.spy(new MonitorService());

    @InjectMocks
    private ProjectService projectService = Mockito.spy(new ProjectService());

    private ClusterManager clusterManager = new MockClusterManager();

    private MonitorDao monitorDao = Mockito.mock(MonitorDao.class);

    @Before
    public void setup() {
        super.setup();
        getTestConfig().setProperty("kylin.monitor.interval", "1");
        getTestConfig().setProperty("kylin.monitor.job-statistic-interval", "10");
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password="
        );
        getTestConfig().setProperty("kylin.query.share-state-switch-implement", "jdbc");
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        ReflectionTestUtils.setField(monitorService, "clusterManager", clusterManager);
        ReflectionTestUtils.setField(monitorService, "projectService", projectService);
        Mockito.doReturn(monitorDao).when(monitorService).getMonitorDao();
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.monitor.interval", "60");
        getTestConfig().setProperty("kylin.monitor.job-statistic-interval", "3600");
        cleanupTestMetadata();
    }

    private JobStatusMonitorMetric createJobMetric(long createTime, String host, String port, long f, long p, long e) {
        val metric = new JobStatusMonitorMetric();
        metric.setCreateTime(createTime);
        metric.setHost(host);
        metric.setPort(port);
        metric.setErrorJobs(e);
        metric.setPendingJobs(p);
        metric.setFinishedJobs(f);
        return metric;
    }

    private QueryMonitorMetric createQueryMetric(long createTime, String host, String port, long responseTime, int ea) {
        val metric = new QueryMonitorMetric();
        metric.setCreateTime(createTime);
        metric.setHost(host);
        metric.setPort(port);
        metric.setLastResponseTime(responseTime);
        metric.setErrorAccumulated(ea);
        return metric;
    }

    private List<JobStatusMonitorMetric> mockJobMetrics1() {
        return Lists.newArrayList(createJobMetric(11000L, "127.0.0.1", "7070", 2L, 10L, 4L));
    }

    private List<JobStatusMonitorMetric> mockJobMetrics2() {
        return Lists.newArrayList(createJobMetric(21000L, "127.0.0.1", "7070", 6L, 7L, 3L));
    }

    private List<QueryMonitorMetric> mockQueryMetrics1() {
        return Lists.newArrayList(createQueryMetric(11000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(11000L, "127.0.0.1", "7071", 2000L, 0));
    }

    private List<QueryMonitorMetric> mockQueryMetrics2() {
        return Lists.newArrayList(createQueryMetric(21000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(21000L, "127.0.0.1", "7071", 2000L, 0));
    }

    private void mockData_happyPath() {
        Mockito.doReturn(Lists.newArrayList()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(1000L, 2000L);
        Mockito.doReturn(mockJobMetrics1()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(11000L, 12000L);
        Mockito.doReturn(mockJobMetrics2()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(21000L, 22000L);
        Mockito.doReturn(Lists.newArrayList()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(1000L, 2000L);
        Mockito.doReturn(mockQueryMetrics1()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(11000L, 12000L);
        Mockito.doReturn(mockQueryMetrics2()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(21000L, 22000L);
    }

    private List<QueryMonitorMetric> mockQueryMetrics3() {
        return Lists.newArrayList(createQueryMetric(21000L, "127.0.0.1", "7071", 2000L, 0));
    }

    private void mockData_lost() {
        Mockito.doReturn(Lists.newArrayList()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(1000L, 2000L);
        Mockito.doReturn(mockJobMetrics1()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(11000L, 12000L);
        Mockito.doReturn(Lists.newArrayList()).when(monitorDao).readJobStatusMonitorMetricFromInfluxDB(21000L, 22000L);
        Mockito.doReturn(Lists.newArrayList()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(1000L, 2000L);
        Mockito.doReturn(mockQueryMetrics1()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(11000L, 11000L);
        Mockito.doReturn(mockQueryMetrics3()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(21000L, 22000L);
    }

    @Test
    public void testHappyPath() {
        mockData_happyPath();
        val response = monitorService.timeClusterStatus(22000L);
        Assert.assertEquals(2, response.getActiveInstances());
        Assert.assertEquals(ClusterStatusResponse.NodeState.GOOD, response.getJobStatus());
        Assert.assertEquals(ClusterStatusResponse.NodeState.GOOD, response.getQueryStatus());
    }

    @Test
    public void testHappyPath_jobStart() {
        mockData_happyPath();
        val response = monitorService.timeClusterStatus(12000L);
        Assert.assertEquals(2, response.getActiveInstances());
        Assert.assertEquals(ClusterStatusResponse.NodeState.GOOD, response.getJobStatus());
        Assert.assertEquals(ClusterStatusResponse.NodeState.GOOD, response.getQueryStatus());
    }

    @Test
    public void testHappyPath_lost() {
        mockData_lost();
        val response = monitorService.timeClusterStatus(22000L);
        Assert.assertEquals(2, response.getActiveInstances());
        Assert.assertEquals(ClusterStatusResponse.NodeState.CRASH, response.getJobStatus());
        Assert.assertEquals(ClusterStatusResponse.NodeState.WARNING, response.getQueryStatus());
    }

    @Test
    public void testFetchAndMergeSpark3Metrics() {
        String s1 = monitorService.fetchAndMergeSparkMetrics();
        Assert.assertTrue(StringUtils.isBlank(s1));

        getTestConfig().setProperty("kylin.storage.columnar.spark-conf.spark.ui.prometheus.enabled", "true");
        getTestConfig().setProperty("kylin.storage.columnar.spark-conf.spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet");
        getTestConfig().setProperty("kylin.storage.columnar.spark-conf.spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus");
        ExpectedException.none().expect(ConnectException.class);
        monitorService.fetchAndMergeSparkMetrics();
    }

    private List<JobStatusMonitorMetric> mockJobMetricList1() {
        int i = 0;
        val res = Lists.newArrayList(createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 2L, 10L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 3L, 9L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 5L, 7L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 8L, 9L, 2L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 10L, 15L, 2L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 13L, 11L, 3L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 15L, 9L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 15L, 6L, 7L)

        );
        i = 0;
        res.addAll(Lists.newArrayList(createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 20L, 10L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 24L, 6L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 24L, 7L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 24L, 9L, 2L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 28L, 5L, 2L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 10L, 3L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 9L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 6L, 7L)));
        return res;
    }

    private List<QueryMonitorMetric> mockQueryMetricList1() {
        int i = 0;
        int j = 0;
        return Lists.newArrayList(createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 4000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 8000L, 1),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 4000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 8000L, 2),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 9000L, 5),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 2000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 1),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0)

        );
    }

    private List<JobStatusMonitorMetric> mockJobMetricList2() {
        int i = 0;
        val res = Lists.newArrayList(createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 2L, 10L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 3L, 9L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 5L, 7L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 8L, 9L, 2L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 10L, 15L, 2L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 13L, 11L, 3L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 15L, 9L, 4L),
                createJobMetric(10000L + (i++) * 1000L, "127.0.0.1", "7070", 15L, 6L, 7L)

        );
        i = 0;
        res.addAll(Lists.newArrayList(createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 20L, 10L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 24L, 6L, 4L),
                createJobMetric(20030L + (i++) * 1000L, "127.0.0.1", "7070", 24L, 7L, 4L)));
        i += 2;
        res.addAll(Lists.newArrayList(createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 10L, 3L),
                createJobMetric(20050L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 9L, 4L),
                createJobMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 30L, 6L, 7L)));
        return res;
    }

    private List<QueryMonitorMetric> mockQueryMetricList2() {
        int i = 0;
        int j = 0;
        val res = Lists.newArrayList(createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 4000L, 0),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 8000L, 1),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 4000L, 0));
        i += 1;
        res.addAll(Lists.newArrayList(createQueryMetric(20030L + (i++) * 1000L, "127.0.0.1", "7070", 8000L, 2),
                createQueryMetric(20080L + (i++) * 1000L, "127.0.0.1", "7070", 9000L, 5),
                createQueryMetric(20000L + (i++) * 1000L, "127.0.0.1", "7070", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 2000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0)));
        j += 3;
        res.addAll(Lists.newArrayList(createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0),
                createQueryMetric(20000L + (j++) * 1000L, "127.0.0.1", "7071", 3000L, 0)

        ));
        return res;
    }

    private void mockData_statistic() {
        Mockito.doReturn(mockJobMetricList1()).when(monitorDao)
                .readJobStatusMonitorMetricFromInfluxDB(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(mockQueryMetricList1()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(Mockito.anyLong(),
                Mockito.anyLong());
    }

    private void mockData_statisticLostUnaligned() {
        Mockito.doReturn(mockJobMetricList2()).when(monitorDao)
                .readJobStatusMonitorMetricFromInfluxDB(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(mockQueryMetricList2()).when(monitorDao).readQueryMonitorMetricFromInfluxDB(Mockito.anyLong(),
                Mockito.anyLong());
    }

    @Test
    public void testStatistic() {
        mockData_statistic();
        val response = monitorService.statisticCluster(20000L, 28000L);
        val job = response.getJob().get(0);
        val queries = response.getQuery();

        Assert.assertEquals(0, job.getUnavailableCount());
        Assert.assertEquals(1, queries.get(0).getUnavailableCount());
        Assert.assertEquals(ClusterStatusResponse.NodeState.CRASH, queries.get(0).getDetails().get(6).getState());
        Assert.assertEquals(1000L, queries.get(0).getUnavailableTime());
        Assert.assertEquals(8, queries.get(1).getDetails().size());
    }

    @Test
    public void testStatistic_lostUnaligned() {
        mockData_statisticLostUnaligned();
        val response = monitorService.statisticCluster(20000L, 28000L);
        val job = response.getJob().get(0);
        val queries = response.getQuery();

        Assert.assertEquals(2, job.getUnavailableCount());
        Assert.assertEquals(22030L, job.getDetails().get(2).getTime());
        Assert.assertEquals(2, queries.get(0).getUnavailableCount());
        Assert.assertEquals(ClusterStatusResponse.NodeState.CRASH, queries.get(0).getDetails().get(4).getState());
        Assert.assertEquals(ClusterStatusResponse.NodeState.CRASH, queries.get(0).getDetails().get(6).getState());
        Assert.assertEquals(26080L, queries.get(0).getDetails().get(6).getTime());
        Assert.assertEquals(2000L, queries.get(0).getUnavailableTime());
        Assert.assertEquals(3000L, queries.get(1).getUnavailableTime());
    }

    @Test
    public void testHandleAlertMessage() {
        QueryShareStateManager manager = QueryShareStateManager.getInstance();

        AlertMessageRequest request = new AlertMessageRequest();
        request.setAlerts(new ArrayList<>());
        monitorService.handleAlertMessage(request);

        String stateVal = manager.getState("QueryLimit");
        Assert.assertEquals("false", stateVal);

        AlertMessageRequest.Labels label = new AlertMessageRequest.Labels();
        label.setAlertname("Spark Utilization Is Too High");
        label.setInstance(AddressUtil.concatInstanceName());
        AlertMessageRequest.Alerts alerts = new AlertMessageRequest.Alerts();
        alerts.setLabels(label);
        request.setAlerts(Collections.singletonList(alerts));

        request.setStatus("firing");
        alerts.setStatus("firing");
        monitorService.handleAlertMessage(request);
        stateVal = manager.getState(StateSwitchConstant.QUERY_LIMIT_STATE);
        Assert.assertEquals("true", stateVal);

        request.setStatus("resolved");
        alerts.setStatus("resolved");
        monitorService.handleAlertMessage(request);
        stateVal = manager.getState(StateSwitchConstant.QUERY_LIMIT_STATE);
        Assert.assertEquals("false", stateVal);
    }

}
