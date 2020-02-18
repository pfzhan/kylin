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
package io.kyligence.kap.common.metric;

import io.kyligence.kap.common.metrics.service.InfluxDBInstance;
import io.kyligence.kap.common.metrics.service.JobStatusMonitorMetric;
import io.kyligence.kap.common.metrics.service.MonitorDao;
import io.kyligence.kap.common.metrics.service.QueryMonitorMetric;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.eq;

public class MonitorDaoTest {

    private InfluxDBInstance influxDBInstance = Mockito.mock(InfluxDBInstance.class);

    @Before
    public void setUp() {
        Mockito.doReturn("KE_MONITOR").when(influxDBInstance).getDatabase();
        Mockito.doReturn(true).when(influxDBInstance).write(eq("KE_MONITOR"), eq("tb_query"), Mockito.anyMap(),
                Mockito.anyMap(), Mockito.anyLong());

        Mockito.doReturn(false).when(influxDBInstance).write(eq("KE_MONITOR"), eq("tb_job_status"), Mockito.anyMap(),
                Mockito.anyMap(), Mockito.anyLong());

        Mockito.doReturn(mockQueryMonitorMetricQueryResult()).when(influxDBInstance).read("KE_MONITOR",
                String.format(MonitorDao.QUERY_METRICS_BY_TIME_SQL_FORMAT, "tb_query", 0, Long.MAX_VALUE));
        Mockito.doReturn(mockJobStatusMonitorMetricQueryResult()).when(influxDBInstance).read("KE_MONITOR",
                String.format(MonitorDao.QUERY_METRICS_BY_TIME_SQL_FORMAT, "tb_job_status", 0, Long.MAX_VALUE));
    }

    public QueryMonitorMetric mockQueryMonitorMetric() {
        QueryMonitorMetric monitorMetric = new QueryMonitorMetric();
        monitorMetric.setHost("localhost");
        monitorMetric.setIp("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("query");
        monitorMetric.setCreateTime(System.currentTimeMillis());
        monitorMetric.setLastResponseTime(29 * 1000L);

        monitorMetric.setErrorAccumulated(1);
        monitorMetric.setSparkRestarting(false);

        return monitorMetric;
    }

    public QueryResult mockQueryMonitorMetricQueryResult() {
        QueryResult.Series series = new QueryResult.Series();
        series.setName("tb_query");
        series.setColumns(Lists.newArrayList("host", "ip", "port", "pid", "node_type", "create_time", "response_time",
                "error_accumulated", "spark_restarting"));
        List<Object> value = Lists.newArrayList("localhost", "127.0.0.1", "7070", "22333", "query",
                1.0 * System.currentTimeMillis(), 1.0 * 29 * 1000L, 1.0 * 2, Boolean.FALSE);
        series.setValues(Lists.newArrayList());
        series.getValues().add(value);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Lists.newArrayList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Lists.newArrayList(result));
        return queryResult;
    }

    public JobStatusMonitorMetric mockJobStatusMonitorMetric() {
        JobStatusMonitorMetric monitorMetric = new JobStatusMonitorMetric();
        monitorMetric.setHost("localhost");
        monitorMetric.setIp("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("job");
        monitorMetric.setCreateTime(System.currentTimeMillis());

        monitorMetric.setFinishedJobs(1L);
        monitorMetric.setPendingJobs(2L);
        monitorMetric.setErrorJobs(3L);
        return monitorMetric;
    }

    public QueryResult mockJobStatusMonitorMetricQueryResult() {
        QueryResult.Series series = new QueryResult.Series();
        series.setName("tb_job_status");
        series.setColumns(Lists.newArrayList("host", "ip", "port", "pid", "node_type", "create_time", "finished_jobs",
                "pending_jobs", "error_jobs"));
        List<Object> value = Lists.newArrayList("localhost", "127.0.0.1", "7070", "33222", "job",
                1.0 * System.currentTimeMillis(), 1.0 * 29L, 1.0 * 10, 1.0 * 5);
        series.setValues(Lists.newArrayList());
        series.getValues().add(value);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Lists.newArrayList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Lists.newArrayList(result));
        return queryResult;
    }

    @Test
    public void testWrite2InfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        boolean result = monitorDao.write2InfluxDB(monitorDao.convert2InfluxDBWriteRequest(mockQueryMonitorMetric()));
        Assert.assertTrue(result);

        result = monitorDao.write2InfluxDB(monitorDao.convert2InfluxDBWriteRequest(mockJobStatusMonitorMetric()));
        Assert.assertFalse(result);
    }

    @Test
    public void testReadQueryMonitorMetricFromInfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        List<QueryMonitorMetric> monitorMetric = monitorDao.readQueryMonitorMetricFromInfluxDB(0L, Long.MAX_VALUE);
        Assert.assertEquals(monitorMetric.get(0).getHost(), "localhost");
        Assert.assertEquals(monitorMetric.get(0).getErrorAccumulated(), Integer.valueOf(2));
    }

    @Test
    public void testReadJobStatusMonitorMetricFromInfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        List<JobStatusMonitorMetric> monitorMetric = monitorDao.readJobStatusMonitorMetricFromInfluxDB(0L,
                Long.MAX_VALUE);
        Assert.assertEquals(monitorMetric.get(0).getPid(), "33222");
        Assert.assertEquals(monitorMetric.get(0).getErrorJobs(), Long.valueOf(5));
    }
}
