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
package io.kyligence.kap.common.metrics.service;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import io.kyligence.kap.shaded.influxdb.org.influxdb.impl.InfluxDBResultMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitorDao {

    private InfluxDBInstance influxDBInstance;
    public static final String QUERY_METRICS_BY_TIME_SQL_FORMAT = "SELECT * FROM %s WHERE create_time >= %d AND create_time < %d";

    private MonitorDao() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        val database = generateDatabase(kylinConfig);
        val rp = generateRetentionPolicy(kylinConfig);
        val retentionDuration = KapConfig.wrap(kylinConfig).getMonitorRetentionDuration();
        val shardDuration = KapConfig.wrap(kylinConfig).getMonitorShardDuration();
        val replicationFactor = KapConfig.wrap(kylinConfig).getMonitorReplicationFactor();
        val useDefault = KapConfig.wrap(kylinConfig).isMonitorUserDefault();
        this.influxDBInstance = new InfluxDBInstance(database, rp, retentionDuration, shardDuration, replicationFactor,
                useDefault);
    }

    @VisibleForTesting
    public MonitorDao(InfluxDBInstance influxDBInstance) {
        this.influxDBInstance = influxDBInstance;
    }

    public static MonitorDao getInstance() {
        return Singletons.getInstance(MonitorDao.class, monitorDaoClass -> {
            val dao = new MonitorDao();
            dao.influxDBInstance.init();
            return dao;
        });
    }

    public static String generateDatabase(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorDatabase();
    }

    public static String generateRetentionPolicy(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorRetentionPolicy();
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class InfluxDBWriteRequest {
        private String database;
        private String measurement;
        private Map<String, String> tags;
        private Map<String, Object> fields;
        private Long timeStamp;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class InfluxDBReadRequest {
        private String database;
        private String measurement;
        private Long startTime;
        private Long endTime;
    }

    public InfluxDBWriteRequest convert2InfluxDBWriteRequest(MonitorMetric monitorMetric) {
        return new InfluxDBWriteRequest(influxDBInstance.getDatabase(), monitorMetric.getTable(),
                monitorMetric.getTags(), monitorMetric.getFields(), monitorMetric.getCreateTime());
    }

    public boolean write2InfluxDB(InfluxDBWriteRequest writeRequest) {
        return this.influxDBInstance.write(writeRequest.getMeasurement(), writeRequest.getTags(),
                writeRequest.getFields(), writeRequest.getTimeStamp());
    }

    public List<QueryMonitorMetric> readQueryMonitorMetricFromInfluxDB(Long startTime, Long endTime) {
        QueryResult queryResult = readFromInfluxDBByTime(new InfluxDBReadRequest(this.influxDBInstance.getDatabase(),
                QueryMonitorMetric.QUERY_MONITOR_METRIC_TABLE, startTime, endTime));

        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        return resultMapper.toPOJO(queryResult, QueryMonitorMetric.class,
                QueryMonitorMetric.QUERY_MONITOR_METRIC_TABLE);
    }

    public List<JobStatusMonitorMetric> readJobStatusMonitorMetricFromInfluxDB(Long startTime, Long endTime) {
        QueryResult queryResult = readFromInfluxDBByTime(new InfluxDBReadRequest(this.influxDBInstance.getDatabase(),
                JobStatusMonitorMetric.JOB_STATUS_MONITOR_METRIC_TABLE, startTime, endTime));

        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        return resultMapper.toPOJO(queryResult, JobStatusMonitorMetric.class,
                JobStatusMonitorMetric.JOB_STATUS_MONITOR_METRIC_TABLE);
    }

    private QueryResult readFromInfluxDBByTime(InfluxDBReadRequest readRequest) {
        String influxDBSql = String.format(QUERY_METRICS_BY_TIME_SQL_FORMAT, readRequest.getMeasurement(),
                readRequest.getStartTime(), readRequest.getEndTime());

        return this.influxDBInstance.read(influxDBSql);
    }

}
