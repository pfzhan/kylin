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

package io.kyligence.kap.metadata.query;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.metric.MetricWriter;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

public class QueryHistoryDAO {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryDAO.class);
    static volatile InfluxDB influxDB;
    private String queryMetricMeasurement;
    private String realizationMetricMeasurement;

    private final String QUERY_TIMES_SQL_FORMAT = "select count(query_id) as query_times from realization_metric where suite=~ /^%s$/ and project=~ /^%s$/ and model=~ /^%s$/ and time>=%dms and time<=%dms group by model";

    public static QueryHistoryDAO getInstance(KylinConfig config, String project) {
        return config.getManager(project, QueryHistoryDAO.class);
    }

    static QueryHistoryDAO newInstance(KylinConfig kylinConfig, String project) {
        return new QueryHistoryDAO(kylinConfig, project);
    }

    private KapConfig kapConfig;

    public QueryHistoryDAO(KylinConfig config, String project) {
        logger.info("Initializing QueryHistoryDAO with config " + config);
        this.kapConfig = KapConfig.wrap(config);
        this.queryMetricMeasurement = getMeasurementName(config, project, QueryHistory.QUERY_MEASUREMENT_PREFIX);
        this.realizationMetricMeasurement = getMeasurementName(config, project,
                QueryHistory.REALIZATION_MEASUREMENT_PREFIX);
    }

    private String getMeasurementName(KylinConfig config, String projectName, String measurementPrefix) {
        String projectId = NProjectManager.getInstance(config).getProject(projectName).getId().replace('-', '_');
        return config.getMetadataUrl().getIdentifier().replaceAll("[^0-9|a-z|A-Z|_]{1,}", "_") + "_" + measurementPrefix
                + "_" + projectId;
    }

    public String getQueryMetricMeasurement() {
        return queryMetricMeasurement;
    }

    public String getRealizationMetricMeasurement() {
        return realizationMetricMeasurement;
    }

    private InfluxDB getInfluxDB() {
        if (influxDB == null) {
            synchronized (this) {
                if (influxDB != null) {
                    return this.influxDB;
                }

                this.influxDB = InfluxDBFactory.connect("http://" + kapConfig.influxdbAddress(),
                        kapConfig.influxdbUsername(), kapConfig.influxdbPassword());
            }
        }

        return this.influxDB;
    }

    public <T> List<T> getQueryHistoriesBySql(String query, Class clazz) {
        if (!getInfluxDB().databaseExists(QueryHistory.DB_NAME))
            return Lists.newArrayList();
        final QueryResult result = getInfluxDB().query(new Query(query, QueryHistory.DB_NAME));
        final InfluxDBResultMapper mapper = new InfluxDBResultMapper();

        return mapper.toPOJO(result, clazz, this.queryMetricMeasurement);
    }

    public List<QueryHistory> getQueryHistoriesByTime(long startTime, long endTime) {
        return getQueryHistoriesBySql(getQueryHistoriesByTimeSql(startTime, endTime), QueryHistory.class);
    }

    String getQueryHistoriesByTimeSql(long startTime, long endTime) {
        return String.format("SELECT * FROM %s WHERE time >= %dms AND time < %dms", this.queryMetricMeasurement,
                startTime, endTime);
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset) {
        String sql = getQueryHistoriesSql(getQueryHistoryFilterSql(request), limit, offset);
        return getQueryHistoriesBySql(sql, QueryHistory.class);
    }

    public int getQueryHistoriesSize(QueryHistoryRequest request) {
        String sql = getQueryHistoriesSizeSql(getQueryHistoryFilterSql(request));
        List<QueryHistory> queryHistories = getQueryHistoriesBySql(sql, QueryHistory.class);
        if (queryHistories.isEmpty())
            return 0;
        return queryHistories.get(0).getCount();
    }

    String getQueryHistoriesSql(String filterSql, int limit, int offset) {
        return String.format("SELECT * FROM %s ", queryMetricMeasurement) + filterSql
                + String.format("ORDER BY time DESC LIMIT %d OFFSET %d", limit, offset * limit);
    }

    String getQueryHistoriesSizeSql(String filterSql) {
        return String.format("SELECT count(query_id) FROM %s ", queryMetricMeasurement) + filterSql;
    }

    String getQueryHistoryFilterSql(QueryHistoryRequest request) {
        StringBuilder sb = new StringBuilder();

        // filter by time
        sb.append(String.format("WHERE (query_time >= %d AND query_time < %d) ", request.getStartTimeFrom(),
                request.getStartTimeTo()));
        // filter by duration
        sb.append(String.format("AND (\"duration\" >= %d AND \"duration\" <= %d) ", request.getLatencyFrom() * 1000L,
                request.getLatencyTo() * 1000L));

        if (StringUtils.isNotEmpty(request.getSql())) {
            sb.append(String.format("AND sql_text =~ /%s/ ", request.getSql()));
        }

        if (request.getRealizations() != null && !request.getRealizations().isEmpty()) {
            sb.append("AND (");
            for (int i = 0; i < request.getRealizations().size(); i++) {
                switch (request.getRealizations().get(i)) {
                case "pushdown":
                    sb.append("cube_hit = 'false' OR ");
                    break;
                case "modelName":
                    sb.append("cube_hit = 'true' OR ");
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Illegal realization type %s", request.getRealizations().get(i)));
                }

                if (i == request.getRealizations().size() - 1) {
                    sb.setLength(sb.length() - 4);
                    sb.append(") ");
                }
            }
        }

        if (request.getAccelerateStatuses() != null && !request.getAccelerateStatuses().isEmpty()) {
            sb.append("AND (");
            for (int i = 0; i < request.getAccelerateStatuses().size(); i++) {
                if (i == request.getAccelerateStatuses().size() - 1)
                    sb.append(String.format("accelerate_status = '%s') ", request.getAccelerateStatuses().get(i)));
                else
                    sb.append(String.format("accelerate_status = '%s' OR ", request.getAccelerateStatuses().get(i)));
            }
        }

        return sb.toString();
    }

    public <T> List<T> getQueryTimesResponseBySql(String suite, String project, String model, long start, long end,
            Class clazz) {
        String sql = String.format(QUERY_TIMES_SQL_FORMAT, suite, project, model, start,
                end == 0 ? System.currentTimeMillis() : end);
        return getQueryHistoriesBySql(sql, clazz);
    }

    public <T> List<T> getCuboidLayoutQueryTimes(String project, int queryTimesThreshold, Class clazz) {
        String query = getCuboidLayoutQueryTimesSql(project, queryTimesThreshold);
        return getQueryHistoriesBySql(query, clazz);
    }

    private String getCuboidLayoutQueryTimesSql(String project, int queryTimesThreshold) {
        return String.format(
                "SELECT * FROM (SELECT count(query_id) as query_times FROM %s "
                        + "WHERE project = '%s' group by model, cuboid_layout_id) WHERE query_times > %d ",
                QueryHistory.REALIZATION_MEASUREMENT_PREFIX, project, queryTimesThreshold);
    }

    private void checkMetricWriterType() {
        if (!MetricWriter.Type.INFLUX.name().equals(kapConfig.diagnosisMetricWriterType())) {
            throw new IllegalStateException("Not set kap.metric.diagnosis.graph-writer-type to 'INFLUX'");
        }
    }

    public List<QueryStatisticsResponse.QueryStatistics> getQueryStatistics(long startTime, long endTime) {
        checkMetricWriterType();
        return getQueryHistoriesBySql(getQueryStatisticsSql(startTime, endTime), QueryStatisticsResponse.QueryStatistics.class);
    }

    private String getQueryStatisticsSql(long startTime, long endTime) {
        return String.format(
                "SELECT COUNT(query_id), MEAN(\"duration\") FROM %s WHERE (time >= %dms AND time <= %dms) AND error_type = '' GROUP BY engine_type",
                queryMetricMeasurement, startTime, endTime);
    }
}
