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

package io.kyligence.kap.metadata.query;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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

    private static final String CUBOID_LAYOUT_QUERY_TIMES_SQL_FORMAT = "SELECT * FROM (SELECT count(query_id) as query_times FROM %s group by model, cuboid_layout_id) WHERE query_times > %d";
    private static final String QUERY_TIMES_BY_MODEL_SQL_FORMAT = "SELECT COUNT(DISTINCT(query_id)) as query_times FROM %s WHERE suite=~ /^%s$/ AND model=~ /^%s$/ AND time>=%dms AND time<=%dms GROUP BY model";
    private static final String QUERY_STATISTICS_SQL_FORMAT = "SELECT COUNT(query_id), MEAN(\"duration\") FROM %s WHERE time>=%dms AND time <= %dms";
    private static final String QUERY_COUNT_BY_MODEL_SQL_FORMAT = "SELECT COUNT(DISTINCT(query_id)) FROM %s WHERE time>=%dms AND time <=%dms GROUP BY model";
    private static final String AVG_DURATION_BY_MODEL_SQL_FORMAT = "SELECT MEAN(query_duration) FROM (select mean(\"duration\") AS query_duration FROM %s WHERE time>=%dms AND time<=%dms GROUP BY model, query_id) GROUP BY model";
    private static final String QUERY_STATISTICS_BY_ENGINES_SQL_FORMAT = "SELECT COUNT(query_id), MEAN(\"duration\") FROM %s WHERE (time>=%dms AND time<=%dms) AND error_type = '' GROUP BY engine_type";
    private static final String QUERY_HISTORY_BY_TIME_SQL_FORMAT = "SELECT * FROM %s WHERE time >= %dms AND time < %dms";

    private static final String QUERY_COUNT_BY_TIME_SQL_PREFIX = "SELECT COUNT(query_id) FROM %s WHERE time>=%dms AND time<=%dms GROUP BY ";
    private static final String AVG_DURATION_BY_TIME_SQL_PREFIX = "SELECT MEAN(\"duration\") FROM %s WHERE time>=%dms AND time<=%dms GROUP BY ";

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

    protected <T> List<T> getResultBySql(String query, Class clazz, String tableName) {
        if (!getInfluxDB().databaseExists(QueryHistory.DB_NAME))
            return Lists.newArrayList();
        final QueryResult result = getInfluxDB().query(new Query(query, QueryHistory.DB_NAME));
        final InfluxDBResultMapper mapper = new InfluxDBResultMapper();

        return mapper.toPOJO(result, clazz, tableName);
    }

    public List<QueryHistory> getQueryHistoriesByTime(long startTime, long endTime) {
        return getResultBySql(getQueryHistoriesByTimeSql(startTime, endTime), QueryHistory.class, this.queryMetricMeasurement);
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset) {
        String sql = getQueryHistoriesSql(getQueryHistoryFilterSql(request), limit, offset);
        return getResultBySql(sql, QueryHistory.class, this.queryMetricMeasurement);
    }

    public int getQueryHistoriesSize(QueryHistoryRequest request) {
        String sql = getQueryHistoriesSizeSql(getQueryHistoryFilterSql(request));
        List<QueryHistory> queryHistories = getResultBySql(sql, QueryHistory.class, this.queryMetricMeasurement);
        if (queryHistories.isEmpty())
            return 0;
        return queryHistories.get(0).getCount();
    }

    public <T> List<T> getQueryTimesByModel(String suite, String model, long start, long end, Class clazz) {
        String sql = String.format(QUERY_TIMES_BY_MODEL_SQL_FORMAT, this.realizationMetricMeasurement, suite, model, start,
                end == 0 ? System.currentTimeMillis() : end);
        return getResultBySql(sql, clazz, this.realizationMetricMeasurement);
    }

    public <T> List<T> getCuboidLayoutQueryTimes(int queryTimesThreshold, Class clazz) {
        String query = getCuboidLayoutQueryTimesSql(queryTimesThreshold);
        return getResultBySql(query, clazz, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getQueryEngineStatistics(long startTime, long endTime) {
        String sql = getQueryEngineStatisticsSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime) {
        String sql = getQueryStatisticsSql(startTime, endTime);
        List<QueryStatistics> result = getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
        return result.get(0);
    }

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime) {
        String sql = getQueryCountByModelSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension) {
        String sql = getQueryStatsByTimeSql(QUERY_COUNT_BY_TIME_SQL_PREFIX, startTime, endTime, timeDimension);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime) {
        String sql = getAvgDurationMeanByModelSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension) {
        String sql = getQueryStatsByTimeSql(AVG_DURATION_BY_TIME_SQL_PREFIX, startTime, endTime, timeDimension);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }
    /**
     *  format sqls to query Query History statistics
     */

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

    String getQueryHistoriesByTimeSql(long startTime, long endTime) {
        return String.format(QUERY_HISTORY_BY_TIME_SQL_FORMAT, this.queryMetricMeasurement,
                startTime, endTime);
    }

    private String getQueryEngineStatisticsSql(long startTime, long endTime) {
        return String.format(QUERY_STATISTICS_BY_ENGINES_SQL_FORMAT, this.queryMetricMeasurement, startTime, endTime);
    }

    private String getCuboidLayoutQueryTimesSql(int queryTimesThreshold) {
        return String.format(CUBOID_LAYOUT_QUERY_TIMES_SQL_FORMAT, this.realizationMetricMeasurement, queryTimesThreshold);
    }

    private String getQueryStatisticsSql(long startTime, long endTime) {
        return String.format(QUERY_STATISTICS_SQL_FORMAT, this.queryMetricMeasurement, startTime, endTime);
    }

    protected String getQueryStatsByTimeSql(String sqlPrefix, long startTime, long endTime, String timeDimension) {
        switch (timeDimension) {
            case "day":
                return String.format(sqlPrefix, this.queryMetricMeasurement, startTime, endTime) + "time(1d)";
            case "week":
                // influxDB start a week from thursday, so need to set a 4 day offset to start a week from monday
                return String.format(sqlPrefix, this.queryMetricMeasurement, startTime, endTime) + "time(1w, 4d)";
            case "month":
                return String.format(sqlPrefix, this.queryMetricMeasurement, startTime, endTime) + "month";
            default:
                return String.format(sqlPrefix, this.queryMetricMeasurement, startTime, endTime) + "time(1d)";
        }
    }

    private String getQueryCountByModelSql(long startTime, long endTime) {
        return String.format(QUERY_COUNT_BY_MODEL_SQL_FORMAT, this.realizationMetricMeasurement, startTime, endTime);
    }

    private String getAvgDurationMeanByModelSql(long startTime, long endTime) {
        return String.format(AVG_DURATION_BY_MODEL_SQL_FORMAT, this.realizationMetricMeasurement, startTime, endTime);
    }
}
