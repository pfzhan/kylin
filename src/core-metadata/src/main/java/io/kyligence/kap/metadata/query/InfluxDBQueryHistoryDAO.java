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
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

public class InfluxDBQueryHistoryDAO implements QueryHistoryDAO {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBQueryHistoryDAO.class);
    static volatile InfluxDB influxDB;
    private String queryMetricMeasurement;
    private String realizationMetricMeasurement;

    private static final String QUERY_TIMES_BY_MODEL_SQL_FORMAT = "SELECT COUNT(DISTINCT(query_id)) as query_times FROM %s WHERE suite=~ /^%s$/ AND model=~ /^%s$/ AND time>=%dms AND time<=%dms GROUP BY model";
    private static final String QUERY_STATISTICS_SQL_FORMAT = "SELECT COUNT(query_id), MEAN(\"duration\") FROM %s WHERE time>=%dms AND time <= %dms";
    private static final String QUERY_COUNT_BY_MODEL_SQL_FORMAT = "SELECT COUNT(DISTINCT(query_id)) FROM %s WHERE time>=%dms AND time <=%dms GROUP BY model";
    private static final String AVG_DURATION_BY_MODEL_SQL_FORMAT = "SELECT MEAN(query_duration) FROM (select mean(\"duration\") AS query_duration FROM %s WHERE time>=%dms AND time<=%dms GROUP BY model, query_id) GROUP BY model";
    private static final String QUERY_STATISTICS_BY_ENGINES_SQL_FORMAT = "SELECT COUNT(query_id), MEAN(\"duration\") FROM %s WHERE (time>=%dms AND time<=%dms) AND error_type = '' GROUP BY engine_type";
    private static final String QUERY_HISTORY_BY_TIME_SQL_FORMAT = "SELECT * FROM %s WHERE time >= %dms AND time < %dms";
    private static final String FIRST_QUERY_HISTORY_SQL = "SELECT FIRST(query_id) FROM %s WHERE time >= %dms AND time < %dms";
    private static final String QUERY_COUNT_BY_TIME_SQL_PREFIX = "SELECT COUNT(query_id) FROM %s WHERE time>=%dms AND time<=%dms GROUP BY ";
    private static final String AVG_DURATION_BY_TIME_SQL_PREFIX = "SELECT MEAN(\"duration\") FROM %s WHERE time>=%dms AND time<=%dms GROUP BY ";

    private static final int MAX_SIZE = 1000000;
    private static final String QUERY_TIME_IN_MAX_SIZE = "SELECT time, query_id FROM %s ORDER BY time DESC OFFSET "
            + MAX_SIZE;

    private final String queryIdReg = "[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}";

    public static InfluxDBQueryHistoryDAO getInstance(KylinConfig config, String project) {
        return config.getManager(project, InfluxDBQueryHistoryDAO.class);
    }

    static InfluxDBQueryHistoryDAO newInstance(KylinConfig kylinConfig, String project) {
        return new InfluxDBQueryHistoryDAO(kylinConfig, project);
    }

    private KapConfig kapConfig;

    public InfluxDBQueryHistoryDAO(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing QueryHistoryDAO with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.kapConfig = KapConfig.wrap(config);
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.queryMetricMeasurement = metadataIdentifier + "_" + project + "_" + QueryHistory.QUERY_MEASUREMENT_SURFIX;
        this.realizationMetricMeasurement = metadataIdentifier + "_" + project + "_"
                + QueryHistory.REALIZATION_MEASUREMENT_SURFIX;
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
        long startTime = System.currentTimeMillis();
        final QueryResult result = getInfluxDB().query(new Query(query, QueryHistory.DB_NAME));
        long duration = System.currentTimeMillis() - startTime;
        if (duration > 3000) {
            logger.warn("current influxdb query {} takes too long time {}ms to get result", query, duration);
        } else
            logger.debug("current influxdb query takes {}ms to complete", duration);
        final InfluxDBResultMapper mapper = new InfluxDBResultMapper();

        return mapper.toPOJO(result, clazz, tableName);
    }

    public void deleteQueryHistoriesIfMaxSizeReached() {
        List<QueryStatistics> statistics = getResultBySql(
                String.format(QUERY_TIME_IN_MAX_SIZE, this.queryMetricMeasurement), QueryStatistics.class,
                this.queryMetricMeasurement);

        if (CollectionUtils.isNotEmpty(statistics)) {
            long time = statistics.get(0).getTime().toEpochMilli();
            String deleteQueryMetricSql = "delete from " + this.queryMetricMeasurement + " where time < " + time + "ms";
            getInfluxDB().query(new Query(deleteQueryMetricSql, QueryHistory.DB_NAME));

            String deleteRealizationMetricSql = "delete from " + this.realizationMetricMeasurement + " where time < "
                    + time + "ms";
            getInfluxDB().query(new Query(deleteRealizationMetricSql, QueryHistory.DB_NAME));
        }
    }

    public void deleteQueryHistoriesIfProjectMaxSizeReached(String project) {

    }

    public void deleteQueryHistoriesIfRetainTimeReached() {
    }

    public void dropProjectMeasurement() {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        String deleteQueryMetricMeasurement = "drop measurement " + queryMetricMeasurement;
        String deleteRealizationMetricMeasurement = "drop measurement " + realizationMetricMeasurement;
        getInfluxDB().query(new Query(deleteQueryMetricMeasurement, QueryHistory.DB_NAME));
        getInfluxDB().query(new Query(deleteRealizationMetricMeasurement, QueryHistory.DB_NAME));
    }

    public List<QueryHistory> getQueryHistoriesByTime(long startTime, long endTime, String project) {
        return getResultBySql(getQueryHistoriesByTimeSql(startTime, endTime), QueryHistory.class,
                this.queryMetricMeasurement);
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset, String project) {
        String sql = getQueryHistoriesSql(getQueryHistoryFilterSql(request), limit, offset);
        return getResultBySql(sql, QueryHistory.class, this.queryMetricMeasurement);
    }

    @Override
    public long getQueryHistoriesSize(QueryHistoryRequest request, String project) {
        String sql = getQueryHistoriesSizeSql(getQueryHistoryFilterSql(request));
        List<QueryHistory> queryHistories = getResultBySql(sql, QueryHistory.class, this.queryMetricMeasurement);
        if (queryHistories.isEmpty())
            return 0;
        return queryHistories.get(0).getCount();
    }

    public <T> List<T> getQueryTimesByModel(String suite, String model, long start, long end, Class clazz) {
        String sql = String.format(QUERY_TIMES_BY_MODEL_SQL_FORMAT, this.realizationMetricMeasurement, suite, model,
                start, end == 0 ? System.currentTimeMillis() : end);
        return getResultBySql(sql, clazz, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getQueryEngineStatistics(long startTime, long endTime) {
        String sql = getQueryEngineStatisticsSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project) {
        String sql = getQueryStatisticsSql(startTime, endTime);
        List<QueryStatistics> result = getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
        if (CollectionUtils.isEmpty(result))
            return new QueryStatistics();
        return result.get(0);
    }

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project) {
        String sql = getQueryCountByModelSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension, String project) {
        String sql = getQueryStatsByTimeSql(QUERY_COUNT_BY_TIME_SQL_PREFIX, startTime, endTime, timeDimension);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project) {
        String sql = getAvgDurationMeanByModelSql(startTime, endTime);
        return getResultBySql(sql, QueryStatistics.class, this.realizationMetricMeasurement);
    }

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension, String project) {
        String sql = getQueryStatsByTimeSql(AVG_DURATION_BY_TIME_SQL_PREFIX, startTime, endTime, timeDimension);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    public List<QueryStatistics> getFirstQH(long minTime, long maxTime) {
        String sql = getFirstQHSql(minTime, maxTime);
        return getResultBySql(sql, QueryStatistics.class, this.queryMetricMeasurement);
    }

    /**
     * format sqls to query Query History statistics
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

        sb.append("WHERE (1 = 1) ");

        if (StringUtils.isNotEmpty(request.getStartTimeFrom()) && StringUtils.isNotEmpty(request.getStartTimeTo())) {
            // filter by time
            sb.append(String.format("AND (query_time >= %s AND query_time < %s) ", request.getStartTimeFrom(),
                    request.getStartTimeTo()));
        }

        if (StringUtils.isNotEmpty(request.getLatencyFrom()) && StringUtils.isNotEmpty(request.getLatencyTo())) {
            // filter by duration
            sb.append(String.format("AND (\"duration\" >= %d AND \"duration\" <= %d) ",
                    Long.valueOf(request.getLatencyFrom()) * 1000L, Long.valueOf(request.getLatencyTo()) * 1000L));
            sb.append("AND (query_status = 'SUCCEEDED') ");
        }

        if (StringUtils.isNotEmpty(request.getServer())) {
            // filter by hostname
            sb.append(String.format("AND (server = '%s') ", request.getServer()));
        }

        if (StringUtils.isNotEmpty(request.getSql())) {
            sb.append(String.format("AND (sql_text =~ /%s/", escapeExprSpecialWord(request.getSql())));
            if (Pattern.matches(queryIdReg, request.getSql())) {
                sb.append(String.format(" OR query_id = '%s'", request.getSql()));
            }
            sb.append(") ");
        }

        if (request.getRealizations() != null && !request.getRealizations().isEmpty()) {
            sb.append("AND (");
            for (int i = 0; i < request.getRealizations().size(); i++) {
                switch (request.getRealizations().get(i)) {
                case "pushdown":
                    sb.append("index_hit = 'false' OR ");
                    break;
                case "modelName":
                    sb.append("index_hit = 'true' OR ");
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Illegal realization type %s", request.getRealizations().get(i)));
                }
            }

            sb.setLength(sb.length() - 4);
            sb.append(") ");
        }

        if (request.getQueryStatus() != null && !request.getQueryStatus().isEmpty()) {
            sb.append("AND (");
            for (String status : request.getQueryStatus()) {
                sb.append(String.format("query_status = '%s' OR ", status));
            }
            sb.setLength(sb.length() - 4);
            sb.append(") ");
        }

        return sb.toString();
    }

    String getQueryHistoriesByTimeSql(long startTime, long endTime) {
        return String.format(QUERY_HISTORY_BY_TIME_SQL_FORMAT, this.queryMetricMeasurement, startTime, endTime);
    }

    private String getQueryEngineStatisticsSql(long startTime, long endTime) {
        return String.format(QUERY_STATISTICS_BY_ENGINES_SQL_FORMAT, this.queryMetricMeasurement, startTime, endTime);
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

    private String getFirstQHSql(long minTime, long maxTime) {
        return String.format(FIRST_QUERY_HISTORY_SQL, this.queryMetricMeasurement, minTime, maxTime);
    }

    private String escapeExprSpecialWord(String keyword) {
        if (StringUtils.isNotBlank(keyword)) {
            keyword = keyword.replaceAll("/", "\\\\/");
            keyword = Pattern.quote(keyword);
        }
        return "(?i)" + keyword;
    }
}
