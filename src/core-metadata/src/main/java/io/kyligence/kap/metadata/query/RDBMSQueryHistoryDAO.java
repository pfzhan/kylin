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

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import org.springframework.jdbc.core.JdbcTemplate;

@NoArgsConstructor
public class RDBMSQueryHistoryDAO implements QueryHistoryDAO {
    private static final Logger logger = LoggerFactory.getLogger(RDBMSQueryHistoryDAO.class);
    @Setter
    private String queryMetricMeasurement;
    private String realizationMetricMeasurement;
    @Setter
    private JdbcTemplate jdbcTemplate;

    protected static final String QUERY_COUNT_AND_AVG_DURATION_SQL_FORMAT = "SELECT COUNT(query_id) as count, avg(duration) as avg_duration FROM %s WHERE query_time>=? AND query_time <= ? AND project_name = ?";
    protected static final String QUERY_COUNT_BY_MODEL_SQL_FORMAT = "SELECT model,COUNT(query_id) as count FROM %s WHERE query_time>=? AND query_time <=? AND project_name = ? GROUP BY model";
    protected static final String AVG_DURATION_BY_MODEL_SQL_FORMAT = "SELECT model,avg(query_duration) as avg_duration FROM (select avg(duration) AS query_duration, model FROM %s WHERE query_time>=? AND query_time<=? AND project_name = ? GROUP BY model, query_id) as subquery GROUP BY model";
    protected static final String QUERY_COUNT_BY_TIME_SQL_FORMAT = "select %s as time,COUNT(id) as count FROM %s WHERE query_time>=? AND query_time<=? AND project_name = ? GROUP BY %s";
    protected static final String AVG_DURATION_BY_TIME_SQL_FORMAT = "select %s as time,avg(duration) as avg_duration FROM %s WHERE query_time>=? AND query_time<=? AND project_name = ? GROUP BY %s";
    protected static final String QUERY_HISTORY_BY_TIME_SQL_FORMAT = "SELECT * FROM %s WHERE query_time >= ? AND query_time < ? AND project_name = ?";
    protected static final String FIRST_QUERY_HISTORY_SQL_FORMAT = "SELECT query_id,query_time FROM %s WHERE query_time >= ? AND query_time < ? AND project_name = ? order by query_id limit 1";
    protected static final String DELETE_QUERY_HISTORY_SQL_FORMAT = "delete from %s where query_time < ? ";
    protected static final String QUERY_HISTORY_BY_ID_SQL_FORMAT = "SELECT * FROM %s WHERE id > ? and query_time < %s limit %s";

    private static final long RETAIN_TIME = 30;
    protected static final String QUERY_TIME_IN_MAX_SIZE = "SELECT query_time as time,id FROM %s ORDER BY id DESC limit 1 OFFSET %s";
    protected static final String QUERY_TIME_IN_PROJECT_MAX_SIZE = "SELECT query_time as time,id FROM %s where project_name = ? ORDER BY id DESC limit 1 OFFSET %s";

    public static final String WEEK = "week";
    public static final String DAY = "day";

    public static RDBMSQueryHistoryDAO getInstance(KylinConfig config) {
        return config.getManager(RDBMSQueryHistoryDAO.class);
    }

    static RDBMSQueryHistoryDAO newInstance(KylinConfig kylinConfig) throws Exception {
        return new RDBMSQueryHistoryDAO(kylinConfig);
    }

    public RDBMSQueryHistoryDAO(KylinConfig config) throws Exception {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing RDBMSQueryHistoryDAO with KylinConfig Id: {} ", System.identityHashCode(config));
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.queryMetricMeasurement = metadataIdentifier + "_" + QueryHistory.QUERY_MEASUREMENT_SURFIX;
        this.realizationMetricMeasurement = metadataIdentifier + "_" + QueryHistory.REALIZATION_MEASUREMENT_SURFIX;
        initJdbcTemplate(config);
    }

    public String getQueryMetricMeasurement() {
        return queryMetricMeasurement;
    }

    public String getRealizationMetricMeasurement() {
        return realizationMetricMeasurement;
    }

    private void initJdbcTemplate(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void deleteQueryHistoriesIfMaxSizeReached() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<QueryStatistics> maxSizeStatistics = JDBCResultMapper
                .queryStatisticsResultMapper(jdbcTemplate.queryForList(String.format(QUERY_TIME_IN_MAX_SIZE,
                        queryMetricMeasurement, kylinConfig.getQueryHistoryMaxSize())));
        if (CollectionUtils.isNotEmpty(maxSizeStatistics)) {
            long time = maxSizeStatistics.get(0).getTime().toEpochMilli();
            jdbcTemplate.update(String.format(DELETE_QUERY_HISTORY_SQL_FORMAT, this.queryMetricMeasurement),
                    time);
            jdbcTemplate.update(
                    String.format(DELETE_QUERY_HISTORY_SQL_FORMAT, this.realizationMetricMeasurement), time);
        }
    }

    public void deleteQueryHistoriesIfProjectMaxSizeReached(String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<QueryStatistics> projectMaxSizeStatistics = JDBCResultMapper
                .queryStatisticsResultMapper(jdbcTemplate.queryForList(String.format(QUERY_TIME_IN_PROJECT_MAX_SIZE,
                        queryMetricMeasurement, kylinConfig.getQueryHistoryProjectMaxSize()), "'" + project + "'"));
        if (CollectionUtils.isNotEmpty(projectMaxSizeStatistics)) {
            long time = projectMaxSizeStatistics.get(0).getTime().toEpochMilli();
            jdbcTemplate.update(String.format("delete from %s where project_name = ? and query_time < ? ",
                    this.queryMetricMeasurement), "'" + project + "'", time);
            jdbcTemplate.update(String.format("delete from %s where project_name = ? and query_time < ? ",
                    this.realizationMetricMeasurement), "'" + project + "'", time);
        }
    }

    public void deleteQueryHistoriesIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcTemplate.update(String.format(DELETE_QUERY_HISTORY_SQL_FORMAT,
                this.queryMetricMeasurement), retainTime);
        jdbcTemplate.update(String.format(DELETE_QUERY_HISTORY_SQL_FORMAT,
                this.realizationMetricMeasurement), retainTime);
    }
    
    public static long getRetainTime() {
        return new Date(System.currentTimeMillis() - RETAIN_TIME * 86400000L).getTime();
    }

    public void dropProjectMeasurement(String project) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        String deleteQueryHistoryForProjectSql = "delete from %s where project_name = ?";
        jdbcTemplate.update(String.format(deleteQueryHistoryForProjectSql, queryMetricMeasurement), project);
        jdbcTemplate.update(String.format(deleteQueryHistoryForProjectSql, realizationMetricMeasurement), project);
    }

    public List<QueryHistory> getQueryHistoriesByTime(long startTime, long endTime, String project) {
        return JDBCResultMapper.queryHistoryResultMapper(
                jdbcTemplate.queryForList(String.format(QUERY_HISTORY_BY_TIME_SQL_FORMAT, this.queryMetricMeasurement),
                        startTime, endTime, project));
    }

    public List<QueryHistory> getQueryHistoriesById(long idOffset, int batchSize) {
        return JDBCResultMapper
                .queryHistoryResultMapper(jdbcTemplate.queryForList(String.format(QUERY_HISTORY_BY_ID_SQL_FORMAT,
                        this.queryMetricMeasurement, System.currentTimeMillis(), batchSize), idOffset));
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset, String project) {
        String sql = getQueryHistoriesSql(getQueryHistoryFilterSql(request));
        return JDBCResultMapper
                .queryHistoryResultMapper(jdbcTemplate.queryForList(sql, project, limit, offset * limit));
    }

    public long getQueryHistoriesSize(QueryHistoryRequest request, String project) {
        String sql = getQueryHistoriesSizeSql(getQueryHistoryFilterSql(request));
        return JDBCResultMapper.queryHistoryCountResultMapper(
                jdbcTemplate.queryForList(sql, project));
    }

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project) {
        String sql = String.format(QUERY_COUNT_AND_AVG_DURATION_SQL_FORMAT, this.queryMetricMeasurement);
        List<QueryStatistics> result = JDBCResultMapper
                .queryStatisticsResultMapper(jdbcTemplate.queryForList(sql, startTime, endTime, project));
        if (CollectionUtils.isEmpty(result))
            return new QueryStatistics();
        return result.get(0);
    }

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project) {
        return JDBCResultMapper.queryStatisticsResultMapper(
                jdbcTemplate.queryForList(String.format(QUERY_COUNT_BY_MODEL_SQL_FORMAT, realizationMetricMeasurement),
                        startTime, endTime, project));
    }

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension,
            String project) {
        return JDBCResultMapper.queryStatisticsResultMapper(jdbcTemplate.queryForList(
                getQueryStatsByTimeSql(QUERY_COUNT_BY_TIME_SQL_FORMAT, timeDimension), startTime, endTime, project));
    }

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project) {
        return JDBCResultMapper.queryStatisticsResultMapper(
                jdbcTemplate.queryForList(String.format(AVG_DURATION_BY_MODEL_SQL_FORMAT, this.realizationMetricMeasurement),
                        startTime, endTime, project));
    }

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension, String project) {
        return JDBCResultMapper.queryStatisticsResultMapper(jdbcTemplate.queryForList(
                getQueryStatsByTimeSql(AVG_DURATION_BY_TIME_SQL_FORMAT, timeDimension), startTime, endTime, project));
    }

    public List<QueryStatistics> getFirstQH(long minTime, long maxTime, String project) {
        String sql = String.format(FIRST_QUERY_HISTORY_SQL_FORMAT, queryMetricMeasurement);
        return JDBCResultMapper
                .firstQHResultMapper(jdbcTemplate.queryForList(sql, minTime, maxTime, project));
    }

    /**
     * format sqls to query Query History statistics
     */

    String getQueryHistoriesSql(String filterSql) {
        return String.format("SELECT * FROM %s ", queryMetricMeasurement) + filterSql
                + " ORDER BY query_time DESC LIMIT ? OFFSET ?";
    }

    String getQueryHistoriesSizeSql(String filterSql) {
        return String.format("SELECT count(query_id) as count FROM %s ", queryMetricMeasurement) + filterSql;
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
            sb.append(String.format("AND (sql_text like '%s'", "%"+request.getSql()+"%"));
            sb.append(String.format(" OR query_id like '%s'", "%"+request.getSql()+"%"));
            sb.append(") ");
        }

        if (request.getRealizations() != null && !request.getRealizations().isEmpty()) {
            boolean pushdown = request.getRealizations().contains("pushdown");
            boolean model = request.getRealizations().contains("modelName");
            if (pushdown && !model) {
                sb.append(" AND (index_hit = false ) ");
            } else if (!pushdown && model) {
                sb.append(" AND (index_hit = true ) ");
            } else if (!pushdown && !model) {
                throw new IllegalArgumentException("Illegal realization type ");
            }
        }

        if (request.getQueryStatus() != null && !request.getQueryStatus().isEmpty()) {
            sb.append("AND (");
            for (String status : request.getQueryStatus()) {
                sb.append(String.format("query_status = '%s' OR ", status));
            }
            sb.setLength(sb.length() - 4);
            sb.append(") ");
        }
        sb.append("AND project_name = ? ");

        if (!request.isAdmin()) {
            sb.append(String.format(" AND submitter = '%s' ", request.getUsername()));
        }

        return sb.toString();
    }

    protected String getQueryStatsByTimeSql(String sqlPrefix, String timeDimension) {
        if (timeDimension.equals("month")) {
            return String.format(sqlPrefix, QueryHistory.QUERY_FIRST_DAY_OF_MONTH, queryMetricMeasurement,
                    QueryHistory.QUERY_FIRST_DAY_OF_MONTH);
        } else if (timeDimension.equals("week")) {
            return String.format(sqlPrefix, QueryHistory.QUERY_FIRST_DAY_OF_WEEK, queryMetricMeasurement,
                    QueryHistory.QUERY_FIRST_DAY_OF_WEEK);
        } else {
            return String.format(sqlPrefix, QueryHistory.QUERY_DAY, queryMetricMeasurement, QueryHistory.QUERY_DAY);
        }
    }

    public static void fillZeroForQueryStatistics(List<QueryStatistics> queryStatistics, long startTime, long endTime,
            String dimension) {
        if (!dimension.equals(DAY) && !dimension.equals(WEEK)) {
            return;
        }
        if (dimension.equals(WEEK)) {
            startTime = TimeUtil.getWeekStart(startTime);
            endTime = TimeUtil.getWeekStart(endTime);
        }
        Set<Instant> instantSet = queryStatistics.stream().map(QueryStatistics::getTime).collect(Collectors.toSet());
        int rawOffsetTime = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();

        long startOffSetTime = Instant.ofEpochMilli(startTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant startInstant = Instant.ofEpochMilli(startOffSetTime - startOffSetTime % (1000 * 60 * 60 * 24));
        long endOffSetTime = Instant.ofEpochMilli(endTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant endInstant = Instant.ofEpochMilli(endOffSetTime - endOffSetTime % (1000 * 60 * 60 * 24));
        while (!startInstant.isAfter(endInstant)) {
            if (!instantSet.contains(startInstant)) {
                QueryStatistics zeroStatistics = new QueryStatistics();
                zeroStatistics.setCount(0);
                zeroStatistics.setTime(startInstant);
                queryStatistics.add(zeroStatistics);
            }
            if (dimension.equals(DAY)) {
                startInstant = startInstant.plus(Duration.ofDays(1));
            } else if (dimension.equals(WEEK)) {
                startInstant = startInstant.plus(Duration.ofDays(7));
            }
        }
    }
}
