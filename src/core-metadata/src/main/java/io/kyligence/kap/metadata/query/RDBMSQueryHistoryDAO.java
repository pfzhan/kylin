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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class RDBMSQueryHistoryDAO implements QueryHistoryDAO {
    private static final Logger logger = LoggerFactory.getLogger(RDBMSQueryHistoryDAO.class);
    @Setter
    private String queryMetricMeasurement;
    private String realizationMetricMeasurement;
    private JdbcQueryHistoryStore jdbcQueryHisStore;
    private static final long RETAIN_TIME = 30;

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
        jdbcQueryHisStore = new JdbcQueryHistoryStore(config);
    }

    public String getQueryMetricMeasurement() {
        return queryMetricMeasurement;
    }

    public String getRealizationMetricMeasurement() {
        return realizationMetricMeasurement;
    }

    public int insert(QueryMetrics metrics) {
        return jdbcQueryHisStore.insert(metrics);
    }

    public void insert(List<QueryMetrics> metricsList) {
        jdbcQueryHisStore.insert(metricsList);
    }

    public void dropQueryHistoryTable() throws SQLException {
        jdbcQueryHisStore.dropQueryHistoryTable();
    }

    public void deleteAllQueryHistory() {
        jdbcQueryHisStore.deleteQueryHistory();
    }

    public void deleteAllQueryHistoryRealization() {
        jdbcQueryHisStore.deleteQueryHistoryRealization();
    }

    public void deleteQueryHistoryForProject(String project) {
        jdbcQueryHisStore.deleteQueryHistory(project);
    }

    public void deleteAllQueryHistoryRealizationForProject(String project) {
        jdbcQueryHisStore.deleteQueryHistoryRealization(project);
    }

    public void deleteQueryHistoriesIfMaxSizeReached() {
        QueryHistory queryHistory = jdbcQueryHisStore
                .queryOldestQueryHistory(KylinConfig.getInstanceFromEnv().getQueryHistoryMaxSize());
        if (Objects.nonNull(queryHistory)) {
            long time = queryHistory.getQueryTime();
            jdbcQueryHisStore.deleteQueryHistory(time);
            jdbcQueryHisStore.deleteQueryHistoryRealization(time);
        }
    }

    public void deleteQueryHistoriesIfProjectMaxSizeReached(String project) {
        QueryHistory queryHistory = jdbcQueryHisStore
                .queryOldestQueryHistory(KylinConfig.getInstanceFromEnv().getQueryHistoryProjectMaxSize(), project);
        if (Objects.nonNull(queryHistory)) {
            long time = queryHistory.getQueryTime();
            jdbcQueryHisStore.deleteQueryHistory(time, project);
            jdbcQueryHisStore.deleteQueryHistoryRealization(time, project);
        }
    }

    public void deleteQueryHistoriesIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcQueryHisStore.deleteQueryHistory(retainTime);
        jdbcQueryHisStore.deleteQueryHistoryRealization(retainTime);
    }

    public void batchUpdataQueryHistorieInfo(List<Pair<Long, QueryHistoryInfo>> idToQHInfoList) {
        jdbcQueryHisStore.updateQueryHistoryInfo(idToQHInfoList);
    }

    public static long getRetainTime() {
        return new Date(System.currentTimeMillis() - RETAIN_TIME * 86400000L).getTime();
    }

    public void dropProjectMeasurement(String project) {
        jdbcQueryHisStore.deleteQueryHistory(project);
        jdbcQueryHisStore.deleteQueryHistoryRealization(project);
    }

    public List<QueryHistory> getAllQueryHistories() {
        return jdbcQueryHisStore.queryAllQueryHistories();
    }

    public List<QueryHistory> queryQueryHistoriesByIdOffset(long idOffset, int batchSize, String project) {
        return jdbcQueryHisStore.queryQueryHistoriesByIdOffset(idOffset, batchSize, project);
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset) {
        return jdbcQueryHisStore.queryQueryHistoriesByConditions(request, limit, offset);
    }

    public long getQueryHistoriesSize(QueryHistoryRequest request, String project) {
        return jdbcQueryHisStore.queryQueryHistoriesSize(request).getCount();
    }

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project) {
        List<QueryStatistics> result = jdbcQueryHisStore.queryCountAndAvgDuration(startTime, endTime, project);
        if (CollectionUtils.isEmpty(result))
            return new QueryStatistics();
        return result.get(0);
    }

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project) {
        return jdbcQueryHisStore.queryCountByModel(startTime, endTime, project);
    }

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension,
            String project) {
        return jdbcQueryHisStore.queryCountByTime(startTime, endTime, timeDimension, project);
    }

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project) {
        return jdbcQueryHisStore.queryAvgDurationByModel(startTime, endTime, project);
    }

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension,
            String project) {
        return jdbcQueryHisStore.queryAvgDurationByTime(startTime, endTime, timeDimension, project);
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
