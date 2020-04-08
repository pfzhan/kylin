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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;

public class JDBCResultMapper {

    public static final String COUNT = "count";
    public static final String AVG_DURATION = "avg_duration";

    public static List<QueryHistory> queryHistoryResultMapper(List<Map<String, Object>> rdbmsResultBySql) {
        List<QueryHistory> queryHistoryList = Lists.newArrayList();
        for (Map<String, Object> rowMap : rdbmsResultBySql) {
            QueryHistory queryHistory = new QueryHistory();
            queryHistory.setQueryId((String) rowMap.get(QueryHistory.QUERY_ID));
            queryHistory.setSql((String) rowMap.get(QueryHistory.SQL_TEXT));
            queryHistory.setSqlPattern((String) rowMap.get(QueryHistory.SQL_PATTERN));
            queryHistory.setDuration((Long) rowMap.get(QueryHistory.QUERY_DURATION));
            queryHistory.setTotalScanBytes((Long) rowMap.get(QueryHistory.TOTAL_SCAN_BYTES));
            queryHistory.setTotalScanCount((Long) rowMap.get(QueryHistory.TOTAL_SCAN_COUNT));
            queryHistory.setResultRowCount((Long) rowMap.get(QueryHistory.RESULT_ROW_COUNT));
            queryHistory.setQuerySubmitter((String) rowMap.get(QueryHistory.SUBMITTER));
            queryHistory.setQueryRealizations((String) rowMap.get(QueryHistory.REALIZATIONS));
            queryHistory.setHostName((String) rowMap.get(QueryHistory.QUERY_SERVER));
            queryHistory.setErrorType((String) rowMap.get(QueryHistory.ERROR_TYPE));
            queryHistory.setEngineType((String) rowMap.get(QueryHistory.ENGINE_TYPE));
            queryHistory.setCacheHit((Boolean) rowMap.get(QueryHistory.IS_CACHE_HIT));
            queryHistory.setQueryStatus((String) rowMap.get(QueryHistory.QUERY_STATUS));
            queryHistory.setIndexHit((Boolean) rowMap.get(QueryHistory.IS_INDEX_HIT));
            queryHistory.setQueryTime((Long) rowMap.get(QueryHistory.QUERY_TIME));
            queryHistory.setProjectName((String) rowMap.get(QueryHistory.PROJECT_NAME));
            queryHistoryList.add(queryHistory);
        }
        return queryHistoryList;
    }

    public static List<QueryStatistics> queryStatisticsResultMapper(List<Map<String, Object>> rdbmsResultBySql) {
        List<QueryStatistics> queryStatisticsList = Lists.newArrayList();
        for (Map<String, Object> rowMap : rdbmsResultBySql) {
            QueryStatistics queryStatistics = new QueryStatistics();
            if (rowMap.get(COUNT) != null) {
                queryStatistics.setCount((long) rowMap.get(COUNT));
            }
            if (rowMap.get("time") != null) {
                int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
                long offetTime = Instant.ofEpochMilli((Long) rowMap.get("time")).plusMillis(offset).toEpochMilli();
                queryStatistics.setTime(Instant.ofEpochMilli(offetTime));
            }
            if (rowMap.get(AVG_DURATION) != null) {
                if (rowMap.get(AVG_DURATION) instanceof BigDecimal) {
                    queryStatistics.setMeanDuration(((BigDecimal) rowMap.get(AVG_DURATION)).longValue());
                } else if (rowMap.get(AVG_DURATION) instanceof Double) {
                    queryStatistics.setMeanDuration((Double) rowMap.get(AVG_DURATION));
                } else if (rowMap.get(AVG_DURATION) instanceof Long) {
                    queryStatistics.setMeanDuration((Long) rowMap.get(AVG_DURATION));
                }
            }
            if (rowMap.get(QueryHistory.MODEL) != null) {
                queryStatistics.setModel((String) rowMap.get(QueryHistory.MODEL));
            }
            queryStatisticsList.add(queryStatistics);
        }
        return queryStatisticsList;
    }

    public static long queryHistoryCountResultMapper(List<Map<String, Object>> rdbmsResultBySql) {
        for (Map<String, Object> rowMap : rdbmsResultBySql) {
            return (long) rowMap.get(COUNT);
        }
        return 0;
    }

    public static List<QueryStatistics> firstQHResultMapper(List<Map<String, Object>> rdbmsResultBySql) {
        List<QueryStatistics> queryStatisticsList = Lists.newArrayList();
        for (Map<String, Object> rowMap : rdbmsResultBySql) {
            QueryStatistics queryStatistics = new QueryStatistics();
            if (rowMap.get(QueryHistory.QUERY_TIME) != null) {
                queryStatistics.setTime(new Date((Long) (rowMap.get(QueryHistory.QUERY_TIME))).toInstant());
            }
            queryStatisticsList.add(queryStatistics);
        }
        return queryStatisticsList;
    }
}
