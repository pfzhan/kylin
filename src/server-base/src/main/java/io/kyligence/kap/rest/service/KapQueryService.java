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

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.metric.MetricWriterStrategy;
import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.rest.response.QueryEngineStatisticsResponse;
import lombok.val;

@Component("kapQueryService")
public class KapQueryService extends QueryService {
    private static final Logger logger = LoggerFactory.getLogger(KapQueryService.class);

    @Override
    protected String makeErrorMsgUserFriendly(Throwable e) {
        String message = QueryUtil.makeErrorMsgUserFriendly(e);

        /**
         * TODO: enable adviser
         ISQLAdvisor advisor = new BasicSQLAdvisor();
         List<SQLAdvice> advices = advisor.provideAdvice(SQLResult.failedSQL(message),
         OLAPContext.getThreadLocalContexts());
         if (!CollectionUtils.isEmpty(advices)) {
         StringBuilder sb = new StringBuilder();
         for (SQLAdvice advice : advices) {
         if (advice != null)
         sb.append(advice.getIncapableReason()).append(' ').append(advice.getSuggestion()).append(' ');
         }
         message = sb.toString();
         }
        
         */
        return message;
    }

    @Override
    protected void recordMetric(SQLRequest sqlRequest, SQLResponse sqlResponse) throws UnknownHostException {
        if (sqlResponse.isPrepare()) {
            // preparing statement should not be recorded
            return;
        }

        if (QueryMetricsContext.isStarted()) {
            final QueryMetricsContext queryMetricsContext = QueryMetricsContext.collect(sqlRequest, sqlResponse,
                    QueryContext.current());

            MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME,
                    getQueryHistoryDao(sqlRequest.getProject()).getQueryMetricMeasurement(),
                    queryMetricsContext.getInfluxdbTags(), queryMetricsContext.getInfluxdbFields(),
                    System.currentTimeMillis());

            for (final QueryMetricsContext.RealizationMetrics realizationMetrics : queryMetricsContext
                    .getRealizationMetrics()) {

                MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME,
                        getQueryHistoryDao(sqlRequest.getProject()).getRealizationMetricMeasurement(),
                        realizationMetrics.getInfluxdbTags(), realizationMetrics.getInfluxdbFields(),
                        System.currentTimeMillis());
            }
        }

        long duration = TimeUnit.MILLISECONDS.toSeconds(sqlResponse.getDuration());
        String project = sqlRequest.getProject();
        NMetricsGroup.counterInc(NMetricsName.QUERY, NMetricsCategory.PROJECT, project);

        updateQueryTimeMetrics(duration, project);
        updateQueryTypeMetrics(sqlResponse, project);

        NMetricsGroup.counterInc(NMetricsName.QUERY_HOST, NMetricsCategory.HOST, sqlResponse.getServer());
        NMetricsGroup.counterInc(NMetricsName.QUERY_SCAN_BYTES_HOST, NMetricsCategory.HOST, sqlResponse.getServer(), sqlResponse.getTotalScanBytes());

        NMetricsGroup.histogramUpdate(NMetricsName.QUERY_LATENCY, NMetricsCategory.PROJECT, sqlRequest.getProject(),
                sqlResponse.getDuration());
        NMetricsGroup.histogramUpdate(NMetricsName.QUERY_TIME_HOST, NMetricsCategory.HOST, sqlResponse.getServer(), sqlResponse.getDuration());

        NMetricsGroup.histogramUpdate(NMetricsName.QUERY_SCAN_BYTES, NMetricsCategory.PROJECT, project, sqlResponse.getTotalScanBytes());

        super.recordMetric(sqlRequest, sqlResponse);
    }

    private void updateQueryTypeMetrics(SQLResponse sqlResponse, String project) {
        if (sqlResponse.isException()) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_FAILED, NMetricsCategory.PROJECT, project);
            NMetricsGroup.meterMark(NMetricsName.QUERY_FAILED_RATE, NMetricsCategory.PROJECT, project);
        }

        if (sqlResponse.isQueryPushDown()) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_PUSH_DOWN, NMetricsCategory.PROJECT, project);
            NMetricsGroup.meterMark(NMetricsName.QUERY_PUSH_DOWN_RATE, NMetricsCategory.PROJECT, project);
        }

        if (sqlResponse.isTimeout()) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_TIMEOUT, NMetricsCategory.PROJECT, project);
            NMetricsGroup.meterMark(NMetricsName.QUERY_TIMEOUT_RATE, NMetricsCategory.PROJECT, project);
        }

        if (sqlResponse.isHitExceptionCache() || sqlResponse.isStorageCacheUsed()) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_CACHE, NMetricsCategory.PROJECT, project);
        }

        if (sqlResponse.getNativeRealizations() != null) {
            boolean hitAggIndex = sqlResponse.getNativeRealizations().stream().anyMatch(realization -> realization != null && QueryMetricsContext.AGG_INDEX.equals(realization.getIndexType()));
            boolean hitTableIndex = sqlResponse.getNativeRealizations().stream().anyMatch(realization -> realization != null && QueryMetricsContext.TABLE_INDEX.equals(realization.getIndexType()));
            if (hitAggIndex) {
                NMetricsGroup.counterInc(NMetricsName.QUERY_AGG_INDEX, NMetricsCategory.PROJECT, project);
            }
            if (hitTableIndex) {
                NMetricsGroup.counterInc(NMetricsName.QUERY_TABLE_INDEX, NMetricsCategory.PROJECT, project);
            }
        }
    }

    private void updateQueryTimeMetrics(long duration, String project) {
        if (duration <= 1) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_LT_1S, NMetricsCategory.PROJECT, project);
        } else if (duration <= 3) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_1S_3S, NMetricsCategory.PROJECT, project);
        } else if (duration <= 5) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_3S_5S, NMetricsCategory.PROJECT, project);
        } else if (duration <= 10) {
            NMetricsGroup.counterInc(NMetricsName.QUERY_5S_10S, NMetricsCategory.PROJECT, project);
        } else {
            NMetricsGroup.counterInc(NMetricsName.QUERY_SLOW, NMetricsCategory.PROJECT, project);
            NMetricsGroup.meterMark(NMetricsName.QUERY_SLOW_RATE, NMetricsCategory.PROJECT, project);
        }
    }

    public QueryEngineStatisticsResponse getQueryStatisticsByEngine(String project, long startTime, long endTime) {
        Preconditions.checkArgument(project != null && !project.isEmpty());

        return QueryEngineStatisticsResponse
                .valueOf(getQueryHistoryDao(project).getQueryEngineStatistics(startTime, endTime));
    }

    public List<String> format(List<String> sqls) {
        List<Pair<Integer, String>> pairs = Lists.newArrayList();
        int index = 0;
        for (String sql : sqls) {
            pairs.add(Pair.newPair(index, sql));
        }
        return pairs.parallelStream().map(pair -> {
            try {
                val node = SqlParser.create(pair.getSecond()).parseQuery();
                val writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
                writer.setIndentation(2);
                writer.setSelectListExtraIndentFlag(true);
                writer.setSelectListItemsOnSeparateLines(true);
                return Pair.newPair(pair.getFirst(), writer.format(node));
            } catch (SqlParseException e) {
                logger.info("Sql {} cannot be formatted", pair.getSecond());
                return pair;
            }
        }).sorted(Comparator.comparingInt(Pair::getFirst)).map(Pair::getSecond).collect(Collectors.toList());
    }
}
