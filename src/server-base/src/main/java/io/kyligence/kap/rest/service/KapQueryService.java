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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.calcite.KEDialect;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.constant.Constant;
import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.metadata.query.QueryMetricsContext;
import lombok.val;

@Component("kapQueryService")
public class KapQueryService extends QueryService {
    private static final Logger logger = LoggerFactory.getLogger("query");

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
    protected void recordMetric(SQLRequest sqlRequest, SQLResponse sqlResponse) throws Throwable {
        if (sqlResponse.isPrepare()) {
            // preparing statement should not be recorded
            return;
        }

        if (QueryMetricsContext.isStarted()) {
            final QueryMetricsContext queryMetricsContext = QueryMetricsContext.collect(QueryContext.current());
            NQueryHistoryScheduler queryHistoryScheduler = NQueryHistoryScheduler.getInstance();
            queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsContext);
        }

        String project = sqlRequest.getProject();

        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), sqlResponse.getServer().concat("-").concat(project));

        MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.PROJECT, project, tags);

        updateQueryTimeMetrics(sqlResponse.getDuration(), project, tags);
        updateQueryTypeMetrics(sqlResponse, project, tags);

        MetricsGroup.counterInc(MetricsName.QUERY_HOST, MetricsCategory.HOST, sqlResponse.getServer());
        MetricsGroup.counterInc(MetricsName.QUERY_SCAN_BYTES_HOST, MetricsCategory.HOST, sqlResponse.getServer(),
                sqlResponse.getTotalScanBytes());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_LATENCY, MetricsCategory.PROJECT, sqlRequest.getProject(), tags,
                sqlResponse.getDuration());
        MetricsGroup.histogramUpdate(MetricsName.QUERY_TIME_HOST, MetricsCategory.HOST, sqlResponse.getServer(),
                sqlResponse.getDuration());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_SCAN_BYTES, MetricsCategory.PROJECT, project, tags,
                sqlResponse.getTotalScanBytes());

        super.recordMetric(sqlRequest, sqlResponse);
    }

    private void updateQueryTypeMetrics(SQLResponse sqlResponse, String project, Map<String, String> tags) {
        if (sqlResponse.isException()) {
            MetricsGroup.counterInc(MetricsName.QUERY_FAILED, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_FAILED_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (sqlResponse.isQueryPushDown()) {
            MetricsGroup.counterInc(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_PUSH_DOWN_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (sqlResponse.isTimeout()) {
            MetricsGroup.counterInc(MetricsName.QUERY_TIMEOUT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_TIMEOUT_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (sqlResponse.isHitExceptionCache() || sqlResponse.isStorageCacheUsed()) {
            MetricsGroup.counterInc(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, project, tags);
        }

        if (sqlResponse.getNativeRealizations() != null) {
            boolean hitAggIndex = sqlResponse.getNativeRealizations().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.AGG_INDEX.equals(realization.getIndexType()));
            boolean hitTableIndex = sqlResponse.getNativeRealizations().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.TABLE_INDEX.equals(realization.getIndexType()));
            if (hitAggIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, project, tags);
            }
            if (hitTableIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, project, tags);
            }
        }
    }

    @VisibleForTesting
    public void updateQueryTimeMetrics(long duration, String project, Map<String, String> tags) {
        if (duration <= Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 3 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 5 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 10 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, project, tags);
        } else {
            MetricsGroup.counterInc(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_SLOW_RATE, MetricsCategory.PROJECT, project, tags);
        }
    }

    public List<String> format(List<String> sqls) {
        List<Pair<Integer, String>> pairs = Lists.newArrayList();
        int index = 0;
        for (String sql : sqls) {
            pairs.add(Pair.newPair(index, sql));
        }
        return pairs.parallelStream().map(pair -> {
            try {
                val node = CalciteParser.parse(pair.getSecond());
                val writer = new SqlPrettyWriter(KEDialect.DEFAULT);
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
