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

import com.google.common.base.Preconditions;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.metadata.query.QueryStatisticsResponse;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.metric.MetricWriterStrategy;

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
        if (QueryMetricsContext.isStarted()) {
            final QueryMetricsContext queryMetricsContext = QueryMetricsContext.collect(sqlRequest, sqlResponse,
                    QueryContext.current());

            MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME, getQueryHistoryDao(sqlRequest.getProject()).getQueryMetricMeasurement(),
                    queryMetricsContext.getInfluxdbTags(), queryMetricsContext.getInfluxdbFields(),
                    System.currentTimeMillis());

            for (final QueryMetricsContext.RealizationMetrics realizationMetrics : queryMetricsContext
                    .getRealizationMetrics()) {

                MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME, getQueryHistoryDao(sqlRequest.getProject()).getRealizationMetricMeasurement(),
                        realizationMetrics.getInfluxdbTags(), realizationMetrics.getInfluxdbFields(),
                        System.currentTimeMillis());
            }
        }

        super.recordMetric(sqlRequest, sqlResponse);
    }

    public QueryStatisticsResponse getQueryStatistics(String project, long startTime, long endTime) {
        Preconditions.checkArgument(project != null && !project.isEmpty());

        return QueryStatisticsResponse.valueOf(getQueryHistoryDao(project).getQueryStatistics(startTime, endTime));
    }
}
