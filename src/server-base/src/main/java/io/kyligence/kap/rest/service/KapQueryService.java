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

import io.kyligence.kap.common.metric.MetricWriter;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import io.kyligence.kap.shaded.influxdb.org.influxdb.impl.InfluxDBResultMapper;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.msg.MsgPicker;
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

    private static final String STATISTICS_SQL = "SELECT COUNT(query_id), MEAN(\"duration\") FROM query_metric WHERE (time >= %dms AND time <= %dms) AND error_type = '' GROUP BY engine_type";

    private final KapConfig kapConfig = KapConfig.getInstanceFromEnv();

    private volatile InfluxDB influxDB;

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

            MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME, QueryHistory.QUERY_MEASUREMENT,
                    queryMetricsContext.getInfluxdbTags(), queryMetricsContext.getInfluxdbFields(),
                    System.currentTimeMillis());

            for (final QueryMetricsContext.RealizationMetrics realizationMetrics : queryMetricsContext
                    .getRealizationMetrics()) {

                MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME, QueryHistory.REALIZATION_MEASUREMENT,
                        realizationMetrics.getInfluxdbTags(), realizationMetrics.getInfluxdbFields(),
                        System.currentTimeMillis());
            }
        }

        super.recordMetric(sqlRequest, sqlResponse);
    }

    public QueryStatisticsResponse getQueryStatistics(long startTime, long endTime) {
        if (!MetricWriter.Type.INFLUX.name().equals(kapConfig.diagnosisMetricWriterType())) {
            throw new IllegalStateException(MsgPicker.getMsg().getNOT_SET_INFLUXDB());
        }

        final String statisticsQuery = String.format(STATISTICS_SQL, startTime, endTime);

        final QueryResult result = getInfluxDB().query(new Query(statisticsQuery, QueryHistory.DB_NAME));
        final InfluxDBResultMapper mapper = new InfluxDBResultMapper();
        return QueryStatisticsResponse.valueOf(mapper.toPOJO(result, QueryStatisticsResponse.QueryStatistics.class));
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
}
