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
package io.kyligence.kap.rest.metrics;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.util.QueryPatternUtil;
import lombok.Getter;
import lombok.Setter;

public class QueryMetricsContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv().getInstanceFromEnv();
    public static final String UNKNOWN = "Unknown";

    public static final String AGG_INDEX = "Agg Index";
    public static final String TABLE_INDEX = "Table Index";
    public static final String TABLE_SNAPSHOT = "Table Snapshot";

    private static final InheritableThreadLocal<QueryMetricsContext> contexts = new InheritableThreadLocal<>();

    private final String queryId;
    private long queryTime;

    private String sql;
    private String sqlPattern;

    private String submitter;
    private String hostname;
    private String suite;

    private long queryDuration;
    private long totalScanBytes;
    private long totalScanCount;
    private long resultRowCount;

    private String errorType;

    private String engineType;

    private boolean isCacheHit;
    private boolean isCubeHit;
    private String queryStatus;
    private String accelerateStatus;
    private String answeredBy;
    private String month;

    private final List<RealizationMetrics> realizationMetrics = new ArrayList<>();

    private QueryMetricsContext(String queryId) {
        this.queryId = queryId;
    }

    public static void start(final String queryId) {
        if (!isCollectEnabled()) {
            logger.warn(
                    "Can't to start QueryMetricsContext, please set kap.metric.write-destination to 'INFLUX'");
            return;
        }

        if (isStarted()) {
            logger.warn("Query metric context already started in thread named {}", Thread.currentThread().getName());
            return;
        }
        contexts.set(new QueryMetricsContext(queryId));
    }

    private static boolean isCollectEnabled() {
        return "INFLUX".equals(kapConfig.getMetricWriteDest());
    }

    public static boolean isStarted() {
        return contexts.get() != null;
    }

    public static QueryMetricsContext collect(final SQLRequest request, final SQLResponse response,
            final QueryContext context) {
        final QueryMetricsContext current = obtainCurrentQueryMetrics();

        current.doCollect(request, response, context);

        return current;
    }

    public static void reset() {
        contexts.remove();
    }

    private static QueryMetricsContext obtainCurrentQueryMetrics() {
        QueryMetricsContext current = null;
        Preconditions.checkState((current = contexts.get()) != null, "Query metric context is not started.");
        return current;
    }

    private void doCollect(final SQLRequest request, final SQLResponse response, final QueryContext context) {
        String correctedSql = QueryContext.current().getCorrectedSql();

        // this case happens when a query hit cache, the process did not proceed to the place where massaged sql is set
        if (correctedSql == null) {
            correctedSql = QueryUtil.massageSql(request.getSql(), request.getProject(), request.getLimit(),
                    request.getOffset(), null);
        }

        this.sql = correctedSql;

        this.sqlPattern = QueryPatternUtil.normalizeSQLPattern(this.sql);

        this.queryTime = QueryContext.current().getQueryStartMillis();

        // for query stats
        TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
        LocalDate date = Instant.ofEpochMilli(this.queryTime).atZone(timeZone.toZoneId()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        this.month = date.withDayOfMonth(1).format(formatter);

        this.submitter = request.getUsername();

        this.hostname = response.getServer();
        this.suite = response.getSuite() == null ? UNKNOWN : response.getSuite();

        this.queryDuration = response.getDuration();
        this.totalScanBytes = response.getTotalScanBytes();
        this.totalScanCount = response.getTotalScanCount();
        this.answeredBy = response.getAnsweredBy() == null ? UNKNOWN : Joiner.on(",").join(response.getAnsweredBy());
        this.engineType = response.getEngineType() == null ? UNKNOWN : response.getEngineType();

        if (response.getIsException())
            this.queryStatus = QueryHistory.QUERY_HISTORY_FAILED;
        else
            this.queryStatus = QueryHistory.QUERY_HISTORY_SUCCEEDED;

        if (response.isHitExceptionCache() || response.isStorageCacheUsed()) {
            this.isCacheHit = true;
        }

        if (response.getResults() != null)
            this.resultRowCount = response.getResults().size();

        if (response.isPushDown())
            this.isCubeHit = false;
        else
            this.isCubeHit = true;

        if (response.getIsException() || response.isPushDown()) {
            this.accelerateStatus = QueryHistory.QUERY_HISTORY_UNACCELERATED;
        } else
            this.accelerateStatus = QueryHistory.QUERY_HISTORY_ACCELERATED;

        collectErrorType(context);
        collectRealizationMetrics(response);

        logger.debug("Query[{}] collect metrics {}, {}, {}, {}, {}, {}, {}, {}", queryId, sql, submitter, hostname,
                suite, queryDuration, totalScanBytes, errorType, engineType);
    }

    private void collectErrorType(final QueryContext context) {
        Throwable cause = context.getErrorCause();
        while (cause != null) {
            if (cause instanceof SqlValidatorException || cause instanceof SqlParseException
                    || cause.getClass().getName().contains("ParseException")) {
                this.errorType = "Syntax error";
                return;

            } else if (cause instanceof NoRealizationFoundException) {
                this.errorType = "No realization found";
                return;
            }

            cause = cause.getCause();
        }

        if (context.getErrorCause() != null) {
            this.errorType = "Other error";
        }
    }

    public List<RealizationMetrics> getRealizationMetrics() {
        return ImmutableList.copyOf(realizationMetrics);
    }

    private void collectRealizationMetrics(final SQLResponse response) {
        if (response.getRealizationMetrics() == null)
            return;

        for (RealizationMetrics singleMetric : response.getRealizationMetrics()) {
            singleMetric.setQueryId(queryId);
            singleMetric.setDuration(queryDuration);
            singleMetric.setSuite(suite);
            this.realizationMetrics.add(singleMetric);
        }
    }

    public Map<String, String> getInfluxdbTags() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String> builder() //
                .put(QueryHistory.SUBMITTER, submitter) //
                .put(QueryHistory.SUITE, suite) //
                .put(QueryHistory.ENGINE_TYPE, engineType).put(QueryHistory.ANSWERED_BY, answeredBy)
                .put(QueryHistory.IS_CUBE_HIT, String.valueOf(isCubeHit))
                .put(QueryHistory.ACCELERATE_STATUS, accelerateStatus).put(QueryHistory.QUERY_MONTH, month);

        if (StringUtils.isBlank(hostname)) {
            try {
                hostname = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                hostname = UNKNOWN;
            }
        }
        builder.put(QueryHistory.QUERY_HOSTNAME, hostname);

        if (StringUtils.isNotBlank(this.errorType)) {
            builder.put(QueryHistory.ERROR_TYPE, errorType);
        }

        return builder.build();
    }

    public Map<String, Object> getInfluxdbFields() {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object> builder() //
                .put(QueryHistory.SQL_TEXT, sql) //
                .put(QueryHistory.QUERY_ID, queryId) //
                .put(QueryHistory.QUERY_DURATION, queryDuration).put(QueryHistory.TOTAL_SCAN_BYTES, totalScanBytes)
                .put(QueryHistory.TOTAL_SCAN_COUNT, totalScanCount).put(QueryHistory.RESULT_ROW_COUNT, resultRowCount)
                .put(QueryHistory.IS_CACHE_HIT, isCacheHit).put(QueryHistory.QUERY_STATUS, queryStatus)
                .put(QueryHistory.QUERY_TIME, queryTime).put(QueryHistory.SQL_PATTERN, sqlPattern);

        if (!realizationMetrics.isEmpty()) {
            final Collection<String> realizations = Collections2.transform(realizationMetrics,
                    new Function<RealizationMetrics, String>() {
                        @Override
                        public String apply(RealizationMetrics input) {
                            return input.cuboidLayoutId;
                        }
                    });

            builder.put(QueryHistory.REALIZATIONS, Joiner.on(",").join(realizations));
        }

        return builder.build();
    }

    public static RealizationMetrics createRealizationMetrics(String cuboidLayoutId, String realizationType,
            String modelId) {
        return new RealizationMetrics(cuboidLayoutId, realizationType, modelId);
    }

    @Getter
    @Setter
    public static class RealizationMetrics implements Serializable {

        private String queryId;

        private long duration;

        private String suite;

        private String cuboidLayoutId;

        private String realizationType;

        private String modelId;

        public RealizationMetrics(String cuboidLayoutId, String realizationType, String modelId) {
            this.cuboidLayoutId = cuboidLayoutId;
            this.realizationType = realizationType;
            this.modelId = modelId;
        }

        public Map<String, String> getInfluxdbTags() {
            return ImmutableMap.<String, String> builder() //
                    .put(QueryHistory.SUITE, suite) //
                    .put(QueryHistory.MODEL, modelId) //
                    .put(QueryHistory.LAYOUT_ID, cuboidLayoutId) //
                    .put(QueryHistory.REALIZATION_TYPE, realizationType) //
                    .build();
        }

        public Map<String, Object> getInfluxdbFields() {
            return ImmutableMap.<String, Object> builder().put(QueryHistory.QUERY_ID, queryId)
                    .put(QueryHistory.QUERY_DURATION, duration).build();
        }
    }
}
