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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.util.QueryPatternUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class QueryMetricsContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv().getInstanceFromEnv();
    private static final String LOG_METRIC = "log";
    public static final String UNKNOWN = "Unknown";

    private static final InheritableThreadLocal<QueryMetricsContext> contexts = new InheritableThreadLocal<>();

    private final String queryId;
    private long queryTime;

    private String sql;
    private String sqlPattern;

    private String submitter;
    private String project;
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

    private final List<RealizationMetrics> realizationMetrics = new ArrayList<>();

    private String log;

    private QueryMetricsContext(String queryId) {
        this.queryId = queryId;
    }

    public static void start(final String queryId) {
        if (!isCollectEnabled()) {
            logger.warn(
                    "Can't to start QueryMetricsContext, please set kap.metric.diagnosis.graph-writer-type to 'INFLUX'");
            return;
        }

        if (isStarted()) {
            logger.warn("Query metric context already started in thread named {}", Thread.currentThread().getName());
            return;
        }
        contexts.set(new QueryMetricsContext(queryId));
    }

    private static boolean isCollectEnabled() {
        return "INFLUX".equals(kapConfig.diagnosisMetricWriterType());
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

    public static void log(final String log) {
        obtainCurrentQueryMetrics().log = log;
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
        this.sql = request.getSql();
        try {
            this.sqlPattern = QueryPatternUtil.normalizeSQLPattern(sql);
        } catch (SqlParseException e) {
            logger.error("Caught sql parse error", e);
            throw new RuntimeException(e);
        }
        this.queryTime = QueryContext.current().getQueryStartMillis();

        this.submitter = request.getUsername();
        this.project = request.getProject();

        this.hostname = response.getServer();
        this.suite = response.getSuite();

        this.queryDuration = response.getDuration();
        this.totalScanBytes = response.getTotalScanBytes();
        this.totalScanCount = response.getTotalScanCount();

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

        if (response.getIsException() || response.isPushDown()) {
            this.accelerateStatus = QueryHistory.QUERY_HISTORY_UNACCELERATED;
        } else
            this.accelerateStatus = QueryHistory.QUERY_HISTORY_ACCELERATED;

        collectErrorType(context);
        collectRealizationMetrics(response, context);
        collectEngineTypeAndAnsweredBy(response, context);

        logger.debug("Query[{}] collect metrics {}, {}, {}, {}, {}, {}, {}, {}, {}", queryId, sql, submitter, project,
                hostname, suite, queryDuration, totalScanBytes, errorType, engineType);
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

    private void collectRealizationMetrics(final SQLResponse response, final QueryContext context) {
        if (response.isHitExceptionCache() || null == OLAPContext.getThreadLocalContexts()) {
            logger.debug("Query[{}] hit cache or can't find OLAPContext.", context.getQueryId());
            return;
        }

        for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
            if (ctx.realization != null) {
                final String realizationType = ctx.storageContext.getCandidate().getCuboidLayout().getCuboidDesc()
                        .isTableIndex() ? "Table Index" : "Agg Index";
                addRealizationMetrics(ctx.storageContext.getCuboidId().toString(), realizationType,
                        ctx.realization.getModel().getName());
            }
        }
    }

    private void addRealizationMetrics(String realizationName, String realizationType, String modelName) {
        realizationMetrics
                .add(new RealizationMetrics(queryId, suite, project, realizationName, realizationType, modelName));
        logger.debug("Query[{}] hit project [{}], model [{}], realization name [{}], realization type [{}]", queryId,
                project, modelName, realizationName, realizationType);
    }

    private void collectEngineTypeAndAnsweredBy(final SQLResponse response, final QueryContext context) {
        if (response.isPushDown()) {
            this.engineType = context.getPushdownEngine();
            this.answeredBy = context.getPushdownEngine();
        } else if (!realizationMetrics.isEmpty()) {
            this.engineType = realizationMetrics.iterator().next().realizationType;
            final Collection<String> modelNames = Collections2.transform(realizationMetrics,
                    new Function<RealizationMetrics, String>() {
                        @Override
                        public String apply(RealizationMetrics input) {
                            return input.modelName;
                        }
                    });
            this.answeredBy = Joiner.on(",").join(modelNames);
        } else {
            this.engineType = UNKNOWN;
            this.answeredBy = UNKNOWN;
        }
    }

    public Map<String, String> getInfluxdbTags() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String> builder() //
                .put(QueryHistory.SUBMITTER, submitter) //
                .put(QueryHistory.PROJECT, project) //
                .put(QueryHistory.SUITE, suite == null ? UNKNOWN : suite) //
                .put(QueryHistory.ENGINE_TYPE, engineType)
                .put(QueryHistory.ANSWERED_BY, answeredBy)
                .put(QueryHistory.IS_CUBE_HIT, String.valueOf(isCubeHit))
                .put(QueryHistory.ACCELERATE_STATUS, accelerateStatus);

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
                .put(QueryHistory.QUERY_DURATION, queryDuration)
                .put(QueryHistory.TOTAL_SCAN_BYTES, totalScanBytes)
                .put(QueryHistory.TOTAL_SCAN_COUNT, totalScanCount)
                .put(QueryHistory.RESULT_ROW_COUNT, resultRowCount)
                .put(QueryHistory.IS_CACHE_HIT, isCacheHit)
                .put(QueryHistory.QUERY_STATUS, queryStatus)
                .put(QueryHistory.QUERY_TIME, queryTime)
                .put(QueryHistory.SQL_PATTERN, sqlPattern);
        
        if (!realizationMetrics.isEmpty()) {
            final Collection<String> realizations = Collections2.transform(realizationMetrics,
                    new Function<RealizationMetrics, String>() {
                        @Override
                        public String apply(RealizationMetrics input) {
                            return input.realizationName;
                        }
                    });

            builder.put(QueryHistory.REALIZATIONS, Joiner.on(",").join(realizations));
        }

        if (StringUtils.isNotBlank(this.log)) {
            builder.put(LOG_METRIC, log);
        }

        return builder.build();
    }

    public static class RealizationMetrics {

        private String queryId;

        private String suite;

        private String project;

        private String realizationName;

        private String realizationType;

        private String modelName;

        public RealizationMetrics(String queryId, String suite, String project, String realizationName,
                String realizationType, String modelName) {
            this.queryId = queryId;
            this.suite = suite == null ? UNKNOWN : suite;
            this.project = project;
            this.realizationName = realizationName;
            this.realizationType = realizationType;
            this.modelName = modelName;
        }

        public Map<String, String> getInfluxdbTags() {
            return ImmutableMap.<String, String> builder() //
                    .put(QueryHistory.SUITE, suite) //
                    .put(QueryHistory.PROJECT, project) //
                    .put(QueryHistory.MODEL, modelName) //
                    .put(QueryHistory.REALIZATION_NAME, realizationName) //
                    .put(QueryHistory.REALIZATION_TYPE, realizationType) //
                    .build();
        }

        public Map<String, Object> getInfluxdbFields() {
            return ImmutableMap.<String, Object> builder().put(QueryHistory.QUERY_ID, queryId).build();
        }
    }
}
