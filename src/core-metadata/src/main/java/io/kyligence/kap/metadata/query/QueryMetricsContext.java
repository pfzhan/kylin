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

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.kylin.common.QueryTrace.FETCH_RESULT;

public class QueryMetricsContext extends QueryMetrics {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    private static final ThreadLocal<QueryMetricsContext> contexts = new ThreadLocal<>();

    private QueryMetricsContext(String queryId, String defaultServer) {
        super(queryId, defaultServer);
    }

    public static void start(final String queryId, final String defaultServer) {
        if (isStarted()) {
            logger.warn("Query metric context already started in thread named {}", Thread.currentThread().getName());
            return;
        }
        contexts.set(new QueryMetricsContext(queryId, defaultServer));
    }

    public static boolean isStarted() {
        return contexts.get() != null;
    }

    public static QueryMetricsContext collect(final QueryContext context) {
        final QueryMetricsContext current = obtainCurrentQueryMetrics();

        current.doCollect(context);

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

    private void doCollect(final QueryContext context) {
        // set sql
        this.sql = context.getMetrics().getCorrectedSql();
        this.sqlPattern = context.getMetrics().getSqlPattern();
        this.queryTime = context.getMetrics().getQueryStartTime();

        // for query stats
        TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
        LocalDate date = Instant.ofEpochMilli(this.queryTime).atZone(timeZone.toZoneId()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM", Locale.getDefault(Locale.Category.FORMAT));
        this.month = date.withDayOfMonth(1).format(formatter);
        this.queryFirstDayOfMonth = TimeUtil.getMonthStart(this.queryTime);
        this.queryDay = TimeUtil.getDayStart(this.queryTime);
        this.queryFirstDayOfWeek = TimeUtil.getWeekStart(this.queryTime);

        this.submitter = context.getAclInfo().getUsername();

        this.server = context.getMetrics().getServer();

        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            QueryContext.currentTrace().endLastSpan();
            QueryContext.currentTrace().amendLast(FETCH_RESULT, System.currentTimeMillis());
        }
        this.queryDuration = System.currentTimeMillis() - queryTime;
        this.totalScanBytes = context.getMetrics().getTotalScanBytes();
        this.totalScanCount = context.getMetrics().getTotalScanRows();
        this.isPushdown = context.getQueryTagInfo().isPushdown();
        this.isTimeout = context.getQueryTagInfo().isTimeout();
        if (context.getQueryTagInfo().isStorageCacheUsed() && context.getEngineType() != null) {
            this.engineType = context.getEngineType();
        } else {
            if (context.getQueryTagInfo().isPushdown()) {
                this.engineType = context.getPushdownEngine();
            } else if (context.getQueryTagInfo().isConstantQuery()) {
                this.engineType = QueryHistory.EngineType.CONSTANTS.name();
            } else if (!context.getMetrics().isException()) {
                this.engineType = QueryHistory.EngineType.NATIVE.name();
            }
        }

        this.queryStatus = context.getMetrics().isException() ? QueryHistory.QUERY_HISTORY_FAILED
                : QueryHistory.QUERY_HISTORY_SUCCEEDED;

        if (context.getQueryTagInfo().isHitExceptionCache() || context.getQueryTagInfo().isStorageCacheUsed()) {
            this.isCacheHit = true;
        }

        this.resultRowCount = context.getMetrics().getResultRowCount();

        this.isIndexHit = !context.getMetrics().isException() && !context.getQueryTagInfo().isPushdown()
                && !this.engineType.equals(QueryHistory.EngineType.CONSTANTS.name());
        this.projectName = context.getProject();

        collectErrorType(context);
        List<RealizationMetrics> realizationMetricList = collectRealizationMetrics(
                QueryContext.current().getNativeQueryRealizationList());
        updateSecondStorageStatus(context, realizationMetricList);

        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(context.getMetrics().isExactlyMatch(),
                context.getMetrics().getSegCount(),
                Objects.nonNull(this.errorType) && !this.errorType.equals(QueryHistory.NO_REALIZATION_FOUND_ERROR));
        queryHistoryInfo.setRealizationMetrics(realizationMetricList);

        List<List<String>> querySnapshots = new ArrayList<>();
        for (QueryContext.NativeQueryRealization qcReal : QueryContext.current().getNativeQueryRealizationList()) {
            if (CollectionUtils.isEmpty(qcReal.getSnapshots())) {
                continue;
            }
            querySnapshots.add(qcReal.getSnapshots());
        }

        queryHistoryInfo.setQuerySnapshots(querySnapshots);
        this.queryHistoryInfo = queryHistoryInfo;


        this.queryHistoryInfo.setTraces(context.getQueryTrace().spans().stream()
                .map(span -> new QueryHistoryInfo.QueryTraceSpan(span.getName(), span.getGroup(), span.getDuration()))
                .collect(Collectors.toList()));
    }

    public static void updateSecondStorageStatus(final QueryContext context, final List<RealizationMetrics> realizationMetricList) {
        realizationMetricList.forEach(metric -> {
            if (Objects.isNull(metric.getLayoutId())) {
                // When query conditions don't meet segment range, layout id will be null.
                metric.setSecondStorage(false);
            } else {
                metric.setSecondStorage(context.getSecondStorageUsageMap().getOrDefault(Long.parseLong(metric.getLayoutId()), false));
            }
        });
    }

    private void collectErrorType(final QueryContext context) {
        Throwable olapErrorCause = context.getMetrics().getOlapCause();
        Throwable cause = context.getMetrics().getFinalCause();

        while (olapErrorCause != null) {
            if (olapErrorCause instanceof NoRealizationFoundException) {
                this.errorType = QueryHistory.NO_REALIZATION_FOUND_ERROR;
                return;
            }

            if (olapErrorCause instanceof RoutingIndicatorException) {
                this.errorType = QueryHistory.NOT_SUPPORTED_SQL_BY_OLAP_ERROR;
                return;
            }

            olapErrorCause = olapErrorCause.getCause();
        }

        while (cause != null) {
            if (cause instanceof SqlValidatorException || cause instanceof SqlParseException
                    || cause.getClass().getName().contains("ParseException")) {
                this.errorType = QueryHistory.SYNTAX_ERROR;
                return;
            }

            cause = cause.getCause();
        }

        if (context.getMetrics().getFinalCause() != null) {
            this.errorType = QueryHistory.OTHER_ERROR;
        }
    }

    public List<RealizationMetrics> collectRealizationMetrics(List<QueryContext.NativeQueryRealization> queryRealization) {
        List<RealizationMetrics> realizationMetricList = new ArrayList<>();
        if (CollectionUtils.isEmpty(queryRealization)) {
            return realizationMetricList;
        }

        for (QueryContext.NativeQueryRealization realization : queryRealization) {
            RealizationMetrics realizationMetrics = new RealizationMetrics(String.valueOf(realization.getLayoutId()),
                    realization.getIndexType(), realization.getModelId(), realization.getSnapshots());
            realizationMetrics.setQueryId(queryId);
            realizationMetrics.setDuration(queryDuration);
            realizationMetrics.setQueryTime(queryTime);
            realizationMetrics.setProjectName(projectName);
            realizationMetrics.setStreamingLayout(realization.isStreamingLayout());
            realizationMetrics.setSnapshots(realization.getSnapshots());
            realizationMetricList.add(realizationMetrics);

            if (realization.getIndexType() == null)
                continue;

            if (realization.getIndexType().equals(TABLE_INDEX))
                tableIndexUsed = true;

            if (realization.getIndexType().equals(AGG_INDEX))
                aggIndexUsed = true;

            if (realization.getIndexType().equals(TABLE_SNAPSHOT))
                tableSnapshotUsed = true;
        }
        return realizationMetricList;
    }
}