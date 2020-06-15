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

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import io.kyligence.kap.common.metric.QueryMetrics;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.util.QueryPatternUtil;

public class QueryMetricsContext extends QueryMetrics {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv().getInstanceFromEnv();
    public static final String UNKNOWN = "Unknown";

    public static final String AGG_INDEX = "Agg Index";
    public static final String TABLE_INDEX = "Table Index";
    public static final String TABLE_SNAPSHOT = "Table Snapshot";

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
        // set sql
        this.sql = context.getMetrics().getCorrectedSql();

        if(StringUtils.isEmpty(this.sql) && response.isStorageCacheUsed()) {
            String defaultSchema = "DEFAULT";
            try {
                defaultSchema = new QueryExec(request.getProject(), KylinConfig.getInstanceFromEnv()).getSchema();
            } catch (Exception e) {
                logger.warn("Failed to get connection, project: {}", request.getProject(), e);
            }
            QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(request.getProject()), request.getSql(),
                    request.getProject(), request.getLimit(), request.getOffset(), defaultSchema, false);
            queryParams.setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(request.getProject()));
            this.sql = QueryUtil.massageSql(queryParams);
        }
        if(StringUtils.isEmpty(this.sql))
            this.sql = request.getSql();

        this.sqlPattern = QueryPatternUtil.normalizeSQLPattern(this.sql);
        this.queryTime = request.getQueryStartTime();

        // for query stats
        TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
        LocalDate date = Instant.ofEpochMilli(this.queryTime).atZone(timeZone.toZoneId()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        this.month = date.withDayOfMonth(1).format(formatter);
        this.queryFirstDayOfMonth = TimeUtil.getMonthStart(this.queryTime);
        this.queryDay = TimeUtil.getDayStart(this.queryTime);
        this.queryFirstDayOfWeek = TimeUtil.getWeekStart(this.queryTime);

        this.submitter = request.getUsername();

        this.server = response.getServer();
        this.suite = response.getSuite() == null ? UNKNOWN : response.getSuite();

        this.queryDuration = response.getDuration();
        this.totalScanBytes = response.getTotalScanBytes();
        this.totalScanCount = response.getTotalScanRows();
        this.engineType = response.getEngineType();

        if (response.isException())
            this.queryStatus = QueryHistory.QUERY_HISTORY_FAILED;
        else
            this.queryStatus = QueryHistory.QUERY_HISTORY_SUCCEEDED;

        if (response.isHitExceptionCache() || response.isStorageCacheUsed()) {
            this.isCacheHit = true;
        }

        if (response.getResults() != null)
            this.resultRowCount = response.getResults().size();

        this.isIndexHit = !response.isException() && !response.isQueryPushDown()
                && !response.getEngineType().equals(QueryHistory.EngineType.CONSTANTS.name());
        this.projectName = request.getProject();

        collectErrorType(context);
        collectRealizationMetrics(response);

        QueryHistory.RecordInfo recordInfo = new QueryHistory.RecordInfo(context.getMetrics().getExactlyMatch(),
                context.getMetrics().getSegCount());
        try {
            this.recordInfo = JsonUtil.writeValueAsString(recordInfo);
        } catch (Exception e) {
            logger.info("Fail to collect query record info");
            this.recordInfo = "";
        }
    }

    private void collectErrorType(final QueryContext context) {
        Throwable olapErrorCause = context.getMetrics().getOlapCause();
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

        Throwable cause = context.getMetrics().getFinalCause();
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



    private void collectRealizationMetrics(final SQLResponse response) {
        if (CollectionUtils.isEmpty(response.getNativeRealizations())) {
            return;
        }

        StringBuilder realizationSb = new StringBuilder();

        for (NativeQueryRealization realization : response.getNativeRealizations()) {
            RealizationMetrics realizationMetrics = new RealizationMetrics(String.valueOf(realization.getLayoutId()),
                    realization.getIndexType(), realization.getModelId());
            realizationMetrics.setQueryId(queryId);
            realizationMetrics.setDuration(queryDuration);
            realizationMetrics.setSuite(suite);
            realizationMetrics.setQueryTime(queryTime);
            realizationMetrics.setProjectName(projectName);
            this.realizationMetrics.add(realizationMetrics);
            // example: modelId#layoutid#indexType
            realizationSb.append(realizationMetrics.getModelId() + "#" + realizationMetrics.getLayoutId() + "#"
                    + realizationMetrics.getIndexType() + ",");

            if (realization.getIndexType().equals(QueryMetricsContext.TABLE_INDEX))
                tableIndexUsed = true;

            if (realization.getIndexType().equals(QueryMetricsContext.AGG_INDEX))
                aggIndexUsed = true;

            if (realization.getIndexType().equals(QueryMetricsContext.TABLE_SNAPSHOT))
                tableSnapshotUsed = true;
        }

        this.realizations = realizationSb.substring(0, realizationSb.length() - 1);
    }

    public Map<String, String> getInfluxdbTags() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String> builder() //
                .put(QueryHistory.SUBMITTER, submitter) //
                .put(QueryHistory.SUITE, suite) //
                .put(QueryHistory.IS_INDEX_HIT, String.valueOf(isIndexHit))
                .put(QueryHistory.MONTH, month)
                .put(QueryHistory.IS_TABLE_INDEX_USED, String.valueOf(tableIndexUsed))
                .put(QueryHistory.IS_AGG_INDEX_USED, String.valueOf(aggIndexUsed))
                .put(QueryHistory.IS_TABLE_SNAPSHOT_USED, String.valueOf(tableSnapshotUsed));

        if (StringUtils.isBlank(server)) {
            server = defaultServer;
        }
        builder.put(QueryHistory.QUERY_SERVER, server);

        if (StringUtils.isNotBlank(this.errorType)) {
            builder.put(QueryHistory.ERROR_TYPE, errorType);
        } else {
            builder.put(QueryHistory.ERROR_TYPE, "");
        }

        if (StringUtils.isNotBlank(this.engineType)) {
            builder.put(QueryHistory.ENGINE_TYPE, this.engineType);
        } else {
            builder.put(QueryHistory.ENGINE_TYPE, "");
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

        if (StringUtils.isNotEmpty(this.realizations)) {
            builder.put(QueryHistory.REALIZATIONS, this.realizations);
        } else {
            builder.put(QueryHistory.REALIZATIONS, "");
        }

        return builder.build();
    }
}
