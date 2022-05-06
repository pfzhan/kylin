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

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.kylin.common.util.JsonUtil;

@SuppressWarnings("serial")
@Getter
@Setter
@Slf4j
public class QueryHistory {
    public static final String ADJ_SLOW = "Slow";
    public static final String QUERY_HISTORY_SUCCEEDED = "SUCCEEDED";
    public static final String QUERY_HISTORY_FAILED = "FAILED";
    public static final String DEFAULT_DATABASE = "KE_HISTORY";

    // database name
    public static final String DB_NAME = DEFAULT_DATABASE;

    // table names
    public static final String QUERY_MEASUREMENT_SURFIX = "query_history";
    public static final String REALIZATION_MEASUREMENT_SURFIX = "query_history_realization";

    public static final String QUERY_HISTORY_ID = "id";
    public static final String QUERY_HISTORY_INFO = "query_history_info";
    public static final String QUERY_REALIZATIONS_METRICS = "realization_metrics";
    public static final String PROJECT_NAME = "project_name";
    public static final String QUERY_ID = "query_id";
    public static final String SQL_TEXT = "sql_text";
    public static final String SQL_PATTERN = "sql_pattern";
    public static final String QUERY_DURATION = "duration";
    public static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
    public static final String TOTAL_SCAN_COUNT = "total_scan_count";
    public static final String RESULT_ROW_COUNT = "result_row_count";
    public static final String SUBMITTER = "submitter";
    public static final String REALIZATIONS = "realizations";
    public static final String QUERY_SERVER = "server";
    public static final String SUITE = "suite";
    public static final String ERROR_TYPE = "error_type";
    public static final String ENGINE_TYPE = "engine_type";
    public static final String IS_CACHE_HIT = "cache_hit";
    public static final String QUERY_STATUS = "query_status";
    public static final String IS_INDEX_HIT = "index_hit";
    public static final String QUERY_TIME = "query_time";
    public static final String MONTH = "month";
    public static final String QUERY_FIRST_DAY_OF_MONTH = "query_first_day_of_month";
    public static final String QUERY_FIRST_DAY_OF_WEEK = "query_first_day_of_week";
    public static final String QUERY_DAY = "query_day";
    public static final String IS_TABLE_INDEX_USED = "is_table_index_used";
    public static final String IS_AGG_INDEX_USED = "is_agg_index_used";
    public static final String IS_TABLE_SNAPSHOT_USED = "is_table_snapshot_used";
    public static final String RESERVED_FIELD_3 = "reserved_field_3";

    public static final String MODEL = "model";
    public static final String LAYOUT_ID = "layout_id";
    public static final String INDEX_TYPE = "index_type";

    // error types
    public static final String SYNTAX_ERROR = "Syntax error";
    public static final String NO_REALIZATION_FOUND_ERROR = "No realization found";
    public static final String NOT_SUPPORTED_SQL_BY_OLAP_ERROR = "Not Supported By OLAP SQL";
    public static final String OTHER_ERROR = "Other error";
    public static final String USER_STOP_QUERY_ERROR = "Stopped By User";

    @JsonProperty(SQL_TEXT)
    private String sql;

    @JsonProperty(SQL_PATTERN)
    private String sqlPattern;

    @JsonProperty(QUERY_TIME)
    private long queryTime;

    @JsonProperty(QUERY_DURATION)
    private long duration;

    // this field is composed of modelId, layout id and index type
    // it's written as modelId#layoutId#indexType
    // This way to serialized query realizations had been deprecated. See KE-20697
    private String queryRealizations;

    @JsonProperty(QUERY_SERVER)
    private String hostName;

    @JsonProperty(SUBMITTER)
    private String querySubmitter;

    @JsonProperty(QUERY_STATUS)
    private String queryStatus;

    //query details
    @JsonProperty(QUERY_ID)
    private String queryId;

    @JsonProperty(QUERY_HISTORY_ID)
    private long id;

    @JsonProperty(TOTAL_SCAN_COUNT)
    private long totalScanCount;

    @JsonProperty(TOTAL_SCAN_BYTES)
    private long totalScanBytes;

    @JsonProperty(RESULT_ROW_COUNT)
    private long resultRowCount;

    @JsonProperty(IS_CACHE_HIT)
    private boolean cacheHit;

    @JsonProperty(IS_INDEX_HIT)
    private boolean indexHit;

    @JsonProperty(ENGINE_TYPE)
    private String engineType;

    @JsonProperty(PROJECT_NAME)
    private String projectName;

    @JsonProperty(REALIZATIONS)
    private List<NativeQueryRealization> nativeQueryRealizations;

    private int count;

    @JsonProperty(ERROR_TYPE)
    private String errorType;

    @JsonProperty(QUERY_HISTORY_INFO)
    private QueryHistoryInfo queryHistoryInfo;
    // only for test
    private transient long insertTime;

    public QueryHistory() {
    }

    public QueryHistory(String sqlPattern, String queryStatus, String querySubmitter, long queryTime, long duration) {
        this.sqlPattern = sqlPattern;
        this.queryStatus = queryStatus;
        this.querySubmitter = querySubmitter;
        this.queryTime = queryTime;
        this.duration = duration;
    }

    public QueryHistory(String sql) {
        this.sql = sql;
    }

    public QueryHistorySql getQueryHistorySql() {
        if (JsonUtil.isJson(sql)) {
            try {
                return JsonUtil.readValue(sql, QueryHistorySql.class);
            } catch (IOException e) {
                log.error("Convert sql json string failed", e);
            }
        }
        return new QueryHistorySql(sql, sql, null);
    }

    public boolean isException() {
        return queryStatus.equals(QUERY_HISTORY_FAILED);
    }

    public List<NativeQueryRealization> transformRealizations() {
        List<NativeQueryRealization> realizations = Lists.newArrayList();
        if (queryHistoryInfo == null || queryHistoryInfo.getRealizationMetrics() == null
                || queryHistoryInfo.getRealizationMetrics().isEmpty()) {
            return transformStringRealizations();
        }

        List<QueryMetrics.RealizationMetrics> realizationMetrics = queryHistoryInfo.realizationMetrics;

        for (QueryMetrics.RealizationMetrics metrics : realizationMetrics) {
            val realization = new NativeQueryRealization(metrics.modelId,
                    metrics.layoutId == null || metrics.layoutId.equals("null") ? null : Long.parseLong(metrics.layoutId),
                    metrics.indexType == null || metrics.indexType.equals("null") ? null : metrics.indexType,
                    metrics.snapshots == null || metrics.snapshots.isEmpty() ? Lists.newArrayList() : metrics.snapshots);
            realization.setSecondStorage(metrics.isSecondStorage);
            realization.setStreamingLayout(metrics.isStreamingLayout);
            realizations.add(realization);
        }
        return realizations;
    }

    // This way to serialized query realizations had been deprecated. See KE-20697
    // Just for compatibility with previous versions
    public List<NativeQueryRealization> transformStringRealizations() {
        List<NativeQueryRealization> realizations = Lists.newArrayList();

        if (StringUtils.isEmpty(this.queryRealizations))
            return realizations;

        String[] queryRealizations;
        Pattern p = Pattern.compile("\\[.*?\\]+");
        if (p.matcher(this.queryRealizations).find()) {
            queryRealizations = this.queryRealizations.split(";");
        } else {
            queryRealizations = this.queryRealizations.split(",");
        }
        for (String realization : queryRealizations) {
            String[] info = realization.split("#");
            transformStringRealizations(info, realizations);
        }

        return realizations;
    }

    private void transformStringRealizations(String[] info, List<NativeQueryRealization> realizations) {
        List<String> snapshots = Lists.newArrayList();
        if (info.length > 3) {
            if (!info[3].equals("[]")) {
                snapshots.addAll(Lists.newArrayList(info[3].substring(1, info[3].length() - 1).split(",\\s*")));
            }
            realizations.add(new NativeQueryRealization(info[0],
                    info[1].equalsIgnoreCase("null") ? null : Long.parseLong(info[1]),
                    info[2].equalsIgnoreCase("null") ? null : info[2],
                    info[3].equalsIgnoreCase("null") ? null : snapshots));
        } else {
            realizations.add(new NativeQueryRealization(info[0],
                    info[1].equalsIgnoreCase("null") ? null : Long.parseLong(info[1]),
                    info[2].equalsIgnoreCase("null") ? null : info[2], snapshots));
        }
    }

    public enum EngineType {
        NATIVE, CONSTANTS, RDBMS, HIVE
    }

    public enum CacheType {
        EHCACHE, REDIS
    }
}
