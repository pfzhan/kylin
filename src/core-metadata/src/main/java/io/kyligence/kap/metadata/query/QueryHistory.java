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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import java.util.List;

@SuppressWarnings("serial")
@Getter
@Setter
public class QueryHistory {
    public static final String ADJ_SLOW = "Slow";
    public static final String QUERY_HISTORY_SUCCEEDED = "SUCCEEDED";
    public static final String QUERY_HISTORY_FAILED = "FAILED";

    // database name
    public static final String DB_NAME = InfluxDBWriter.DEFAULT_DATABASE;

    // table names
    public static final String QUERY_MEASUREMENT_SURFIX = "query_history";
    public static final String REALIZATION_MEASUREMENT_SURFIX = "query_history_realization";

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
    public static final String QUERY_MONTH = "month";
    public static final String IS_TABLE_INDEX_USED = "is_table_index_used";
    public static final String IS_AGG_INDEX_USED = "is_agg_index_used";
    public static final String IS_TABLE_SNAPSHOT_USED = "is_table_snapshot_used";

    public static final String MODEL = "model";
    public static final String LAYOUT_ID = "layout_id";
    public static final String INDEX_TYPE = "index_type";

    // error types
    public static final String SYNTAX_ERROR = "Syntax error";
    public static final String NO_REALIZATION_FOUND_ERROR = "No realization found";
    public static final String OTHER_ERROR = "Other error";

    @JsonProperty(SQL_TEXT)
    @Column(name = SQL_TEXT)
    private String sql;

    @JsonProperty(SQL_PATTERN)
    @Column(name = SQL_PATTERN)
    private String sqlPattern;

    @JsonProperty(QUERY_TIME)
    @Column(name = QUERY_TIME)
    private long queryTime;

    @JsonProperty(QUERY_DURATION)
    @Column(name = QUERY_DURATION)
    private long duration;

    // this field is composed of modelId, layout id and index type
    // it's written as modelId#layoutId#indexType
    @Column(name = REALIZATIONS)
    private String queryRealizations;

    @JsonProperty(QUERY_SERVER)
    @Column(name = QUERY_SERVER, tag = true)
    private String hostName;

    @JsonProperty(SUBMITTER)
    @Column(name = SUBMITTER, tag = true)
    private String querySubmitter;

    @JsonProperty(QUERY_STATUS)
    @Column(name = QUERY_STATUS)
    private String queryStatus;

    //query details
    @JsonProperty(QUERY_ID)
    @Column(name = QUERY_ID)
    private String queryId;

    @JsonProperty(TOTAL_SCAN_COUNT)
    @Column(name = TOTAL_SCAN_COUNT)
    private long totalScanCount;

    @JsonProperty(TOTAL_SCAN_BYTES)
    @Column(name = TOTAL_SCAN_BYTES)
    private long totalScanBytes;

    @JsonProperty(RESULT_ROW_COUNT)
    @Column(name = RESULT_ROW_COUNT)
    private long resultRowCount;

    @JsonProperty(IS_CACHE_HIT)
    @Column(name = IS_CACHE_HIT)
    private boolean cacheHit;

    @JsonProperty(IS_INDEX_HIT)
    @Column(name = IS_INDEX_HIT)
    private boolean indexHit;

    @JsonProperty(ENGINE_TYPE)
    @Column(name = ENGINE_TYPE)
    private String engineType;

    @JsonProperty("realizations")
    private List<NativeQueryRealization> nativeQueryRealizations;

    @Column(name = "count")
    private int count;

    @JsonProperty(ERROR_TYPE)
    @Column(name = ERROR_TYPE)
    private String errorType;

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

    public boolean isException() {
        return queryStatus.equals(QUERY_HISTORY_FAILED);
    }

    public List<NativeQueryRealization> transformRealizations() {
        List<NativeQueryRealization> realizations = Lists.newArrayList();

        if (StringUtils.isEmpty(this.queryRealizations))
            return realizations;

        for (String realization : this.queryRealizations.split(",")) {
            String[] info = realization.split("#");

            realizations.add(new NativeQueryRealization(info[0], Long.valueOf(info[1]), info[2]));
        }

        return realizations;
    }

}
