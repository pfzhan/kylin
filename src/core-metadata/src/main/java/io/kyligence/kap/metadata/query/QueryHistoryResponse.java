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


import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@Getter
@Setter
public class QueryHistoryResponse {

    public static final String QUERY_HISTORY_ID = "id";
    public static final String QUERY_HISTORY_INFO = "query_history_info";
    public static final String SQL_TEXT = "sql_text";
    public static final String QUERY_TIME = "query_time";
    public static final String QUERY_DURATION = "duration";
    public static final String QUERY_SERVER = "server";
    public static final String SUBMITTER = "submitter";
    public static final String QUERY_STATUS = "query_status";
    public static final String QUERY_ID = "query_id";
    public static final String TOTAL_SCAN_COUNT = "total_scan_count";
    public static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
    public static final String RESULT_ROW_COUNT = "result_row_count";
    public static final String IS_CACHE_HIT = "cache_hit";
    public static final String IS_INDEX_HIT = "index_hit";
    public static final String ENGINE_TYPE = "engine_type";
    public static final String PROJECT_NAME = "project_name";
    public static final String REALIZATIONS = "realizations";
    public static final String ERROR_TYPE = "error_type";

    //query details
    @JsonProperty(QUERY_ID)
    private String queryId;

    @JsonProperty(QUERY_HISTORY_INFO)
    private QueryHistoryInfoResponse queryHistoryInfo;

    @JsonProperty(SQL_TEXT)
    private String sql;

    @JsonProperty(QUERY_TIME)
    private long queryTime;

    @JsonProperty(QUERY_DURATION)
    private long duration;

    @JsonProperty(QUERY_SERVER)
    private String hostName;

    @JsonProperty(SUBMITTER)
    private String querySubmitter;

    @JsonProperty(IS_INDEX_HIT)
    private boolean indexHit;

    @JsonProperty(QUERY_STATUS)
    private String queryStatus;

    @JsonProperty(RESULT_ROW_COUNT)
    private long resultRowCount;

    @JsonProperty(QUERY_HISTORY_ID)
    private long id;

    @JsonProperty(ENGINE_TYPE)
    private String engineType;

    @JsonProperty(TOTAL_SCAN_COUNT)
    private long totalScanCount;

    @JsonProperty(PROJECT_NAME)
    private String projectName;

    @JsonProperty(REALIZATIONS)
    private List<QueryRealization> nativeQueryRealizations;

    @JsonProperty(TOTAL_SCAN_BYTES)
    private long totalScanBytes;

    @JsonProperty(ERROR_TYPE)
    private String errorType;

    @JsonProperty(IS_CACHE_HIT)
    private boolean cacheHit;

    private String queryRealizations;
}
