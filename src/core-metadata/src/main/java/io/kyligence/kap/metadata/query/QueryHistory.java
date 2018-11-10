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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Measurement;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@Measurement(name = "query_metric")
@Getter
@Setter
public class QueryHistory {
    public static final String ADJ_SLOW = "Slow";
    public static final String QUERY_HISTORY_SUCCEEDED = "SUCCEEDED";
    public static final String QUERY_HISTORY_FAILED = "FAILED";
    public static final String QUERY_HISTORY_UNACCELERATED = "UNACCELERATED";
    public static final String QUERY_HISTORY_PARTLY_ACCELERATED = "PARTLY_ACCELERATED";
    public static final String QUERY_HISTORY_ACCELERATED = "FULLY_ACCELERATED";

    // database name
    public static final String DB_NAME = InfluxDBWriter.DEFAULT_DATABASE;

    // table names
    public static final String QUERY_MEASUREMENT = "query_metric";
    public static final String REALIZATION_MEASUREMENT = "realization_metric";

    public static final String QUERY_ID = "query_id";
    public static final String SQL_TEXT = "sql_text";
    public static final String SQL_PATTERN = "sql_pattern";
    public static final String QUERY_DURATION = "duration";
    public static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
    public static final String TOTAL_SCAN_COUNT = "total_scan_count";
    public static final String RESULT_ROW_COUNT = "result_row_count";
    public static final String SUBMITTER = "submitter";
    public static final String PROJECT = "project";
    public static final String MODEL = "model";
    public static final String REALIZATIONS = "realizations";
    public static final String REALIZATION_NAME = "realization_name";
    public static final String REALIZATION_TYPE = "realization_type";
    public static final String QUERY_HOSTNAME = "hostname";
    public static final String SUITE = "suite";
    public static final String ERROR_TYPE = "error_type";
    public static final String ENGINE_TYPE = "engine_type";
    public static final String IS_CACHE_HIT = "cache_hit";
    public static final String QUERY_STATUS = "query_status";
    public static final String ACCELERATE_STATUS = "accelerate_status";
    public static final String ANSWERED_BY = "answered_by";
    public static final String IS_CUBE_HIT = "cube_hit";
    public static final String QUERY_TIME = "query_time";

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

    @JsonProperty(REALIZATIONS)
    @Column(name = REALIZATIONS)
    private String realization;

    @JsonProperty(QUERY_HOSTNAME)
    @Column(name = QUERY_HOSTNAME, tag = true)
    private String hostName;

    @JsonProperty(SUBMITTER)
    @Column(name = SUBMITTER, tag = true)
    private String querySubmitter;

    @JsonProperty(QUERY_STATUS)
    @Column(name = QUERY_STATUS)
    private String queryStatus;

    @JsonProperty(ACCELERATE_STATUS)
    @Column(name = ACCELERATE_STATUS)
    private String accelerateStatus;

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

    @JsonProperty(ANSWERED_BY)
    @Column(name = ANSWERED_BY, tag = true)
    private String answeredBy;

    @Column(name = PROJECT, tag = true)
    private String queryProject;

    @Column(name = "count")
    private int size;

    // only for test
    private transient long insertTime;

    public QueryHistory() {
    }

    public QueryHistory(String sqlPattern, String project, String queryStatus, String querySubmitter, long queryTime, long duration) {
        this.sqlPattern = sqlPattern;
        this.queryProject = project;
        this.queryStatus = queryStatus;
        this.querySubmitter = querySubmitter;
        this.queryTime = queryTime;
        this.duration = duration;
    }

    public QueryHistory(String sql) {
        this.sql = sql;
    }

    public String getProject() {
       return this.queryProject;
    }

    public void setProject(String project) {
        this.queryProject = project;
    }

    public boolean isException() {
        return queryStatus.equals(QUERY_HISTORY_FAILED);
    }
}
