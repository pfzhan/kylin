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

package io.kyligence.kap.common.metrics.prometheus;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kyligence.kap.common.metrics.MetricsName;
import lombok.Getter;

@Getter
public enum PrometheusMetrics {
    // ========== jvm ============
    JVM_GC_PAUSE_TIME("kylin_jvm_gc_milliseconds", Type.INSTANCE_METRIC), //
    JVM_DB_CONNECTIONS("ke_db_connections", Type.INSTANCE_METRIC), //

    // ========== sparder ========
    SPARK_TASKS("spark_tasks", Type.INSTANCE_METRIC), //
    SPARK_TASK_UTILIZATION("spark_tasks_utilization", Type.INSTANCE_METRIC), //

    // ========== query ============
    QUERY_TIMES("kylin_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER), //
    QUERY_SLOW_TIMES("kylin_slow_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER), //
    QUERY_FAILED_TIMES("kylin_failed_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER), //
    QUERY_SECONDS("ke_queries_seconds", Type.PROJECT_METRIC), //
    QUERY_SCAN_BYTES("ke_queries_scan_bytes", Type.PROJECT_METRIC), //

    // ========== job ============
    JOB_WAIT_DURATION_MAX("kylin_job_wait_duration_max", Type.PROJECT_METRIC), //
    JOB_RUNNING_DURATION_MAX("kylin_job_running_duration_max", Type.PROJECT_METRIC), //
    JOB_ERROR_NUM("kylin_error_job_num", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG), //
    JOB_PENDING_NUM("kylin_pending_job_num", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG), //
    MODEL_JOB_EXCEED_LAST_JOB_TIME_THRESHOLD("kylin_model_job_exceed_last_job_time_threshold", Type.MODEL_METRIC), //
    MODEL_BUILD_DURATION("ke_model_build_seconds", Type.PROJECT_METRIC | Type.MODEL_METRIC), //

    // Used in statistics
    // ========== job ============
    JOB_COUNT("kylin_job_count", Type.PROJECT_METRIC), //
    SUCCESSFUL_JOB_COUNT("kylin_successful_job_count", Type.PROJECT_METRIC), //
    ERROR_JOB_COUNT("kylin_error_job_count", Type.PROJECT_METRIC), //
    TERMINATED_JOB_COUNT("kylin_terminated_job_count", Type.PROJECT_METRIC), //
    JOB_COUNT_LT_5("kylin_job_count_lt_5", Type.PROJECT_METRIC), //
    JOB_COUNT_5_10("kylin_job_count_5_10", Type.PROJECT_METRIC), //
    JOB_COUNT_10_30("kylin_job_count_10_30", Type.PROJECT_METRIC), //
    JOB_COUNT_30_60("kylin_job_count_30_60", Type.PROJECT_METRIC), //
    JOB_COUNT_GT_60("kylin_job_count_gt_60", Type.PROJECT_METRIC), //
    JOB_TOTAL_DURATION("kylin_job_total_duration", Type.PROJECT_METRIC), //
    // ========== storage ============
    STORAGE_SIZE("kylin_storage_size", Type.PROJECT_METRIC), //
    GARBAGE_SIZE("kylin_garbage_size", Type.PROJECT_METRIC), //
    INDEX_USAGE("kylin_index_usage", Type.EXTRA), //
    RECOMMENDED_DELETE_INDEX_NUM("kylin_recommended_delete_index_num", Type.MODEL_METRIC), //

    // Used in report
    PROJECT_NUM_DAILY("kylin_project_num_daily", Type.GLOBAL), //
    USER_NUM_DAILY("kylin_user_num_daily", Type.GLOBAL), //
    QUERY_COUNT_DAILY("kylin_query_count_daily", Type.PROJECT_METRIC), //
    FAILED_QUERY_COUNT_DAILY("kylin_failed_query_count_daily", Type.PROJECT_METRIC), //
    QUERY_TOTAL_DURATION_DAILY("kylin_query_total_duration_daily", Type.PROJECT_METRIC), //
    QUERY_COUNT_LT_1S_DAILY("kylin_query_count_lt_1s_daily", Type.PROJECT_METRIC), //
    QUERY_COUNT_1S_3S_DAILY("kylin_query_count_1s_3s_daily", Type.PROJECT_METRIC), //
    STORAGE_SIZE_DAILY("kylin_storage_size_daily", Type.PROJECT_METRIC), //
    MODEL_NUM_DAILY("kylin_model_num_daily", Type.PROJECT_METRIC), //
    SNAPSHOT_NUM_DAILY("kylin_snapshot_num_daily", Type.PROJECT_METRIC), //
    JOB_COUNT_DAILY("kylin_job_count_daily", Type.PROJECT_METRIC), //
    SUCCESSFUL_JOB_COUNT_DAILY("kylin_successful_job_count_daily", Type.PROJECT_METRIC), //
    ERROR_JOB_COUNT_DAILY("kylin_error_job_count_daily", Type.PROJECT_METRIC), //
    JOB_TOTAL_DURATION_DAILY("kylin_job_total_duration_daily", Type.PROJECT_METRIC), //
    JOB_COUNT_LT_5_DAILY("kylin_job_count_lt_5_daily", Type.PROJECT_METRIC), //
    JOB_COUNT_5_10_DAILY("kylin_job_count_5_10_daily", Type.PROJECT_METRIC); //

    private static class Type {
        public static final int GLOBAL = 0;
        public static final int INSTANCE_METRIC = 0x0001;
        public static final int PROJECT_METRIC = 0x0002;
        public static final int MODEL_METRIC = 0x0004;
        public static final int PROJECT_METRIC_FROM_COUNTER = 0x0008;
        public static final int PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG = 0x0010;
        public static final int EXTRA = 0x1000;
    }

    private final String value;
    private final int types;

    PrometheusMetrics(String value, int types) {
        this.value = value;
        this.types = types;
    }

    public static Set<PrometheusMetrics> listProjectMetrics() {
        return Stream.of(PrometheusMetrics.values()).filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetrics> listModelMetrics() {
        return Stream.of(PrometheusMetrics.values()).filter(metric -> (metric.getTypes() & Type.MODEL_METRIC) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetrics> listProjectMetricsFromCounter() {
        return Stream.of(PrometheusMetrics.values())
                .filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC_FROM_COUNTER) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetrics> listProjectMetricsFromGaugeWithoutHostTag() {
        return Stream.of(PrometheusMetrics.values())
                .filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG) != 0)
                .collect(Collectors.toSet());
    }

    public MetricsName toMetricsName() {
        switch (this) {
        case QUERY_TIMES:
        case QUERY_COUNT_DAILY:
            return MetricsName.QUERY;
        case QUERY_SLOW_TIMES:
            return MetricsName.QUERY_SLOW;
        case QUERY_FAILED_TIMES:
        case FAILED_QUERY_COUNT_DAILY:
            return MetricsName.QUERY_FAILED;
        case JOB_ERROR_NUM:
            return MetricsName.JOB_ERROR_GAUGE;
        case JOB_PENDING_NUM:
            return MetricsName.JOB_PENDING_GAUGE;
        case QUERY_TOTAL_DURATION_DAILY:
            return MetricsName.QUERY_TOTAL_DURATION;
        case QUERY_COUNT_LT_1S_DAILY:
            return MetricsName.QUERY_LT_1S;
        case QUERY_COUNT_1S_3S_DAILY:
            return MetricsName.QUERY_1S_3S;
        default:
            throw new IllegalArgumentException("Invalid metrics name: " + this);
        }
    }
}