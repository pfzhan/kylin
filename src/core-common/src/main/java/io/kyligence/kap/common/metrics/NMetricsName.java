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

package io.kyligence.kap.common.metrics;

public enum NMetricsName {

    //summary
    SUMMARY_COUNTER("summary_exec_total_times"), SUMMARY_DURATION("summary_exec_total_duration"),
    //project
    PROJECT_GAUGE("project_num_gauge"), //need a gauge instead of a counter!
    //storage statistic
    PROJECT_STORAGE_SIZE("storage_size_gauge"), PROJECT_GARBAGE_SIZE("garbage_size_gauge"),

    //model
    MODEL_GAUGE("model_num_gauge"), HEALTHY_MODEL_GAUGE("non_broken_model_num_gauge"), OFFLINE_MODEL_GAUGE(
            "offline_model_num_gauge"), MODEL_SEGMENTS("model_segments_num_gauge"), MODEL_STORAGE(
                    "model_total_storage_gauge"), MODEL_LAST_QUERY_TIME(
                            "model_last_query_time_gauge"), MODEL_QUERY_COUNT(
                                    "model_query_count_gauge"), MODEL_BUILD_DURATION("model_build_duration"), MODEL_WAIT_DURATION("model_wait_duration"),

    //query
    QUERY("query_total_times"), QUERY_SLOW("gt10s_query_total_times"), QUERY_FAILED(
            "failed_query_total_times"), QUERY_PUSH_DOWN("pushdown_query_total_times"), QUERY_PUSH_DOWN_RATIO("pushdown_query_ratio"), QUERY_TIMEOUT(
                    "timeout_query_total_times"), QUERY_LATENCY("query_latency"), QUERY_SLOW_RATE(
                            "gt10s_query_rate"), QUERY_FAILED_RATE("failed_query_rate"), QUERY_PUSH_DOWN_RATE(
                                    "pushdown_query_rate"), QUERY_TIMEOUT_RATE("timeout_query_rate"), QUERY_CACHE(
                                            "cache_query_total_times"), QUERY_CACHE_RATIO("cache_query_ratio"), QUERY_AGG_INDEX(
                                                    "agg_index_query_total_times"), QUERY_AGG_INDEX_RATIO("agg_index_query_ratio"),  QUERY_TABLE_INDEX(
                                                            "table_index_query_total_times"), QUERY_TABLE_INDEX_RATIO("table_index_query_ratio"), QUERY_SCAN_BYTES(
                                                                    "query_scan_bytes"), QUERY_LT_1S("lt_1s_total_times"), QUERY_1S_3S(
                                                                                    "bw_1s_3s_total_times"), QUERY_3S_5S("bw_3s_5s_total_times"), QUERY_5S_10S(
                                                                                            "bw_5s_10s_total_times"), QUERY_LT_1S_RATIO("lt_1s_ratio"), QUERY_1S_3S_RATIO(
                                                                                                    "bw_1s_3s_ratio"), QUERY_3S_5S_RATIO("bw_3s_5s_ratio"), QUERY_5S_10S_RATIO(
                                                                                                            "bw_5s_10s_ratio"), QUERY_SLOW_RATIO("gt10s_query_ratio"),

    //job
    JOB("job_created_total_times"), JOB_DURATION("finished_jobs_total_duration"), JOB_FINISHED(
            "job_finished_total_times"), JOB_DURATION_HISTOGRAM("job_duration"), JOB_STEP_ATTEMPTED(
                    "job_step_attempted_total_times"), JOB_FAILED_STEP_ATTEMPTED(
                            "failed_job_step_attempted_total_times"), JOB_RESUMED(
                                    "job_resumed_total_times"), JOB_DISCARDED("job_discarded_total_times"), JOB_ERROR(
                                            "error_job_total_times"), JOB_ERROR_GAUGE(
                                                    "error_job_num_gauge"), JOB_RUNNING_GAUGE("running_job_num_gauge"), JOB_PENDING_GAUGE(
                                                            "pending_job_num_gauge"), JOB_WAIT_DURATION("job_wait_duration"),

    // host
    QUERY_HOST("query_num_per_host"), QUERY_SCAN_BYTES_HOST("query_scan_bytes_per_host"), QUERY_TIME_HOST(
            "query_time_per_host"),

    //garbage management
    STORAGE_CLEAN("storage_clean_total_times"), STORAGE_CLEAN_DURATION(
            "storage_clean_total_duration"), STORAGE_CLEAN_FAILED("failed_storage_clean_total_times"),

    //metadata management
    METADATA_CLEAN("metadata_clean_total_times"), METADATA_BACKUP(
            "metadata_backup_total_times"), METADATA_BACKUP_DURATION(
                    "metadata_backup_total_duration"), METADATA_BACKUP_FAILED(
                            "failed_metadata_backup_total_times"), METADATA_OPS_CRON(
                                    "metadata_ops_total_times"), METADATA_OPS_CRON_SUCCESS(
                                            "metadata_success_ops_total_times"),

    // favorite queue, maybe you know:
    // Equivalent definitions: ["fe": "front-end"], ["be": "back-end"]
    // "Accel." is an abbreviation for the noun "acceleration"
    FQ_FE_INVOKED("fq_accepted_total_times"), FQ_BE_INVOKED("fq_proposed_total_times"), FQ_BE_INVOKED_DURATION(
            "fq_proposed_total_duration"), FQ_BE_INVOKED_FAILED("failed_fq_proposed_total_times"), FQ_ADJUST_INVOKED(
                    "fq_adjusted_total_times"), FQ_ADJUST_INVOKED_DURATION(
                            "fq_adjusted_total_duration"), FQ_UPDATE_USAGE(
                                    "fq_update_usage_total_times"), FQ_UPDATE_USAGE_DURATION(
                                            "fq_update_usage_total_duration"), FQ_FAILED_UPDATE_USAGE(
                                                    "failed_fq_update_usage_total_times"),

    // FQ gauges
    FQ_TO_BE_ACCELERATED("fq_tobeaccelerated_num_gauge"), FQ_ACCELERATED("fq_accelerated_num_gauge"), FQ_FAILED(
            "fq_failed_num_gauge"), FQ_ACCELERATING("fq_accelerating_num_gauge"), FQ_PENDING(
                    "fq_pending_num_gauge"), FQ_BLACKLIST("fq_blacklist_num_gauge"),

    //index

    // JVM
    HEAP_MAX("jvm_memory_heap_max"), HEAP_USED("jvm_memory_heap_used"), HEAP_USAGE(
            "jvm_memory_heap_usage"), JVM_GC("jvm_garbage_collection"), JVM_AVAILABLE_CPU("jvm_available_cpu_count"),


    //spark context
    SPARDER_RESTART("sparder_restart_total_times"),

    //transaction
    TRANSACTION_RETRY_COUNTER("transaction_retry_total_times"), TRANSACTION_LATENCY("transaction_latency"),

    //user management
    USER_GAUGE("user_num_gauge"),

    //table management
    TABLE_GAUGE("table_num_gauge"),

    //database management
    DB_GAUGE("db_num_gauge"),

    //rest api
    NON_QUERY_RESTAPI_LATENCY("non_query_restapi_latency"),

    BUILD_UNAVAILABLE_DURATION("build_unavailable_duration"), QUERY_UNAVAILABLE_DURATION("query_unavailable_duration"),

    //event statistics
    EVENT_GAUGE("event_num_gauge"), EVENT_COUNTER("event_created_total_num"),

    // streaming statistics
    BATCH_TIMES("batch_times"), NEW_DATA_AVAILABLE_BATCH_TIMRS("new_data_available_batch_times"), NUM_INPUT_ROWS("num_input_rows"),
    INPUT_ROWS_PER_SECOND("input_rows_per_second"), BATCH_DURATION("batch_duration"),
    RETAIN_SEGMENTS("retain_segments"), SEGMENTS_MERGE_THRESHOLDS("segments_merge_thresholds"), SEGMENTS_MERGE_COST_TIME("segments_merge_cost_time"),
    MODEL_QUERYABLE_SEGMENT_NUM("model_queryable_segment_num");

    private String value;

    NMetricsName(String value) {
        this.value = value;
    }

    public String getVal() {
        return this.value;
    }
}
