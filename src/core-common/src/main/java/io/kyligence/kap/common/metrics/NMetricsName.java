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
            "offline_model_num_gauge"),

    //query
    QUERY("query_total_times"), QUERY_SLOW("gt10s_query_total_times"), QUERY_FAILED(
            "failed_query_total_times"), QUERY_PUSH_DOWN("pushdown_query_total_times"), QUERY_TIMEOUT(
                    "timeout_query_total_times"), QUERY_LATENCY("query_latency"), QUERY_SLOW_RATE(
                            "gt10s_query_rate"), QUERY_FAILED_RATE("failed_query_rate"), QUERY_PUSH_DOWN_RATE(
                                    "pushdown_query_rate"), QUERY_TIMEOUT_RATE("timeout_query_rate"),

    //job
    JOB("job_created_total_times"), JOB_DURATION("finished_jobs_total_duration"), JOB_FINISHED(
            "job_finished_total_times"), JOB_DURATION_HISTOGRAM("job_duration"), JOB_STEP_ATTEMPTED(
                    "job_step_attempted_total_times"), JOB_FAILED_STEP_ATTEMPTED(
                            "failed_job_step_attempted_total_times"), JOB_RESUMED(
                                    "job_resumed_total_times"), JOB_DISCARDED("job_discarded_total_times"), JOB_ERROR(
                                            "error_job_total_times"), JOB_ERROR_GAUGE(
                                                    "error_job_num_gauge"), JOB_RUNNING_GAUGE("running_job_num_gauge"),

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

    //event statistics
    EVENT_GAUGE("event_num_gauge"), EVENT_COUNTER("event_created_total_num");

    private String value;

    NMetricsName(String value) {
        this.value = value;
    }

    public String getVal() {
        return this.value;
    }
}
