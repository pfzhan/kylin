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

import lombok.Getter;

@Getter
public enum PrometheusMetrics {

    JVM_DB_CONNECTIONS("ke_db_connections", Type.INSTANCE_METRIC), //

    SPARK_TASKS("spark_tasks", Type.INSTANCE_METRIC), //
    SPARK_TASK_UTILIZATION("spark_tasks_utilization", Type.INSTANCE_METRIC), //

    QUERY_SECONDS("ke_queries_seconds", Type.PROJECT_METRIC), //
    QUERY_SCAN_BYTES("ke_queries_scan_bytes", Type.PROJECT_METRIC), //
    QUERY_RESULT_ROWS("ke_queries_result_rows", Type.PROJECT_METRIC), //
    QUERY_JOBS("ke_queries_jobs", Type.PROJECT_METRIC), //
    QUERY_STAGES("ke_queries_stages", Type.PROJECT_METRIC), //
    QUERY_TASKS("ke_queries_tasks", Type.PROJECT_METRIC), //

    SPARDER_UP("ke_sparder_up", Type.INSTANCE_METRIC), //

    JOB_COUNTS("ke_job_counts", Type.PROJECT_METRIC), //
    JOB_MINUTES("ke_job_minutes", Type.PROJECT_METRIC), //

    MODEL_BUILD_DURATION("ke_model_build_minutes", Type.PROJECT_METRIC | Type.MODEL_METRIC);

    private static class Type {
        public static final int GLOBAL = 0;
        public static final int INSTANCE_METRIC = 0x0001;
        public static final int PROJECT_METRIC = 0x0002;
        public static final int MODEL_METRIC = 0x0004;
    }

    private final String value;
    private final int types;

    PrometheusMetrics(String value, int types) {
        this.value = value;
        this.types = types;
    }

    public static Set<PrometheusMetrics> listModelMetrics() {
        return Stream.of(PrometheusMetrics.values()).filter(metric -> (metric.getTypes() & Type.MODEL_METRIC) != 0)
                .collect(Collectors.toSet());
    }
}
