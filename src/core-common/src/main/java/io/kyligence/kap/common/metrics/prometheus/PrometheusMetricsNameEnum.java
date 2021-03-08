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
public enum PrometheusMetricsNameEnum {
    // ========== jvm ============
    JVM_GC_PAUSE_TIME("kylin_jvm_gc_milliseconds", Type.INSTANCE_METRIC),

    // ========== query ============
    QUERY_TIMES("kylin_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER),
    QUERY_SLOW_TIMES("kylin_slow_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER),
    QUERY_FAILED_TIMES("kylin_failed_query_times", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_COUNTER),

    // ========== job ============
    JOB_WAIT_DURATION_MAX("kylin_job_wait_duration_max", Type.PROJECT_METRIC),
    JOB_RUNNING_DURATION_MAX("kylin_job_running_duration_max", Type.PROJECT_METRIC),
    JOB_ERROR_NUM("kylin_error_job_num", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG),
    JOB_PENDING_NUM("kylin_pending_job_num", Type.PROJECT_METRIC | Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG),
    MODEL_JOB_EXCEED_LAST_JOB_TIME_THRESHOLD("kylin_model_job_exceed_last_job_time_threshold", Type.MODEL_METRIC);

    private static class Type {
        public static final int INSTANCE_METRIC = 0x0001;
        public static final int PROJECT_METRIC = 0x0002;
        public static final int MODEL_METRIC = 0x0004;
        public static final int PROJECT_METRIC_FROM_COUNTER = 0x0008;
        public static final int PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG = 0x0010;
    }

    private final String value;
    private final int types;

    PrometheusMetricsNameEnum(String value, int types) {
        this.value = value;
        this.types = types;
    }

    public static Set<PrometheusMetricsNameEnum> listProjectMetrics() {
        return Stream.of(PrometheusMetricsNameEnum.values())
                .filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetricsNameEnum> listModelMetrics() {
        return Stream.of(PrometheusMetricsNameEnum.values())
                .filter(metric -> (metric.getTypes() & Type.MODEL_METRIC) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetricsNameEnum> listProjectMetricsFromCounter() {
        return Stream.of(PrometheusMetricsNameEnum.values())
                .filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC_FROM_COUNTER) != 0)
                .collect(Collectors.toSet());
    }

    public static Set<PrometheusMetricsNameEnum> listProjectMetricsFromGaugeWithoutHostTag() {
        return Stream.of(PrometheusMetricsNameEnum.values())
                .filter(metric -> (metric.getTypes() & Type.PROJECT_METRIC_FROM_GAUGE_WITHOUT_HOST_TAG) != 0)
                .collect(Collectors.toSet());
    }

    public MetricsName toMetricsName() {
        switch (this) {
            case QUERY_TIMES:
                return MetricsName.QUERY;
            case QUERY_SLOW_TIMES:
                return MetricsName.QUERY_SLOW;
            case QUERY_FAILED_TIMES:
                return MetricsName.QUERY_FAILED;
            case JOB_ERROR_NUM:
                return MetricsName.JOB_ERROR_GAUGE;
            case JOB_PENDING_NUM:
                return MetricsName.JOB_PENDING_GAUGE;
            default:
                throw new IllegalArgumentException("Invalid metrics name: " + this);
        }
    }
}