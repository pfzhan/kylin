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
package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class HealthResponse {
    @JsonProperty("spark_status")
    private RestartSparkStatusResponse restartSparkStatus;
    @JsonProperty("slow_queries_status")
    private List<CanceledSlowQueryStatusResponse> canceledSlowQueryStatus;

    @Getter
    @Setter
    @AllArgsConstructor
    public static class RestartSparkStatusResponse {
        @JsonProperty("restart_failure_times")
        private int startSparkFailureTimes;
        @JsonProperty("last_restart_failure_time")
        private long lastStartSparkFailureTime;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class CanceledSlowQueryStatusResponse {
        @JsonProperty("query_id")
        private String queryId;
        @JsonProperty("canceled_times")
        private int canceledTimes;
        @JsonProperty("last_canceled_time")
        private long lastCanceledTime;
        @JsonProperty("duration_time")
        private float queryDurationTime;
    }
}
