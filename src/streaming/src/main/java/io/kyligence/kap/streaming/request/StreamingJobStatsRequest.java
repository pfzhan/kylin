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

package io.kyligence.kap.streaming.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class StreamingJobStatsRequest extends StreamingRequestHeader{

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("project")
    private String project;

    @JsonProperty("batch_row_num")
    private Long batchRowNum;

    @JsonProperty("window_size")
    private Long windowSize;

    @JsonProperty("rows_per_second")
    private Double rowsPerSecond;

    @JsonProperty("trigger_start_time")
    private Long triggerStartTime;

    /**
     * progress.durationMs
     */
    @JsonProperty("processing_time")
    private Long processingTime;

    @JsonProperty("min_data_latency")
    private Long minDataLatency;

    @JsonProperty("max_data_latency")
    private Long maxDataLatency;

    public StreamingJobStatsRequest() {

    }

    public StreamingJobStatsRequest(String jobId, String project, Long batchRowNum, Double rowsPerSecond,
            Long durationMs, Long triggerStartTime, Long minDataLatency, Long maxDataLatency) {
        this.jobId = jobId;
        this.project = project;
        this.batchRowNum = batchRowNum;
        this.rowsPerSecond = rowsPerSecond;
        this.processingTime = durationMs;
        this.triggerStartTime = triggerStartTime;
        this.minDataLatency = minDataLatency;
        this.maxDataLatency = maxDataLatency;
    }
}
