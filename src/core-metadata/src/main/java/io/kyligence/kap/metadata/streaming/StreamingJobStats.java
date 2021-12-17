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

package io.kyligence.kap.metadata.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StreamingJobStats {

    // table names
    public static final String STREAMING_JOB_STATS_SUFFIX = "streaming_job_stats";

    @JsonProperty("id")
    private Long id;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("project_name")
    private String projectName;

    @JsonProperty("batch_row_num")
    private Long batchRowNum;

    @JsonProperty("rows_per_second")
    private Double rowsPerSecond;

    @JsonProperty("processing_time")
    private Long processingTime;

    @JsonProperty("min_data_latency")
    private Long minDataLatency;

    @JsonProperty("max_data_latency")
    private Long maxDataLatency;

    @JsonProperty("create_time")
    private Long createTime;

    public StreamingJobStats() {

    }

    public StreamingJobStats(String jobId, String projectName, Long batchRowNum, Double rowsPerSecond, Long durationMs,
            Long minDataLatency, Long maxDataLatency, Long createTime) {
        this.jobId = jobId;
        this.projectName = projectName;
        this.batchRowNum = batchRowNum;
        this.rowsPerSecond = rowsPerSecond;
        this.processingTime = durationMs;
        this.minDataLatency = minDataLatency;
        this.maxDataLatency = maxDataLatency;
        this.createTime = createTime;
    }

}
