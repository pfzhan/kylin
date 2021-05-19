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
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.Data;

@Data
public class StreamingJobResponse {

    @JsonProperty("build_job_meta")
    private StreamingJobMeta buildJobMeta;

    @JsonProperty("merge_job_meta")
    private StreamingJobMeta mergeJobMeta;

    @JsonProperty("last_build_time")
    private long lastBuildTime;

    @JsonProperty("latency")
    private long latency;

    @JsonProperty("last_update_time")
    private long lastUpdateTime;

    @JsonProperty("avg_consume_rate_in_5mins")
    private Double avgConsumeRateIn5mins;

    @JsonProperty("avg_consume_rate_in_15mins")
    private Double avgConsumeRateIn15mins;

    @JsonProperty("avg_consume_rate_in_30mins")
    private Double avgConsumeRateIn30mins;

    @JsonProperty("avg_consume_rate_in_All")
    private Double avgConsumeRateInAll;

    public StreamingJobResponse() {

    }

    public StreamingJobResponse(StreamingJobMeta meta, StreamingJobMeta mergeMeta) {
        this(meta, mergeMeta, 0L, 0L, 0L, (double) 0, (double) 0, (double) 0, (double) 0);
    }

    public StreamingJobResponse(StreamingJobMeta meta, StreamingJobMeta mergeMeta, long lastBuildTime, long latency,
                                long lastUpdateTime, Double avgConsumeRateIn5mins, Double avgConsumeRateIn15mins,
                                Double avgConsumeRateIn30mins, Double avgConsumeRateInAll) {
        this.buildJobMeta = meta;
        this.mergeJobMeta = mergeMeta;
        this.lastBuildTime = lastBuildTime;
        this.latency = latency;
        this.lastUpdateTime = lastUpdateTime;
        this.avgConsumeRateIn5mins = avgConsumeRateIn5mins;
        this.avgConsumeRateIn15mins = avgConsumeRateIn15mins;
        this.avgConsumeRateIn30mins = avgConsumeRateIn30mins;
        this.avgConsumeRateInAll = avgConsumeRateInAll;
    }

}
