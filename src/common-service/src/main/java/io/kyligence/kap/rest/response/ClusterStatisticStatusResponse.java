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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class ClusterStatisticStatusResponse {
    @JsonProperty("start")
    private long start;
    @JsonProperty("end")
    private long end;
    @JsonProperty("interval")
    private long interval;

    @JsonProperty("job")
    private List<NodeStatisticStatusResponse> job;
    @JsonProperty("query")
    private List<NodeStatisticStatusResponse> query;

    @Data
    public static class NodeStatisticStatusResponse {
        @JsonProperty("instance")
        private String instance;
        @JsonProperty("details")
        private List<NodeTimeState> details;
        @JsonProperty("unavailable_time")
        private long unavailableTime;
        @JsonProperty("unavailable_count")
        private int unavailableCount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeTimeState {
        @JsonProperty("time")
        private long time;
        @JsonProperty("status")
        private ClusterStatusResponse.NodeState state;
    }
}
