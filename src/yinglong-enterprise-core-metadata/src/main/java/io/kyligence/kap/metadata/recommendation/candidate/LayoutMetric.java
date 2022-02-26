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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kylin.common.util.TimeUtil;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LayoutMetric {
    private FrequencyMap frequencyMap;
    private LatencyMap latencyMap;

    public LayoutMetric(FrequencyMap frequencyMap, LatencyMap latencyMap) {
        this.frequencyMap = frequencyMap;
        this.latencyMap = latencyMap;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LatencyMap implements Serializable {
        /**
         *
         */
        @JsonIgnore
        private NavigableMap<Long, Long> totalLatencyMapPerDay = new TreeMap<>();

        @JsonAnySetter
        public void add(String key, long value) {
            totalLatencyMapPerDay.put(Long.parseLong(key), value);
        }

        public void incLatency(long queryTime, long value) {
            long totalLatency = totalLatencyMapPerDay.getOrDefault(getDateInMillis(queryTime), 0L);
            totalLatencyMapPerDay.put(getDateInMillis(queryTime), totalLatency + value);
        }

        @JsonAnyGetter
        public Map<Long, Long> getMap() {
            return totalLatencyMapPerDay;
        }

        public LatencyMap merge(LatencyMap other) {
            other.getTotalLatencyMapPerDay().forEach((k, v) -> this.totalLatencyMapPerDay.merge(k, v, Long::sum));
            return this;
        }

        @JsonIgnore
        public double getLatencyByDate(long queryTime) {
            return totalLatencyMapPerDay.get(getDateInMillis(queryTime)) == null ? 0
                    : totalLatencyMapPerDay.get(getDateInMillis(queryTime));
        }

        private long getDateInMillis(final long queryTime) {
            return TimeUtil.getDayStart(queryTime);
        }

    }
}
