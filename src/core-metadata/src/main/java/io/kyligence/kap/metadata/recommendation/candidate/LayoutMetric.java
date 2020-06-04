package io.kyligence.kap.metadata.recommendation.candidate;

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
    static class LatencyMap {
        @JsonIgnore
        private NavigableMap<Long, Double> totalLatencyMapPerDay = new TreeMap<>();

        @JsonAnySetter
        public void add(String key, Double value) {
            totalLatencyMapPerDay.put(Long.parseLong(key), value);
        }

        @JsonAnyGetter
        public Map<Long, Double> getMap() {
            return totalLatencyMapPerDay;
        }

        public LatencyMap merge(LatencyMap other) {
            other.getTotalLatencyMapPerDay().forEach((k, v) -> this.totalLatencyMapPerDay.merge(k, v, Double::sum));
            return this;
        }

        double getLatencyByDate(long queryTime) {
            return totalLatencyMapPerDay.get(getDateInMillis(queryTime)) == null ? 0
                    : totalLatencyMapPerDay.get(getDateInMillis(queryTime));
        }

        private long getDateInMillis(final long queryTime) {
            return TimeUtil.getDayStart(queryTime);
        }

    }
}
