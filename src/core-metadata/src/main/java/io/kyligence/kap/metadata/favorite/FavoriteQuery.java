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
package io.kyligence.kap.metadata.favorite;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.query.QueryHistory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class FavoriteQuery extends RootPersistentEntity {
    public static final String CHANNEL_FROM_RULE = "Rule-based";
    public static final String CHANNEL_FROM_IMPORTED = "Imported";

    @JsonProperty("sql_pattern")
    private String sqlPattern;
    @JsonProperty("last_query_time")
    private long lastQueryTime;
    @JsonProperty("total_count")
    private int totalCount;
    @JsonProperty("status")
    private FavoriteQueryStatusEnum status;

    @JsonProperty("success_count")
    private int successCount;
    @JsonProperty("total_duration")
    private long totalDuration;
    @JsonProperty("comment")
    private String comment;

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("success_rate")
    private float successRate;
    @JsonProperty("average_duration")
    private float averageDuration;

    @JsonProperty("frequency_map")
    private NavigableMap<Long, Integer> frequencyMap = new TreeMap<>();

    @JsonProperty("realization")
    @JsonManagedReference
    private List<FavoriteQueryRealization> realizations = Lists.newArrayList();

    public FavoriteQuery(final String sqlPattern) {
        this.sqlPattern = sqlPattern;
        this.lastQueryTime = System.currentTimeMillis();
        this.status = FavoriteQueryStatusEnum.WAITING;
    }

    public FavoriteQuery(final String sqlPattern, long lastQueryTime, int totalCount, long totalDuration) {
        this.sqlPattern = sqlPattern;
        this.lastQueryTime = lastQueryTime;
        this.totalCount = totalCount;
        this.totalDuration = totalDuration;
        if (totalCount == 0) {
            this.averageDuration = 0;
            this.successCount = 0;
        } else {
            this.averageDuration = totalDuration / (float) totalCount;
            this.successRate = successCount / (float) totalCount;
        }
        this.status = FavoriteQueryStatusEnum.WAITING;
    }

    public void incStats(QueryHistory queryHistory) {
        this.totalCount++;
        if (!queryHistory.isException())
            this.successCount++;
        this.totalDuration += queryHistory.getDuration();
        this.lastQueryTime = queryHistory.getQueryTime();

        long date = getDateInMillis(queryHistory.getQueryTime());
        Integer freq = frequencyMap.get(date);
        if (freq != null) {
            freq++;
            frequencyMap.put(date, freq);
        } else {
            frequencyMap.put(date, 1);
        }
    }

    private long getDateInMillis(final long queryTime) {
        return queryTime - queryTime % (24 * 60 * 60 * 1000L);
    }

    public void update(FavoriteQuery favoriteQuery) {
        this.lastQueryTime = favoriteQuery.getLastQueryTime();
        this.totalCount += favoriteQuery.getTotalCount();
        this.totalDuration += favoriteQuery.getTotalDuration();
        this.successCount += favoriteQuery.getSuccessCount();
        // this.totalcount is at least 1
        this.averageDuration = totalDuration / (float) totalCount;
        this.successRate = successCount / (float) totalCount;
        // merge two maps
        favoriteQuery.getFrequencyMap()
                .forEach((k, v) -> this.frequencyMap.merge(k, v, (value1, value2) -> value1 + value2));
        long frequencyInitialDate = getDateInMillis(this.lastQueryTime) - KylinConfig.getInstanceFromEnv().getFavoriteQueryFrequencyTimeWindow();

        while (this.frequencyMap.size() != 0) {
            if (frequencyInitialDate <= this.frequencyMap.firstKey())
                break;
            this.frequencyMap.pollFirstEntry();
        }
    }

    public int getFrequency() {
        long frequencyInitialCollectDate = getDateBeforeFrequencyTimeWindow();
        while (frequencyMap.size() != 0) {
            long date = frequencyMap.firstKey();
            if (frequencyInitialCollectDate <= date)
                break;
            frequencyMap.pollFirstEntry();
        }

        return frequencyMap.values().stream().reduce((x, y) -> x + y).orElse(0);
    }

    private long getDateBeforeFrequencyTimeWindow() {
        long daysInMillis = KylinConfig.getInstanceFromEnv().getFavoriteQueryFrequencyTimeWindow();
        return getDateInMillis(System.currentTimeMillis()) - daysInMillis;
    }

    public void updateStatus(FavoriteQueryStatusEnum status, String comment) {
        setStatus(status);
        setComment(comment);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (this.getClass() != obj.getClass())
            return false;

        FavoriteQuery that = (FavoriteQuery) obj;

        return this.sqlPattern.equals(that.getSqlPattern());
    }

    @Override
    public int hashCode() {
        return this.sqlPattern.hashCode();
    }
}
