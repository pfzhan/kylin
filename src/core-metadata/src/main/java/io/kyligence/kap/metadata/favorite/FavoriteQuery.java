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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.FrequencyMap;
import io.kyligence.kap.metadata.query.QueryHistory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

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

    @JsonProperty("total_duration")
    private long totalDuration;
    @JsonProperty("comment")
    private String comment;

    @JsonProperty("channel")
    private String channel;

    @JsonProperty("average_duration")
    private float averageDuration;

    @JsonProperty("frequency_map")
    private FrequencyMap frequencyMap = new FrequencyMap();

    @JsonProperty("realization")
    @JsonManagedReference
    private List<FavoriteQueryRealization> realizations = Lists.newArrayList();

    private String project;

    public FavoriteQuery(final String sqlPattern) {
        this.sqlPattern = sqlPattern;
        this.lastQueryTime = System.currentTimeMillis();
        this.status = FavoriteQueryStatusEnum.TO_BE_ACCELERATED;
    }

    public FavoriteQuery(final String sqlPattern, long lastQueryTime, int totalCount, long totalDuration) {
        this.sqlPattern = sqlPattern;
        this.lastQueryTime = lastQueryTime;
        this.totalCount = totalCount;
        this.totalDuration = totalDuration;
        if (totalCount == 0) {
            this.averageDuration = 0;
        } else {
            this.averageDuration = totalDuration / (float) totalCount;
        }
        this.status = FavoriteQueryStatusEnum.TO_BE_ACCELERATED;
    }

    public void initAfterReload(KylinConfig config, String project) {
        this.project = project;
    }

    public void incStats(QueryHistory queryHistory) {
        this.totalCount++;
        this.totalDuration += queryHistory.getDuration();
        this.lastQueryTime = queryHistory.getQueryTime();

        frequencyMap.incFrequency(queryHistory.getQueryTime());
    }

    public void update(FavoriteQuery favoriteQuery) {
        this.lastQueryTime = favoriteQuery.getLastQueryTime();
        this.totalCount += favoriteQuery.getTotalCount();
        this.totalDuration += favoriteQuery.getTotalDuration();
        // this.totalcount is at least 1
        this.averageDuration = totalDuration / (float) totalCount;
        // merge two maps
        frequencyMap.merge(favoriteQuery.frequencyMap);
        frequencyMap.rotate(lastQueryTime, project);
    }

    public int getFrequency(String project) {
        return frequencyMap.getFrequency(project);
    }

    public void updateStatus(FavoriteQueryStatusEnum status, String comment) {
        setStatus(status);
        setComment(comment);
    }

    public boolean isInWaitingList() {
        return status.equals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED)
                || status.equals(FavoriteQueryStatusEnum.ACCELERATING);
    }

    public boolean isNotAccelerated() {
        return status.equals(FavoriteQueryStatusEnum.PENDING) || status.equals(FavoriteQueryStatusEnum.FAILED);
    }

    public boolean isAccelerated() {
        return status.equals(FavoriteQueryStatusEnum.ACCELERATED);
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

    @Override
    public String getResourcePath() {
        return "/" + project + ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }
}
