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
import lombok.Setter;
import lombok.ToString;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import java.util.List;

@Getter
@Setter
@ToString
public class FavoriteQuery extends RootPersistentEntity {
    public static final String CHANNEL_FROM_RULE = "rule-based";
    public static final String CHANNEL_FROM_WHITE_LIST = "whitelist-based";

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

    @JsonProperty("realization")
    @JsonManagedReference
    private List<FavoriteQueryRealization> realizations = Lists.newArrayList();

    public FavoriteQuery() {
        updateRandomUuid();
    }

    public FavoriteQuery(final String sqlPattern) {
        updateRandomUuid();
        this.sqlPattern = sqlPattern;
        this.lastQueryTime = System.currentTimeMillis();
        this.status = FavoriteQueryStatusEnum.WAITING;
    }

    public FavoriteQuery(final String sqlPattern, long lastQueryTime, int totalCount, long totalDuration) {
        updateRandomUuid();
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

    public void update(QueryHistory queryHistory) {
        this.totalCount ++;
        if (!queryHistory.isException())
            this.successCount ++;
        this.totalDuration += queryHistory.getDuration();
        this.lastQueryTime = queryHistory.getQueryTime();
    }

    public void update(FavoriteQuery favoriteQuery) {
        this.lastQueryTime = favoriteQuery.getLastQueryTime();
        this.totalCount += favoriteQuery.getTotalCount();
        this.totalDuration += favoriteQuery.getTotalDuration();
        this.successCount += favoriteQuery.getSuccessCount();
        // this.totalcount is at least 1
        this.averageDuration = totalDuration / (float) totalCount;
        this.successRate = successCount / (float) totalCount;
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
