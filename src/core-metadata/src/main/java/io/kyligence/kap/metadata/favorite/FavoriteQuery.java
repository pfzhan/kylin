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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FavoriteQuery {
    @JsonProperty("sql_pattern")
    private String sqlPattern;
    @JsonProperty("sql_pattern_hash")
    private int sqlPatternHash;
    @JsonProperty("last_query_time")
    private long lastQueryTime;
    @JsonProperty("success_rate")
    private float successRate;
    @JsonProperty("totalCount")
    private int totalCount;
    @JsonProperty("average_duration")
    private float averageDuration;
    @JsonProperty("status")
    private FavoriteQueryStatusEnum status;
    @JsonProperty("project")
    private String project;

    @JsonProperty("success_count")
    private int successCount;
    @JsonProperty("total_duration")
    private long totalDuration;

    public FavoriteQuery() {
    }

    public FavoriteQuery(final String sqlPattern, final int sqlPatternHash, final String project) {
        this.sqlPattern = sqlPattern;
        this.sqlPatternHash = sqlPatternHash;
        this.totalCount = 1;
        this.project = project;
        this.lastQueryTime = System.currentTimeMillis();
        this.status = FavoriteQueryStatusEnum.WAITING;
    }

    public FavoriteQuery(final String sqlPattern, final int sqlPatternHash, final String project, long lastQueryTime, int totalCount, long totalDuration) {
        this.sqlPattern = sqlPattern;
        this.sqlPatternHash = sqlPatternHash;
        this.project = project;
        this.lastQueryTime = lastQueryTime;
        this.totalCount = totalCount;
        this.totalDuration = totalDuration;
        this.status = FavoriteQueryStatusEnum.WAITING;
    }

    public void increaseTotalCountByOne() {
        this.totalCount ++;
    }

    public void increaseSuccessCountByOne() {
        this.successCount ++;
    }

    public void increaseTotalDuration(long totalDuration) {
        this.totalDuration += totalDuration;
    }

    @Override
    public boolean equals(Object obj) {
        FavoriteQuery that = (FavoriteQuery) obj;

        return this.sqlPattern.equals(that.getSqlPattern()) && this.project.equals(that.getProject());
    }

    @Override
    public int hashCode() {
        return (this.project + this.sqlPattern).hashCode();
    }
}
