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
import io.kyligence.kap.metadata.query.QueryHistory;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class FavoriteQuery {
    @JsonProperty("sql_pattern")
    private String sqlPattern;
    @JsonProperty("sql_pattern_hash")
    private int sqlPatternHash;
    @JsonProperty("last_query_time")
    private long lastQueryTime;
    @JsonProperty("totalCount")
    private int totalCount;
    @JsonProperty("status")
    private FavoriteQueryStatusEnum status;
    @JsonProperty("project")
    private String project;

    @JsonProperty("success_count")
    private int successCount;
    @JsonProperty("total_duration")
    private long totalDuration;
    @JsonProperty("comment")
    private String comment;

    public FavoriteQuery() {
    }

    public FavoriteQuery(final String sqlPattern, final int sqlPatternHash, final String project) {
        this.sqlPattern = sqlPattern;
        this.sqlPatternHash = sqlPatternHash;
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

    public void update(QueryHistory queryHistory) {
        this.totalCount ++;
        if (!queryHistory.isException())
            this.successCount ++;
        this.totalDuration += queryHistory.getDuration();
        this.lastQueryTime = queryHistory.getQueryTime();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (this.getClass() != obj.getClass())
            return false;

        FavoriteQuery that = (FavoriteQuery) obj;

        return this.sqlPattern.equals(that.getSqlPattern()) && this.project.equals(that.getProject());
    }

    @Override
    public int hashCode() {
        return (this.project + this.sqlPattern).hashCode();
    }
}
