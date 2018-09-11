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
import org.apache.kylin.common.persistence.RootPersistentEntity;

public class FavoriteQuery extends RootPersistentEntity {

    @JsonProperty("sql")
    private String sql;
    @JsonProperty("last_query_time")
    private long lastQueryTime;
    @JsonProperty("success_rate")
    private float successRate;
    @JsonProperty("frequency")
    private int frequency;
    @JsonProperty("average_duration")
    private float averageDuration;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("status")
    private FavoriteQueryStatusEnum status;

    public FavoriteQuery() {
        updateRandomUuid();
    }

    public FavoriteQuery(final String sql) {
        this.updateRandomUuid();
        this.sql = sql;
        this.setStatus(FavoriteQueryStatusEnum.WAITING);
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getLastQueryTime() {
        return lastQueryTime;
    }

    public void setLastQueryTime(long lastQueryTime) {
        this.lastQueryTime = lastQueryTime;
    }

    public float getSuccessRate() {
        return successRate;
    }

    public void setSuccessRate(float successRate) {
        this.successRate = successRate;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public float getAverageDuration() {
        return averageDuration;
    }

    public void setAverageDuration(float averageDuration) {
        this.averageDuration = averageDuration;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public FavoriteQueryStatusEnum getStatus() {
        return status;
    }

    public void setStatus(FavoriteQueryStatusEnum status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        FavoriteQuery that = (FavoriteQuery) o;

        return sql.equals(that.sql);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + sql.hashCode();
        return result;
    }
}
