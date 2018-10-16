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
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Measurement;

import java.util.List;

public class QueryStatisticsResponse {

    @JsonProperty("amount")
    private int amount;

    @JsonProperty("HIVE")
    private QueryStatistics hive = new QueryStatistics("HIVE");

    @JsonProperty("RDBMS")
    private QueryStatistics rdbms = new QueryStatistics("RDBMS");

    @JsonProperty("Agg Index")
    private QueryStatistics aggIndex = new QueryStatistics("Agg Index");

    @JsonProperty("Table Index")
    private QueryStatistics tableIndex = new QueryStatistics("Table Index");

    public static QueryStatisticsResponse valueOf(List<QueryStatistics> statistics) {
        final QueryStatisticsResponse response = new QueryStatisticsResponse();
        for (final QueryStatistics queryStatistics : statistics) {
            response.amount += queryStatistics.getCount();
        }

        for (final QueryStatistics other : statistics) {
            other.updateRatio((double) response.amount);

            final QueryStatistics origin = getOrigin(other, response);
            if (origin != null) {
                origin.apply(other);
            }
        }

        return response;
    }

    private static QueryStatistics getOrigin(QueryStatistics other, QueryStatisticsResponse response) {
        switch (other.getEngineType()) {
        case "HIVE":
            return response.hive;
        case "RDBMS":
            return response.rdbms;
        case "Agg Index":
            return response.aggIndex;
        case "Table Index":
            return response.tableIndex;
        default:
            return null;
        }
    }

    public int getAmount() {
        return amount;
    }

    public QueryStatistics getHive() {
        return hive;
    }

    public QueryStatistics getRdbms() {
        return rdbms;
    }

    public QueryStatistics getAggIndex() {
        return aggIndex;
    }

    public QueryStatistics getTableIndex() {
        return tableIndex;
    }

    @Measurement(name = "query_metric")
    public static class QueryStatistics {

        @JsonProperty("engine_type")
        @Column(name = "engine_type", tag = true)
        private String engineType;

        @JsonProperty("count")
        @Column(name = "count")
        private int count;

        @JsonProperty("ratio")
        private double ratio;

        @JsonProperty("mean")
        @Column(name = "mean")
        private double meanDuration;

        public QueryStatistics() {
        }

        private QueryStatistics(String engineType) {
            this.engineType = engineType;
        }

        private void apply(final QueryStatistics other) {
            this.count = other.count;
            this.ratio = other.ratio;
            this.meanDuration = other.meanDuration;
        }

        private void updateRatio(double amount) {
            if (amount > 0d) {
                // Keep two decimals
                this.ratio = ((double) Math.round(((double) count) / amount * 100d)) / 100d;
            }
        }

        public String getEngineType() {
            return engineType;
        }

        public void setEngineType(String engineType) {
            this.engineType = engineType;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public double getMeanDuration() {
            return meanDuration;
        }

        public void setMeanDuration(double meanDuration) {
            this.meanDuration = meanDuration;
        }

        public double getRatio() {
            return ratio;
        }

        public void setRatio(double ratio) {
            this.ratio = ratio;
        }
    }
}
