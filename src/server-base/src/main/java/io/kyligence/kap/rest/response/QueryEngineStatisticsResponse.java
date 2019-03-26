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
import io.kyligence.kap.metadata.query.QueryStatistics;

import java.util.List;

public class QueryEngineStatisticsResponse {

    @JsonProperty("amount")
    private int amount;

    @JsonProperty("HIVE")
    private QueryStatistics hive = new QueryStatistics("HIVE");

    @JsonProperty("RDBMS")
    private QueryStatistics rdbms = new QueryStatistics("RDBMS");

    @JsonProperty("NATIVE")
    private QueryStatistics nativeQuery = new QueryStatistics("NATIVE");

    public static QueryEngineStatisticsResponse valueOf(List<QueryStatistics> statistics) {
        final QueryEngineStatisticsResponse response = new QueryEngineStatisticsResponse();
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

    private static QueryStatistics getOrigin(QueryStatistics other, QueryEngineStatisticsResponse response) {
        switch (other.getEngineType()) {
        case "HIVE":
            return response.hive;
        case "RDBMS":
            return response.rdbms;
        case "NATIVE":
            return response.nativeQuery;
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

    public QueryStatistics getNativeQuery() {
        return nativeQuery;
    }
}
