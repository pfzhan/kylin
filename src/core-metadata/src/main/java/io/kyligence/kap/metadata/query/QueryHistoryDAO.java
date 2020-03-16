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

package io.kyligence.kap.metadata.query;

import java.util.List;

public interface QueryHistoryDAO {

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset, String project);

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project);

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project);

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension, String project);

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project);

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension, String project);

    public String getQueryMetricMeasurement();

    public void deleteQueryHistoriesIfMaxSizeReached();

    public List<QueryHistory> getQueryHistoriesByTime(long startTime, long endTime, String project);

    public long getQueryHistoriesSize(QueryHistoryRequest request, String project);

    public String getRealizationMetricMeasurement();
}
