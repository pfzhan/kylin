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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.query.QueryHistory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class MockedQueryHistoryService extends QueryHistoryService {
    private static final String PROJECT = "default";
    // current time is 2018-02-01 00:00:00
    private long currentTime;

    private List<QueryHistory> overallQueryHistories = Lists.newArrayList();

    public MockedQueryHistoryService() {
        String currentDate = "2018-02-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            currentTime = format.parse(currentDate).getTime();
        } catch (ParseException e) {
            // ignore
        }

        // these are expected to be marked as favorite queries
        for (int i = 0; i < 6; i++) {
            QueryHistory queryHistory = new QueryHistory("sql_pattern" + i, PROJECT,
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistory.setInsertTime(currentTime + 30 * i * 1000L);
            if (i == 4)
                queryHistory.setSqlPattern("SELECT *\nFROM \"TEST_COUNTRY\"\nLIMIT 1");
            if (i == 5)
                queryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
            overallQueryHistories.add(queryHistory);
        }

        // These are three sql patterns that are already loaded in database
        for (int i = 0; i < 3; i++) {
            QueryHistory queryHistoryForUpdate = new QueryHistory("sql" + (i+1), PROJECT,
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistoryForUpdate.setInsertTime(currentTime + 30 * i * 1000L);
            overallQueryHistories.add(queryHistoryForUpdate);
        }
    }

    @Override
    public List<QueryHistory> getQueryHistories(long startTime, long endTime) {
        List<QueryHistory> queryHistories = Lists.newArrayList();

        for (int i = 0; i < overallQueryHistories.size(); i++) {
            QueryHistory queryHistory = overallQueryHistories.get(i);
            if (queryHistory.getInsertTime() >= startTime && queryHistory.getInsertTime() < endTime)
                queryHistories.add(queryHistory);
        }

        return queryHistories;
    }

    public void insert(QueryHistory queryHistory) {
        overallQueryHistories.add(queryHistory);
    }

    public long getCurrentTime() {
        return currentTime;
    }
}
