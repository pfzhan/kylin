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



package io.kyligence.kap.rest.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import lombok.Setter;

public class MockedQueryHistoryDao extends RDBMSQueryHistoryDAO {
    // current time is 2018-01-02 00:00:00
    private long currentTime;

    @Setter
    private List<QueryHistory> overallQueryHistories = Lists.newArrayList();

    public MockedQueryHistoryDao() throws Exception {
        super();
        init();
    }

    private void init() {
        String currentDate = "2018-01-02";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        try {
            currentTime = format.parse(currentDate).getTime();
        } catch (ParseException e) {
            // ignore
        }

        // these are expected to be marked as favorite queries
        for (int i = 0; i < 6; i++) {
            QueryHistory queryHistory = new QueryHistory("select * from sql_pattern" + i,
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistory.setInsertTime(currentTime + 30 * i * 1000L);
            queryHistory.setEngineType("HIVE");
            QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
            queryHistoryInfo.setRealizationMetrics(Lists.newArrayList(
                    new QueryMetrics.RealizationMetrics("1", "Agg Index", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                            Lists.newArrayList()),
                    new QueryMetrics.RealizationMetrics("1", "Agg Index", "82fa7671-a935-45f5-8779-85703601f49a",
                            Lists.newArrayList())));
            queryHistory.setQueryHistoryInfo(queryHistoryInfo);
            if (i == 4)
                queryHistory.setSqlPattern("SELECT *\nFROM \"TEST_KYLIN_FACT\"");
            if (i == 5)
                queryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
            overallQueryHistories.add(queryHistory);
        }

        // These are three sql patterns that are already loaded in database
        for (int i = 0; i < 3; i++) {
            QueryHistory queryHistoryForUpdate = new QueryHistory("select * from sql" + (i + 1),
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistoryForUpdate.setInsertTime(currentTime + 30 * i * 1000L);
            queryHistoryForUpdate.setEngineType("HIVE");
            overallQueryHistories.add(queryHistoryForUpdate);
        }
    }

    public void insert(QueryHistory queryHistory) {
        overallQueryHistories.add(queryHistory);
    }

    public long getCurrentTime() {
        return currentTime;
    }
}
