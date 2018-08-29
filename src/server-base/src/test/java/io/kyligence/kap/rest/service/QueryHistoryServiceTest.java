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
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class QueryHistoryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private List<QueryHistory> mockQueryHistories() {
        QueryHistory query1 = new QueryHistory("query-1", "select * from test_table_1", 1, 1000, "", "", "");
        QueryHistory query2 = new QueryHistory("query-2", "select * from test_table_1", 2, 1000, "", "", "");
        QueryHistory query3 = new QueryHistory("query-3", "select * from test_table_1", 3, 100, "", "", "");
        query3.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        QueryHistory query4 = new QueryHistory("query-4", "select * from test_table_2", 4, 100, "", "", "");
        query4.setQueryStatus(QueryHistoryStatusEnum.FAILED);
        QueryHistory query5 = new QueryHistory("query-5", "select * from test_table_1", 5, 100, "", "", "");
        query5.setQueryStatus(QueryHistoryStatusEnum.FAILED);

        return Lists.newArrayList(query1, query2, query3, query4, query5);
    }

    private QueryHistoryFilterRule prepareRules() {
        QueryHistoryFilterRule.QueryHistoryCond cond1 = new QueryHistoryFilterRule.QueryHistoryCond();
        cond1.setOp(QueryHistoryFilterRule.QueryHistoryCond.Operation.GREATER);
        cond1.setField("startTime");
        cond1.setThreshold("1");

        QueryHistoryFilterRule.QueryHistoryCond cond2 = new QueryHistoryFilterRule.QueryHistoryCond();
        cond2.setOp(QueryHistoryFilterRule.QueryHistoryCond.Operation.LESS);
        cond2.setField("latency");
        cond2.setThreshold("1000");

        QueryHistoryFilterRule.QueryHistoryCond cond3 = new QueryHistoryFilterRule.QueryHistoryCond();
        cond3.setOp(QueryHistoryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond3.setField("queryStatus");
        cond3.setThreshold("FAILED");

        QueryHistoryFilterRule.QueryHistoryCond cond4 = new QueryHistoryFilterRule.QueryHistoryCond();
        cond4.setOp(QueryHistoryFilterRule.QueryHistoryCond.Operation.CONTAIN);
        cond4.setField("sql");
        cond4.setThreshold("test_table_1");

        List<QueryHistoryFilterRule.QueryHistoryCond> conds = Lists.newArrayList(cond1, cond2, cond3, cond4);

        return new QueryHistoryFilterRule(conds);
    }

    @Test
    public void testFilterRule() throws IOException {
        Mockito.when(queryHistoryService.getQueryHistories(PROJECT)).thenReturn(mockQueryHistories());

        List<QueryHistory> queryHistories = queryHistoryService.getQueryHistoriesByRules(PROJECT, prepareRules());

        Assert.assertEquals(1, queryHistories.size());
        Assert.assertEquals("query-5", queryHistories.get(0).getQueryId());
    }
}
