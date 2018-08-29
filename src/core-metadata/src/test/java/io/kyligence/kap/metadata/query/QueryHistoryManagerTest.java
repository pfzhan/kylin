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

package io.kyligence.kap.metadata.query;

import com.google.common.base.Predicate;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryHistoryManagerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private final String QUERY = "da0c9cad-35c1-4f4b-8c10-669248842c2f";
    private final String FAVORITE_QUERY = "bd3285c9-55e3-4f2d-a12c-742a8d631195";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        List<QueryHistory> queryHistories = QueryHistoryManager.getInstance(getTestConfig(), PROJECT)
                .getAllQueryHistories();

        assertEquals(4, queryHistories.size());

        QueryHistory entry1 = queryHistories.get(0);
        assertEquals("Pushdown", entry1.getRealization());
        assertEquals("sandbox.hortonworks.com", entry1.getQueryNode());
        assertEquals("select * from test_kylin_fact limit 10", entry1.getSql());

        QueryHistory entry2 = queryHistories.get(1);
        assertTrue(entry2.getStartTime() > entry1.getStartTime());
    }

    @Test
    public void testAddEntryToProject() throws IOException {
        QueryHistoryManager manager = QueryHistoryManager.getInstance(getTestConfig(), PROJECT);
        QueryHistory entry = new QueryHistory("query-1", "sql", 1459362239992L, 100, "server", "t-0",
                "ADMIN");
        entry.setRealization("pushdown");
        entry.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        manager.upsertEntry(entry);
        List<QueryHistory> entries = manager.getAllQueryHistories();
        assertEquals(5, entries.size());

        QueryHistory newEntry = entries.get(entries.size() - 1);

        assertEquals("sql", newEntry.getSql());
        assertEquals(1459362239992L, newEntry.getStartTime());
        assertEquals("server", newEntry.getQueryNode());
        assertEquals("ADMIN", newEntry.getUser());
        assertEquals("t-0", newEntry.getThread());

    }

    @Test
    public void testFindQueryHistory() throws IOException {
        QueryHistoryManager manager = QueryHistoryManager.getInstance(getTestConfig(), PROJECT);
        QueryHistory queryHistory = manager.findQueryHistory(QUERY);

        assertNotNull(queryHistory);
        assertEquals("select * from test_kylin_fact limit 10", queryHistory.getSql());
        assertEquals("Pushdown", queryHistory.getRealization());
        assertEquals("query-3", queryHistory.getQueryId());

        QueryHistory favoritedQuery = manager.findQueryHistory(new Predicate<QueryHistory>() {
            @Override
            public boolean apply(@Nullable QueryHistory favoritedQuery) {
                return FAVORITE_QUERY.equals(favoritedQuery.getFavorite());
            }
        });

        assertNotNull(favoritedQuery);
        assertEquals(FAVORITE_QUERY, favoritedQuery.getFavorite());
        assertEquals("query-1", favoritedQuery.getQueryId());
    }

}
