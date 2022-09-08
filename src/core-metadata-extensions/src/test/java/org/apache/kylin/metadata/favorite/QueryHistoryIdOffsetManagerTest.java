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

package org.apache.kylin.metadata.favorite;

import static org.apache.kylin.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;


public class QueryHistoryIdOffsetManagerTest extends NLocalFileMetadataTestCase {

    private JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
    }

    @After
    public void cleanUp() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        cleanupTestMetadata();
    }

    @Test
    public void testSaveAndUpdateIdOffset() {
        // test save id offset
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance("default");
        manager.saveOrUpdate(new QueryHistoryIdOffset(12345L, ACCELERATE));

        Assert.assertEquals(12345L, manager.get(ACCELERATE).getOffset());

        // test update id offset
        QueryHistoryIdOffset queryHistoryIdOffset = manager.get(ACCELERATE);
        queryHistoryIdOffset.setOffset(45678L);
        manager.saveOrUpdate(queryHistoryIdOffset);

        Assert.assertEquals(45678L, manager.get(ACCELERATE).getOffset());
    }

    @Test
    public void testInitIdOffset() {
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance("newten");
        QueryHistoryIdOffset queryHistoryIdOffset = manager.get(ACCELERATE);
        Assert.assertNotNull(queryHistoryIdOffset);
        Assert.assertEquals(0, queryHistoryIdOffset.getOffset());
    }

}