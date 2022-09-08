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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.favorite.QueryHistoryTimeOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryTimeOffsetManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class QueryHistoryTimeOffsetManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        // test save time offset
        QueryHistoryTimeOffsetManager manager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), "default");
        QueryHistoryTimeOffset timeOffset = new QueryHistoryTimeOffset(1000, 1000);
        manager.save(timeOffset);

        timeOffset = manager.get();
        Assert.assertEquals(1000L, timeOffset.getAutoMarkTimeOffset());
        Assert.assertEquals(1000L, timeOffset.getFavoriteQueryUpdateTimeOffset());

        // test update time offset
        timeOffset.setAutoMarkTimeOffset(999);
        timeOffset.setFavoriteQueryUpdateTimeOffset(999);

        manager.save(timeOffset);

        timeOffset = manager.get();

        Assert.assertEquals(999, timeOffset.getAutoMarkTimeOffset());
        Assert.assertEquals(999, timeOffset.getFavoriteQueryUpdateTimeOffset());
    }

    @Test
    public void testProjectHasNoTimeOffsetData() {
        QueryHistoryTimeOffsetManager manager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), "newten");
        QueryHistoryTimeOffset timeOffset = manager.get();
        Assert.assertNotNull(timeOffset);
        Assert.assertNotNull(timeOffset.getAutoMarkTimeOffset());
        Assert.assertNotNull(timeOffset.getFavoriteQueryUpdateTimeOffset());
    }
}
