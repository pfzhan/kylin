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

import java.util.Map;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class AsyncTaskManagerTest extends NLocalFileMetadataTestCase {

    private JdbcTemplate jdbcTemplate;
    private String getProject() {
        return "gc_test";
    }

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
    public void testSave() {
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getProject());
        AsyncAccelerationTask task = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> userRefreshedTagMap = task.getUserRefreshedTagMap();
        Assert.assertFalse(task.isAlreadyRunning());
        Assert.assertTrue(userRefreshedTagMap.isEmpty());

        // validate async-acceleration-task state
        task.setAlreadyRunning(true);
        userRefreshedTagMap.putIfAbsent("admin", true);
        userRefreshedTagMap.putIfAbsent("kylin", false);
        instance.save(task);
        AsyncAccelerationTask modified = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> modifiedMap = modified.getUserRefreshedTagMap();
        Assert.assertTrue(modified.isAlreadyRunning());
        Assert.assertTrue(modifiedMap.get("admin"));
        Assert.assertFalse(modifiedMap.get("kylin"));
    }

    @Test
    public void testResetAccelerationTagMap() {
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getProject());
        AsyncAccelerationTask task = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> userRefreshedTagMap = task.getUserRefreshedTagMap();
        Assert.assertFalse(task.isAlreadyRunning());
        Assert.assertTrue(userRefreshedTagMap.isEmpty());

        task.setAlreadyRunning(true);
        userRefreshedTagMap.putIfAbsent("admin", true);
        userRefreshedTagMap.putIfAbsent("kylin", true);
        userRefreshedTagMap.putIfAbsent("root", true);
        instance.save(task);
        AsyncAccelerationTask modifiedTask = (AsyncAccelerationTask) instance
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> modifiedMap = modifiedTask.getUserRefreshedTagMap();
        Assert.assertEquals(3, modifiedMap.size());

        AsyncTaskManager.resetAccelerationTagMap(getProject());
        AsyncAccelerationTask reset = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Assert.assertTrue(reset.getUserRefreshedTagMap().isEmpty());
    }

    @Test
    public void testCleanAccelerationTagByUser() {
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getProject());
        AsyncAccelerationTask task = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> userRefreshedTagMap = task.getUserRefreshedTagMap();
        Assert.assertFalse(task.isAlreadyRunning());
        Assert.assertTrue(userRefreshedTagMap.isEmpty());

        task.setAlreadyRunning(true);
        userRefreshedTagMap.putIfAbsent("admin", true);
        userRefreshedTagMap.putIfAbsent("kylin", true);
        userRefreshedTagMap.putIfAbsent("root", true);
        instance.save(task);
        AsyncAccelerationTask modifiedTask = (AsyncAccelerationTask) instance
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> modifiedMap = modifiedTask.getUserRefreshedTagMap();
        Assert.assertEquals(3, modifiedMap.size());
        modifiedMap.forEach((user, tag) -> Assert.assertTrue(tag));

        AsyncTaskManager.cleanAccelerationTagByUser(getProject(), "admin");
        AsyncAccelerationTask cleaned = (AsyncAccelerationTask) instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        Map<String, Boolean> cleanedMap = cleaned.getUserRefreshedTagMap();
        Assert.assertFalse(cleanedMap.get("admin"));
        Assert.assertTrue(cleanedMap.get("kylin"));
        Assert.assertTrue(cleanedMap.get("root"));
    }
}
