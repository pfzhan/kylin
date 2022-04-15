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

package io.kyligence.kap.metadata.favorite;

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class AsyncTaskManagerTest extends NLocalFileMetadataTestCase {

    private String getProject() {
        return "gc_test";
    }

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testSave() {
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getTestConfig(), getProject());
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
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getTestConfig(), getProject());
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
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getTestConfig(), getProject());
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
