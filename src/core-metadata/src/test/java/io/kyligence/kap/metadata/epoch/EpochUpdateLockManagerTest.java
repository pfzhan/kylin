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
package io.kyligence.kap.metadata.epoch;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.AbstractJdbcMetadataTestCase;
import lombok.val;

public class EpochUpdateLockManagerTest extends AbstractJdbcMetadataTestCase {

    private String project = "test";

    @Test
    public void testGetLock() throws InterruptedException {
        val executorService = Executors.newFixedThreadPool(5);

        getTestConfig().setProperty("kylin.server.leader-race.heart-beat-timeout", "3");

        val lockList = Lists.newCopyOnWriteArrayList();

        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                lockList.add(EpochUpdateLockManager.getLock(project));
            });
        }

        val lockCache = EpochUpdateLockManager.getLock(project);

        Assert.assertTrue(lockList.stream().allMatch(x -> lockCache == x));

        Assert.assertEquals(EpochUpdateLockManager.getInstance().getLockCacheSize(), 1);

        TimeUnit.SECONDS.sleep(3);

        EpochUpdateLockManager.getLock("test2");

        Assert.assertEquals(EpochUpdateLockManager.getInstance().getLockCacheSize(), 2);

        //clean up cache that is expired
        EpochUpdateLockManager.cleanUp();

        Assert.assertEquals(EpochUpdateLockManager.getInstance().getLockCacheSize(), 1);
    }
}
