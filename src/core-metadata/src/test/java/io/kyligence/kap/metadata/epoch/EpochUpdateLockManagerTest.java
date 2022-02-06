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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.AbstractJdbcMetadataTestCase;
import lombok.val;

public class EpochUpdateLockManagerTest extends AbstractJdbcMetadataTestCase {

    private final String project = "test";

    @Test
    public void testGetLock() {
        val executorService = Executors.newFixedThreadPool(5);

        try {
            val lockList = Lists.newCopyOnWriteArrayList();

            for (int i = 0; i < 10; i++) {
                executorService.submit(() -> {
                    lockList.add(EpochUpdateLockManager.getLock(project));
                });
            }
            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            val lockCache = EpochUpdateLockManager.getLock(project);

            Assert.assertTrue(lockList.stream().allMatch(x -> lockCache == x));

            Assert.assertEquals(EpochUpdateLockManager.getInstance().getLockCacheSize(), 1);

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }

    @Test
    public void testGetLockBatch() {
        val executorService = Executors.newFixedThreadPool(2);

        try {
            val lockList = Lists.newCopyOnWriteArrayList();

            val projectList = Lists.<List<String>> newArrayList();
            projectList.add(Arrays.asList("p1", "p3", "p2", "p4"));
            projectList.add(Arrays.asList("p3", "p2", "p1", "p4"));

            for (List<String> locks : projectList) {
                executorService.submit(() -> {
                    lockList.add(EpochUpdateLockManager.getLock(locks));
                });
            }
            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            Assert.assertTrue(ArrayUtils.isEquals(lockList.get(0), lockList.get(1)));

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }

    @Test
    public void testExecuteLockBatch() {
        val executorService = Executors.newFixedThreadPool(3);
        try {

            val lockList = Lists.<List<String>> newArrayList();
            lockList.add(Arrays.asList("p1", "p2", "p3"));
            lockList.add(Arrays.asList("p2", "p3", "p4"));
            lockList.add(Arrays.asList("p3", "p4", "p2", "p3"));

            val resultList = Lists.<String> newCopyOnWriteArrayList();
            for (List<String> locks : lockList) {
                executorService.submit(() -> {
                    EpochUpdateLockManager.executeEpochWithLock(locks, () -> {
                        val r = String.join(",", locks);
                        resultList.add(r);
                        return r;
                    });
                });
            }

            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            Assert.assertEquals(resultList.size(), lockList.size());

        } catch (Exception e) {
            Assert.fail("test error," + Throwables.getRootCause(e).getMessage());
        }
    }
}