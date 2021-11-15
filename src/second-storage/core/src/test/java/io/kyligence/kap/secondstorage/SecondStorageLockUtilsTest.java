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

package io.kyligence.kap.secondstorage;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SecondStorageLockUtilsTest {

    @Test(expected = ExecutionException.class)
    public void acquireLock() throws Exception {
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SegmentRange<Long> range2 = new SegmentRange.TimePartitionedSegmentRange(1L, 2L);
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Boolean> future = executorService.submit(() -> {
            SecondStorageLockUtils.acquireLock(modelId, range2).lock();
            return true;
        });
        future.get();
    }

    @Test
    public void testDoubleAcquireLock() throws Exception {
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        Assert.assertTrue(SecondStorageLockUtils.containsKey(modelId, range));
    }

    @Test(expected = IllegalStateException.class)
    public void testUnlockFailed() {
        SecondStorageLockUtils.unlock(RandomUtil.randomUUIDStr(), SegmentRange.TimePartitionedSegmentRange.createInfinite());
    }
}