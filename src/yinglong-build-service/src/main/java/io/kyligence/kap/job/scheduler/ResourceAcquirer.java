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

package io.kyligence.kap.job.scheduler;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.SystemInfoCollector;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.AbstractJobExecutable;

public class ResourceAcquirer {

    private static final Logger logger = LoggerFactory.getLogger(ResourceAcquirer.class);

    private final AbstractJobConfig jobConfig;

    private final Semaphore memorySemaphore;

    private final AtomicInteger accumulator;

    private final ConcurrentMap<String, NodeResource> registers;

    public ResourceAcquirer(AbstractJobConfig jobConfig) {
        this.jobConfig = jobConfig;

        double memoryRatio = jobConfig.getMaxLocalNodeMemoryRatio();
        memorySemaphore = new Semaphore((int) (memoryRatio * SystemInfoCollector.getAvailableMemoryInfo()));

        accumulator = new AtomicInteger(0);
        registers = Maps.newConcurrentMap();
    }

    public boolean tryAcquire(AbstractJobExecutable jobExecutable) {
        int threshold = jobConfig.getNodeParallelJobCountThreshold();
        if (accumulator.incrementAndGet() > threshold) {
            int c = accumulator.decrementAndGet();
            logger.info("Acquire failed with node parallel job count: {}, threshold {}", c, threshold);
            return false;
        }
        NodeResource resource = new NodeResource(jobExecutable);
        boolean acquired = memorySemaphore.tryAcquire(resource.getMemory());
        if (acquired) {
            registers.put(jobExecutable.getJobId(), resource);
            logger.info("Acquire resource success {}, available: {}MB", resource, memorySemaphore.availablePermits());
            return true;
        }
        logger.warn("Acquire resource failed {}, available: {}MB", resource, memorySemaphore.availablePermits());
        accumulator.decrementAndGet();
        return false;
    }

    public void release(AbstractJobExecutable jobExecutable) {
        accumulator.decrementAndGet();
        String jobId = jobExecutable.getJobId();
        NodeResource resource = registers.get(jobId);
        if (Objects.isNull(resource)) {
            logger.warn("Cannot find job's registered resource: {}", jobId);
            return;
        }
        memorySemaphore.release(resource.getMemory());
        registers.remove(jobExecutable.getJobId());
    }

    public void start() {
        // do nothing
    }

    public void destroy() {
        // do nothing
    }
}
