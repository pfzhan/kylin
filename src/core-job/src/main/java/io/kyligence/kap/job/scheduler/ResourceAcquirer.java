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

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.SystemInfoCollector;
import io.kyligence.kap.job.core.AbstractJobExecutable;

public class ResourceAcquirer {

    private static final Logger logger = LoggerFactory.getLogger(ResourceAcquirer.class);

    private KylinConfig kylinConfig;

    private final ConcurrentMap<String, NodeResource> registers;

    private static volatile Semaphore memorySemaphore = new Semaphore(Integer.MAX_VALUE);;

    public ResourceAcquirer(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;

        if (kylinConfig.getAutoSetConcurrentJob()) {
            double memoryRatio = kylinConfig.getMaxLocalConsumptionRatio();
            if (Integer.MAX_VALUE == memorySemaphore.availablePermits()) {
                memorySemaphore = new Semaphore((int) (memoryRatio * SystemInfoCollector.getAvailableMemoryInfo()));
            }
        }
        registers = Maps.newConcurrentMap();
    }

    public boolean tryAcquire(AbstractJobExecutable jobExecutable) {
        NodeResource resource = new NodeResource(jobExecutable);
        boolean acquired = memorySemaphore.tryAcquire(resource.getMemory());
        if (acquired) {
            registers.put(jobExecutable.getJobId(), resource);
            logger.info("Acquire resource success {}, available: {}MB", resource, memorySemaphore.availablePermits());
            return true;
        }
        logger.warn("Acquire resource failed {}, available: {}MB", resource, memorySemaphore.availablePermits());
        return false;
    }

    public void release(AbstractJobExecutable jobExecutable) {
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
