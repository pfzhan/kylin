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

import java.util.concurrent.atomic.AtomicInteger;

import io.kyligence.kap.job.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelLimiter {

    private static final Logger logger = LoggerFactory.getLogger(ParallelLimiter.class);

    private final JobContext jobContext;

    private final AtomicInteger accumulator;

    public ParallelLimiter(JobContext jobContext) {
        this.jobContext = jobContext;
        accumulator = new AtomicInteger(0);
    }

    public boolean tryAcquire() {
        int threshold = jobContext.getJobConfig().getParallelJobCountThreshold();
        if (accumulator.getAndIncrement() < threshold) {
            return true;
        }

        int c = accumulator.decrementAndGet();
        logger.info("Acquire failed with parallel job count: {}, threshold {}", c, threshold);
        return false;
    }

    public boolean tryRelease() {
        // exclude master lock
        int c = jobContext.getJobLockMapper().findCount() - 1;
        int threshold = jobContext.getJobConfig().getParallelJobCountThreshold();
        if (c < threshold) {
            accumulator.set(c);
            return true;
        }
        logger.info("Release failed with parallel job count: {}, threshold: {}", c, threshold);
        return false;
    }

    public void start() {
        // do nothing
    }

    public void destroy() {
        // do nothing
    }
}
