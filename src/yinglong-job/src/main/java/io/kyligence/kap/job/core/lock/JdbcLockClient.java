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

package io.kyligence.kap.job.core.lock;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.ThreadUtils;
import com.google.common.collect.Maps;

import io.kyligence.kap.job.JobContext;

public class JdbcLockClient {

    private static final Logger logger = LoggerFactory.getLogger(JdbcLockClient.class);

    private final JobContext jobContext;

    private ScheduledExecutorService scheduler;

    private Map<String, JdbcJobLock> renewalMap;

    public JdbcLockClient(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    public void start() {
        scheduler = ThreadUtils.newDaemonThreadScheduledExecutor(
                jobContext.getJobConfig().getJdbcJobLockClientMaxThreads(), "JdbcJobLockClient");

        renewalMap = Maps.newConcurrentMap();
    }

    public void destroy() {
        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
        }

        if (Objects.nonNull(renewalMap)) {
            renewalMap = null;
        }
    }

    public boolean tryAcquire(JdbcJobLock jobLock) throws LockException {
        boolean acquired = tryAcquireInternal(jobLock);
        if (acquired) {
            // register renewal
            renewalMap.put(jobLock.getLockId(), jobLock);
            // auto renewal
            scheduler.schedule(() -> renewal(jobLock), jobLock.getRenewalDelaySec(), TimeUnit.SECONDS);
        }
        return acquired;
    }

    public boolean tryRelease(JdbcJobLock jobLock) throws LockException {
        try {
            int r = jobContext.getJobLockMapper().removeLock(jobLock.getLockId(), jobLock.getLockNode());
            return r > 0;
        } catch (Exception e) {
            throw new LockException("Release lock failed", e);
        } finally {
            renewalMap.remove(jobLock.getLockId());
        }
    }

    private void renewal(JdbcJobLock jobLock) {
        if (!renewalMap.containsKey(jobLock.getLockId())) {
            logger.info("Renewal skip released lock {}", jobLock.getLockId());
            return;
        }
        boolean acquired = false;
        try {
            acquired = tryAcquireInternal(jobLock);
        } catch (LockException e) {
            logger.error("Renewal lock failed", e);
        }
        if (acquired) {
            // schedule next renewal automatically
            scheduler.schedule(() -> renewal(jobLock), jobLock.getRenewalDelaySec(), TimeUnit.SECONDS);
        }
    }

    private boolean tryAcquireInternal(JdbcJobLock jobLock) throws LockException {
        boolean acquired = false;
        try {
            int r = jobContext.getJobLockMapper().upsertLock(jobLock.getLockId(), jobLock.getLockNode(),
                    jobLock.getRenewalSec());
            acquired = r > 0;
            return acquired;
        } catch (Exception e) {
            throw new LockException("Acquire lock failed", e);
        } finally {
            if (acquired) {
                jobLock.getAcquireListener().onSucceed();
            } else {
                jobLock.getAcquireListener().onFailed();
            }
        }
    }

}
