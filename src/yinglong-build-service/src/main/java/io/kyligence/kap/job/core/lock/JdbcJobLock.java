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

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class JdbcJobLock implements JobLock {

    private final String lockId;
    private final String lockNode;

    private final long renewalSec;
    private final long renewalDelaySec;

    private final JdbcLockClient lockClient;
    private final LockAcquireListener acquireListener;

    public JdbcJobLock(String lockId, String lockNode, long renewalSec, double renewalRatio, JdbcLockClient lockClient,
            LockAcquireListener acquireListener) {
        this.lockId = lockId;
        this.lockNode = lockNode;
        this.renewalSec = renewalSec;
        this.renewalDelaySec = (int) (renewalRatio * renewalSec);
        this.lockClient = lockClient;
        this.acquireListener = acquireListener;
    }

    @Override
    public boolean tryAcquire() throws LockException {
        return lockClient.tryAcquire(this);
    }

    public boolean tryAcquireRenewal() throws LockException {
        return lockClient.tryAcquireRenewal(this);
    }

    @Override
    public boolean tryAcquire(long time, TimeUnit unit) throws LockException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryRelease() throws LockException {
        return lockClient.tryRelease(this);
    }

    @Override
    public String toString() {
        return "JdbcJobLock{" + "lockId='" + lockId + '\'' + ", lockNode='" + lockNode + '\'' + '}';
    }

    public String getLockId() {
        return lockId;
    }

    public String getLockNode() {
        return lockNode;
    }

    public Date getNextExpireTime() {
        return new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(renewalSec));
    }

    public long getRenewalSec() {
        return renewalSec;
    }

    public long getRenewalDelaySec() {
        return renewalDelaySec;
    }

    public LockAcquireListener getAcquireListener() {
        return acquireListener;
    }
}
