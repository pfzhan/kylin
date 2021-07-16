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
package io.kyligence.kap.common.persistence.metadata;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;

public class NoopAuditLogStore implements AuditLogStore {
    @Override
    public void save(UnitMessages unitMessages) {
        // just implement it
    }

    @Override
    public List<AuditLog> fetch(long currentId, long size) {
        return Lists.newArrayList();
    }

    @Override
    public long getMaxId() {
        return 0;
    }

    @Override
    public long getMinId() {
        return 0;
    }

    @Override
    public long getLogOffset() {
        return 0;
    }

    @Override
    public void restore(long currentId) {
        // just implement it
    }

    @Override
    public void rotate() {
        // just implement it
    }

    @Override
    public void catchupWithTimeout() throws Exception {
        //do nothing
    }

    @Override
    public void catchup() {
        //do nothing
    }

    @Override
    public void forceCatchup() {
        //do nothing
    }


    @Override
    public void setInstance(String instance) {
        //do nothing
    }

    @Override
    public AuditLog get(String resPath, long mvcc) {
        return null;
    }

    @Override
    public void close() throws IOException {
        // just implement it
    }

}
