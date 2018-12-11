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

package org.apache.kylin.common.persistence;

import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

/**
 */
public class VersionedRawResource {

    @Getter
    private RawResource rawResource;

    //CANNOT expose this!
    //In theory, mvcc is not necessary since we have project lock for all meta change,update,delete actions
    //we keep it just in case project lock protocol is breached somewhere
    private AtomicLong mvcc;

    public VersionedRawResource(RawResource rawResource) {
        this.mvcc = new AtomicLong(rawResource.getMvcc());
        this.rawResource = rawResource;
    }

    public VersionedRawResource(VersionedRawResource origin) {
        this.rawResource = origin.rawResource;
        this.mvcc = new AtomicLong(origin.mvcc.get());
    }

    public void update(RawResource r) {
        if (mvcc.compareAndSet(r.getMvcc() - 1, r.getMvcc())) {
            this.rawResource = r;
        } else {
            throw new VersionConflictException("Overwriting conflict " + r.getResPath() + ", expect old mvcc: "
                    + (r.getMvcc() - 1) + ", but found: " + mvcc.get());
        }
    }

    public long getMvcc(){
        return mvcc.get();
    }
}
