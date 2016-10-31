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

package io.kyligence.kap.cube.raw;

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

public class RawTableUpdate {
    private RawTableInstance rawTableInstance;
    private RawTableSegment[] toAddSegs = null;
    private RawTableSegment[] toRemoveSegs = null;
    private RawTableSegment[] toUpdateSegs = null;
    private RealizationStatusEnum status;
    private String owner;
    private int cost = -1;

    public RawTableUpdate(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
    }

    public RawTableInstance getRawTableInstance() {
        return rawTableInstance;
    }

    public RawTableUpdate setRawTableInstance(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
        return this;
    }

    public RawTableSegment[] getToAddSegs() {
        return toAddSegs;
    }

    public RawTableUpdate setToAddSegs(RawTableSegment... toAddSegs) {
        this.toAddSegs = toAddSegs;
        return this;
    }

    public RawTableSegment[] getToRemoveSegs() {
        return toRemoveSegs;
    }

    public RawTableUpdate setToRemoveSegs(RawTableSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public RawTableSegment[] getToUpdateSegs() {
        return toUpdateSegs;
    }

    public RawTableUpdate setToUpdateSegs(RawTableSegment... toUpdateSegs) {
        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public RawTableUpdate setStatus(RealizationStatusEnum status) {
        this.status = status;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public RawTableUpdate setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public int getCost() {
        return cost;
    }

    public RawTableUpdate setCost(int cost) {
        this.cost = cost;
        return this;
    }
}
