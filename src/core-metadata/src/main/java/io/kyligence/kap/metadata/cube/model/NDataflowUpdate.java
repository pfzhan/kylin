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

package io.kyligence.kap.metadata.cube.model;

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoArgsConstructor
public class NDataflowUpdate {

    @Getter
    private String dataflowId;

    @Getter
    private NDataSegment[] toAddSegs = null;
    @Getter
    private NDataSegment[] toRemoveSegs = null;
    @Getter
    private NDataSegment[] toUpdateSegs = null;

    @Getter
    private NDataLayout[] toAddOrUpdateLayouts = null;
    @Getter
    private NDataLayout[] toRemoveLayouts = null;

    @Accessors(chain = true)
    @Setter
    @Getter
    private RealizationStatusEnum status;

    @Accessors(chain = true)
    @Setter
    @Getter
    private int cost = -1;

    public NDataflowUpdate(String dataflowId) {
        this.dataflowId = dataflowId;
    }

    public NDataflowUpdate setToAddSegs(NDataSegment... toAddSegs) {
        if (toAddSegs != null) {
            for (NDataSegment seg : toAddSegs)
                seg.checkIsNotCachedAndShared();
        }

        this.toAddSegs = toAddSegs;
        return this;
    }

    public NDataflowUpdate setToRemoveSegs(NDataSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataflowUpdate setToRemoveSegsWithArray(NDataSegment[] toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataflowUpdate setToUpdateSegs(NDataSegment... toUpdateSegs) {
        if (toUpdateSegs != null) {
            for (NDataSegment seg : toUpdateSegs)
                seg.checkIsNotCachedAndShared();
        }

        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public void setToAddOrUpdateLayouts(NDataLayout... toAddCuboids) {
        if (toAddCuboids != null) {
            for (NDataLayout cuboid : toAddCuboids)
                cuboid.checkIsNotCachedAndShared();
        }

        this.toAddOrUpdateLayouts = toAddCuboids;
    }

    public void setToRemoveLayouts(NDataLayout... toRemoveLayouts) {
        this.toRemoveLayouts = toRemoveLayouts;
    }

}
