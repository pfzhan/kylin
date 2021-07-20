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

package io.kyligence.kap.engine.spark.builder;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class MLPBuildSource extends SegmentBuildSource {

    private final Long partitionId;

    public MLPBuildSource(Long partitionId, //
            LayoutEntity layout, LayoutEntity parent, NDataSegment dataSegment) {
        super(layout, parent, dataSegment);
        Preconditions.checkNotNull(partitionId);
        this.partitionId = partitionId;
    }

    public static MLPBuildSource newFlatTableSource(Long partitionId, LayoutEntity layout, NDataSegment dataSegment) {
        return new MLPBuildSource(partitionId, layout, null, dataSegment);
    }

    public static MLPBuildSource newLayoutSource(Long partitionId, //
            LayoutEntity layout, LayoutEntity parent, NDataSegment dataSegment) {
        return new MLPBuildSource(partitionId, layout, parent, dataSegment);
    }

    public Long getPartitionId() {
        return this.partitionId;
    }

    public String readableDesc() {
        return super.readableDesc() + " partition " + partitionId;
    }
}