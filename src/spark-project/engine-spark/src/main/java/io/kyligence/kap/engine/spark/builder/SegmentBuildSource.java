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

public class SegmentBuildSource {

    private static final long NA = -1L;

    // Waiting to build.
    protected final LayoutEntity layout;

    // Build from potential parent layout.
    // Coupled with the segment reference.
    protected LayoutEntity parent;

    // Remember the segment reference.
    // Coupled with the parent layout.
    private NDataSegment dataSegment;

    // Parent source is flat table.
    protected final boolean isFT;

    public SegmentBuildSource(LayoutEntity layout) {
        Preconditions.checkNotNull(layout);
        this.layout = layout;
        this.isFT = true;
    }

    public SegmentBuildSource(LayoutEntity layout, // 
            LayoutEntity parent, NDataSegment dataSegment) {
        Preconditions.checkNotNull(layout);
        Preconditions.checkNotNull(parent);
        Preconditions.checkNotNull(dataSegment);
        // TODO Heavy check. Preconditions.checkArgument(parent.getIndex().fullyDerive(layout.getIndex()));
        this.layout = layout;
        this.parent = parent;
        this.dataSegment = dataSegment;
        this.isFT = false;
    }

    public static SegmentBuildSource newFlatTableSource(LayoutEntity layout) {
        return new SegmentBuildSource(layout);
    }

    public static SegmentBuildSource newLayoutSource(LayoutEntity layout, //
            LayoutEntity parent, NDataSegment dataSegment) {
        return new SegmentBuildSource(layout, parent, dataSegment);
    }

    public LayoutEntity getLayout() {
        return this.layout;
    }

    public LayoutEntity getParent() {
        return this.parent;
    }

    public NDataSegment getDataSegment() {
        return this.dataSegment;
    }

    public boolean isFlatTable() {
        return this.isFT;
    }

    public long getLayoutId() {
        return this.layout.getId();
    }

    public long getParentId() {
        return this.isFT ? NA : parent.getId();
    }

    public String readableDesc() {
        StringBuilder sb = new StringBuilder("Build layout ").append(layout.getId()).append(" from ");
        if (isFT) {
            sb.append("flat table");
        } else {
            sb.append(parent.getId());
        }
        return sb.toString();
    }

}
