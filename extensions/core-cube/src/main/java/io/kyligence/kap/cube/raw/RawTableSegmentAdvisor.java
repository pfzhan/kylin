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

import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ISegmentAdvisor;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TimeRange;

import com.google.common.base.Preconditions;

public class RawTableSegmentAdvisor implements ISegmentAdvisor {

    private final RawTableSegment seg;

    // these are just cache of segment attributes, all changes must write through to 'seg'
    private TimeRange tsRange;
    private SegmentRange segRange;

    public RawTableSegmentAdvisor(ISegment segment) {
        this.seg = (RawTableSegment) segment;
    }

    @Override
    public boolean isOffsetCube() {
        return seg._getSourceOffsetStart() != 0 || seg._getSourceOffsetEnd() != 0;
    }

    @Override
    public SegmentRange getSegRange() {
        if (segRange != null)
            return segRange;

        // backward compatible with pre-streaming metadata, TSRange can imply SegmentRange
        segRange = isOffsetCube() //
                ? new SegmentRange.KafkaOffsetPartitionedSegmentRange(seg._getSourceOffsetStart(),
                        seg._getSourceOffsetEnd(), null, null) //
                : new SegmentRange.TimePartitionedSegmentRange(seg._getDateRangeStart(), seg._getDateRangeEnd());

        return segRange;
    }

    @Override
    public void setSegRange(SegmentRange range) {
        // backward compatible with pre-streaming metadata, TSRange can imply SegmentRange
        Preconditions.checkNotNull(range);

        if (range instanceof SegmentRange.TimePartitionedSegmentRange) {
            SegmentRange.TimePartitionedSegmentRange tpsr = (SegmentRange.TimePartitionedSegmentRange) range;
            seg._setDateRangeStart(tpsr.getStart());
            seg._setDateRangeEnd(tpsr.getEnd());
        } else if (range instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange) {
            SegmentRange.KafkaOffsetPartitionedSegmentRange kpsr = (SegmentRange.KafkaOffsetPartitionedSegmentRange) range;
            seg._setSourceOffsetStart(kpsr.getStart());
            seg._setSourceOffsetEnd(kpsr.getEnd());
            //            seg._setSourcePartitionOffsetStart(kpsr.getSourcePartitionOffsetStart());
            //            seg._setSourcePartitionOffsetEnd(kpsr.getSourcePartitionOffsetEnd());
        } else {
            throw new IllegalArgumentException();
        }
        clear();
    }

    @Override
    public TimeRange getTSRange() {
        if (tsRange != null)
            return tsRange;

        tsRange = new TimeRange(seg._getDateRangeStart(), seg._getDateRangeEnd());
        return tsRange;
    }

    @Override
    public void setTSRange(TimeRange range) {
        if (range == null) {
            seg._setDateRangeStart(0);
            seg._setDateRangeEnd(0);
        } else {
            seg._setDateRangeStart(range.getStart());
            seg._setDateRangeEnd(range.getEnd());
        }
        clear();
    }

    private void clear() {
        tsRange = null;
        segRange = null;
    }

}
