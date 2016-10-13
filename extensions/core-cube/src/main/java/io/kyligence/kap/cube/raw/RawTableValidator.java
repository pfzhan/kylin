/**
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class RawTableValidator {
    private static final Logger logger = LoggerFactory.getLogger(RawTableValidator.class);

    /**
     * Validates:
     * - consistent isOffsetsOn()
     * - for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
     * - for all new segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
     * - for all new segments, sourceOffset SHOULD fit/connect another segments
     * - dateRange does not matter any more
     */
    public static void validate(Collection<RawTableSegment> segments) {
        if (segments == null || segments.isEmpty())
            return;

        // make a copy, don't modify given list
        List<RawTableSegment> all = Lists.newArrayList(segments);
        Collections.sort(all);

        // check consistent isOffsetsOn()
        boolean isOffsetsOn = all.get(0).isSourceOffsetsOn();
        for (RawTableSegment seg : all) {
            seg.validate();
            if (seg.isSourceOffsetsOn() != isOffsetsOn)
                throw new IllegalStateException("Inconsistent isOffsetsOn for segment " + seg);
        }

        List<RawTableSegment> ready = Lists.newArrayListWithCapacity(all.size());
        List<RawTableSegment> news = Lists.newArrayListWithCapacity(all.size());
        for (RawTableSegment seg : all) {
            if (seg.getStatus() == SegmentStatusEnum.READY)
                ready.add(seg);
            else
                news.add(seg);
        }

        // for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
        RawTableSegment pre = null;
        for (RawTableSegment seg : ready) {
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
                if (pre.getSourceOffsetEnd() < seg.getSourceOffsetStart())
                    logger.warn("Hole between adjacent READY segments " + pre + " and " + seg);
            }
            pre = seg;
        }

        // for all other segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
        pre = null;
        for (RawTableSegment seg : news) {
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
            }
            pre = seg;

            for (RawTableSegment aReady : ready) {
                if (seg.sourceOffsetOverlaps(aReady) && !seg.sourceOffsetContains(aReady))
                    throw new IllegalStateException("Segments overlap: " + aReady + " and " + seg);
            }
        }

        // for all other segments, sourceOffset SHOULD fit/connect other segments
        for (RawTableSegment seg : news) {
            Pair<Boolean, Boolean> pair = fitInSegments(all, seg);
            boolean startFit = pair.getFirst();
            boolean endFit = pair.getSecond();

            if (!startFit)
                logger.warn("NEW segment start does not fit/connect with other segments: " + seg);
            if (!endFit)
                logger.warn("NEW segment end does not fit/connect with other segments: " + seg);
        }
    }

    public static Pair<Boolean, Boolean> fitInSegments(List<RawTableSegment> segments, RawTableSegment newOne) {
        if (segments == null || segments.isEmpty())
            return null;

        RawTableSegment first = segments.get(0);
        RawTableSegment last = segments.get(segments.size() - 1);
        long start = newOne.getSourceOffsetStart();
        long end = newOne.getSourceOffsetEnd();
        boolean startFit = false;
        boolean endFit = false;
        for (RawTableSegment sss : segments) {
            if (sss == newOne)
                continue;
            startFit = startFit || (start == sss.getSourceOffsetStart() || start == sss.getSourceOffsetEnd());
            endFit = endFit || (end == sss.getSourceOffsetStart() || end == sss.getSourceOffsetEnd());
        }
        if (!startFit && endFit && newOne == first)
            startFit = true;
        if (!endFit && startFit && newOne == last)
            endFit = true;

        return Pair.newPair(startFit, endFit);
    }

}
