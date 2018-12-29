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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;

import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.VolatileRange;

public class Segments<T extends ISegment> extends ArrayList<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(Segments.class);

    public static ISegmentAdvisor newSegmentAdvisor(ISegment seg) {
        try {
            Class<? extends ISegmentAdvisor> clz = ClassUtil.forName(seg.getConfig().getSegmentAdvisor(),
                    ISegmentAdvisor.class);
            return clz.getConstructor(ISegment.class).newInstance(seg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    public Segments() {
        super();
    }

    public Segments(List<T> copy) {
        super(copy);
    }

    public T getFirstSegment() {
        if (this == null || this.size() == 0) {
            return null;
        } else {
            return this.get(0);
        }
    }

    public long getTSStart() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (ISegment seg : readySegs) {
            startTime = Math.min(startTime, seg.getTSRange().start);
        }

        return startTime;
    }

    public long getTSEnd() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (ISegment seg : readySegs) {
            endTime = Math.max(endTime, seg.getTSRange().end);
        }

        return endTime;
    }

    public T getLatestReadySegment() {
        T latest = null;
        for (int i = this.size() - 1; i >= 0; i--) {
            T seg = this.get(i);
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;
            if (latest == null || latest.getTSRange().end < seg.getTSRange().end) {
                latest = seg;
            }
        }
        return latest;
    }

    public T getLatestBuiltSegment() {
        T latest = null;
        for (int i = this.size() - 1; i >= 0; i--) {
            T seg = this.get(i);
            if (seg.getLastBuildTime() > 0) {
                if (latest == null || seg.getLastBuildTime() > latest.getLastBuildTime())
                    latest = seg;
            }
        }
        return latest;
    }

    public Segments<T> getSegments(SegmentStatusEnum status) {
        Segments<T> result = new Segments<>();

        for (T segment : this) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }
        return result;
    }

    public T getSegment(String name, SegmentStatusEnum status) {
        for (T segment : this) {
            if ((null != segment.getName() && segment.getName().equals(name))
                    && (status == null || segment.getStatus() == status)) {
                return segment;
            }
        }
        return null;
    }

    public Segments<T> getBuildingSegments() {
        Segments<T> buildingSegments = new Segments();
        if (null != this) {
            for (T segment : this) {
                if (SegmentStatusEnum.NEW == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }
        return buildingSegments;
    }

    public Segments<T> getSegmentsExcludeRefreshingAndMerging() {
        Segments<T> readySegments = this.getSegments(SegmentStatusEnum.READY);
        Segments<T> buildingSegments = getBuildingSegments();
        if (CollectionUtils.isEmpty(readySegments)) {
            return this;
        }
        SegmentRange readySegmentsRange = readySegments.getFirstSegment().getSegRange()
                .coverWith(readySegments.getLatestReadySegment().getSegRange());
        for (T buildingSegment : buildingSegments) {
            if (readySegmentsRange.contains(buildingSegment.getSegRange())) {
                this.remove(buildingSegment);
            }
        }
        return this;
    }

    public Segments<T> getFlatSegments() {
        Segments<T> result = new Segments<>(this);
        val buildingSegs = result.getBuildingSegments();
        val readySegs = result.getSegments(SegmentStatusEnum.READY);
        for (T segment : readySegs) {
            for (val buildingSeg : buildingSegs) {
                if (segment.getSegRange().overlaps(buildingSeg.getSegRange())) {
                    result.remove(segment);
                    break;
                }
            }
        }
        return result;
    }

    public Segments<T> getMergingSegments(T mergedSegment) {
        Segments<T> result = new Segments();
        if (mergedSegment == null)
            return result;

        for (T seg : this) {
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;

            if (seg == mergedSegment)
                continue;

            if (mergedSegment.getSegRange().contains(seg.getSegRange())) {
                result.add(seg);
            }
        }
        return result;
    }

    public SegmentRange autoMergeSegments(boolean isAutoMergeEnabled, String modelName,
            List<AutoMergeTimeEnum> autoMergeTimeEnums, VolatileRange volatileRange) {
        if (!isAutoMergeEnabled) {
            return null;
        }
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);
        if (volatileRange.isVolatileRangeEnabled()) {
            removeSegmentsByVolatileRange(readySegs, volatileRange);
        }
        //building segments overlaps， can not merge
        Segments segsOverlapsWithBuilding = new Segments();
        for (ISegment buildingSeg : getBuildingSegments()) {
            // exclude those under-building segs
            for (ISegment readySeg : readySegs) {
                if (buildingSeg.getSegRange().overlaps(readySeg.getSegRange())) {
                    segsOverlapsWithBuilding.add(readySeg);
                }
            }
        }
        // exclude those already under merging segments
        readySegs.removeAll(segsOverlapsWithBuilding);
        if (readySegs.size() < 2) {
            return null;
        }
        List<AutoMergeTimeEnum> sortedAutoMergeTimeEnums = sortTimeRanges(autoMergeTimeEnums);
        for (int i = 0; i < sortedAutoMergeTimeEnums.size(); i++) {
            AutoMergeTimeEnum autoMergeTimeEnum = sortedAutoMergeTimeEnums.get(i);
            SegmentRange segmentRangeToMerge = readySegs.findMergeSegmentsRange(autoMergeTimeEnum);
            if (segmentRangeToMerge != null) {
                return segmentRangeToMerge;
            }
        }
        return null;
    }

    public long getMergeEnd(long start, AutoMergeTimeEnum autoMergeTimeEnum) {
        Calendar calendar = Calendar.getInstance();
        TimeZone zone = TimeZone.getTimeZone("GMT");
        calendar.setTimeZone(zone);
        calendar.setTimeInMillis(start);
        int month = calendar.get(Calendar.MONTH);
        String weekFirstDay = KylinConfig.getInstanceFromEnv().getFirstDayOfWeek();
        switch (autoMergeTimeEnum) {
        case HOUR:
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.add(Calendar.HOUR_OF_DAY, 1);
            break;
        case DAY:
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            break;
        case WEEK:

            if (weekFirstDay.equalsIgnoreCase("monday")) {
                if (calendar.get(Calendar.DAY_OF_WEEK) != 1) {
                    calendar.add(Calendar.WEEK_OF_MONTH, 1);
                }
                calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            } else {
                calendar.add(Calendar.WEEK_OF_MONTH, 1);
                calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

            }
            if (calendar.get(Calendar.MONTH) > month) {
                calendar.set(Calendar.DAY_OF_MONTH, 1);
            }
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            break;
        case MONTH:
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.add(Calendar.MONTH, 1);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            break;
        case YEAR:
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.MONTH, 0);
            calendar.add(Calendar.YEAR, 1);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            break;
        default:
            break;
        }
        return calendar.getTimeInMillis();
    }

    private List<AutoMergeTimeEnum> sortTimeRanges(List<AutoMergeTimeEnum> autoMergeTimeEnums) {
        List<AutoMergeTimeEnum> sortedList = Lists
                .newArrayList(FluentIterable.from(autoMergeTimeEnums).toSortedList(new Comparator<AutoMergeTimeEnum>() {
                    @Override
                    public int compare(AutoMergeTimeEnum o1, AutoMergeTimeEnum o2) {
                        return o1.getCode() < o2.getCode() ? 1 : -1;
                    }
                }));
        return sortedList;
    }

    private long getMillisecondByType(AutoMergeTimeEnum autoMergeTimeEnum) {
        long time = 0;
        switch (autoMergeTimeEnum) {
        case HOUR:
            time = 3600000L;
            break;
        case DAY:
            time = 86400000L;
            break;
        case WEEK:
            time = 604800000L;
            break;
        default:
            break;

        }
        return time;
    }

    public void removeSegmentsByVolatileRange(Segments<T> segs, VolatileRange volatileRange) {
        if (volatileRange.getVolatileRangeNumber() <= 0 || volatileRange.getVolatileRangeType() == null) {
            return;
        }
        Long latestSegEnd = Long.parseLong(segs.getLast().getSegRange().getEnd().toString());

        Segments volatileSegs = new Segments();
        long volatileTime = getMillisecondByType(volatileRange.getVolatileRangeType());
        if (volatileTime > 0) {
            for (T seg : segs) {
                if (Long.parseLong(seg.getSegRange().getEnd().toString())
                        + volatileTime * volatileRange.getVolatileRangeNumber() > latestSegEnd) {
                    volatileSegs.add(seg);
                }
            }
        }
        segs.removeAll(volatileSegs);
    }

    public void replace(Comparator<T> comparator, T seg) {
        for (int i = 0; i < size(); i++) {
            if (comparator.compare(get(i), seg) == 0) {
                set(i, seg);
                break;
            }
        }
    }

    public SegmentRange findMergeSegmentsRange(AutoMergeTimeEnum autoMergeTimeEnum) {
        long mergeStart = Long.parseLong(this.getFirst().getSegRange().start.toString());
        SegmentRange rangeToMerge = new SegmentRange.TimePartitionedSegmentRange(mergeStart,
                getMergeEnd(mergeStart, autoMergeTimeEnum));

        Segments segmentsToMerge = new Segments();
        for (ISegment seg : this) {
            if (this.getLast().getSegRange().getEnd().compareTo(rangeToMerge.getEnd()) < 0) {
                return null;
            }
            long mergeEnd = Long.parseLong(rangeToMerge.getEnd().toString());
            SegmentRange segmentRange = seg.getSegRange();
            // include if segment range contained
            if (rangeToMerge.getStart().compareTo(segmentRange.getStart()) <= 0
                    && segmentRange.getEnd().compareTo(rangeToMerge.getEnd()) <= 0) {
                // segment has gap, compute next section
                if (segmentsToMerge.size() > 0 && !segmentsToMerge.getLast().getSegRange().connects(segmentRange)) {
                    rangeToMerge = new SegmentRange.TimePartitionedSegmentRange(mergeEnd,
                            getMergeEnd(mergeEnd, autoMergeTimeEnum));
                    segmentsToMerge.clear();
                    continue;
                } else {
                    segmentsToMerge.add(seg);
                }
            }

            if (seg.getSegRange().getEnd().compareTo(rangeToMerge.getEnd()) >= 0) {
                long end = Long.parseLong(seg.getSegRange().getEnd().toString());
                if (segmentsToMerge.size() > 1 && (segmentsToMerge.getLast().equals(seg)
                        || segmentsToMerge.getLast().getSegRange().connects(segmentRange))) {
                    break;
                } else {
                    //this section can not merge,but has next section data,compute next section
                    rangeToMerge = new SegmentRange.TimePartitionedSegmentRange(end,
                            getMergeEnd(end, autoMergeTimeEnum));
                    segmentsToMerge.clear();

                    continue;
                }

            }
        }
        if (segmentsToMerge.size() < 2) {
            return null;
        }
        return segmentsToMerge.getFirst().getSegRange().coverWith(segmentsToMerge.getLast().getSegRange());
    }

    /**
     * Smartly figure out the TOBE segments once all new segments are built.
     * - Ensures no gap, no overlap
     * - Favors new segments over the old
     * - Favors big segments over the small
     */
    public Segments<T> calculateToBeSegments(T newSegment) {

        Segments<T> tobe = (Segments<T>) this.clone();
        if (newSegment != null && !tobe.contains(newSegment)) {
            tobe.add(newSegment);
        }
        if (tobe.size() == 0)
            return tobe;

        // sort by source offset
        Collections.sort(tobe);

        ISegment firstSeg = tobe.getFirst();
        firstSeg.validate();

        for (int i = 0, j = 1; j < tobe.size();) {
            ISegment is = (ISegment) tobe.get(i);
            ISegment js = (ISegment) tobe.get(j);
            js.validate();

            // check i is either ready or new
            if (!isNew(is) && !isReady(is)) {
                tobe.remove(i);
                continue;
            }

            // check j is either ready or new
            if (!isNew(js) && !isReady(js)) {
                tobe.remove(j);
                continue;
            }

            if (is.getSegRange().start.compareTo(js.getSegRange().start) == 0) {
                // if i, j competes
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {
                    // if both new or ready, favor the bigger segment
                    if (is.getSegRange().end.compareTo(js.getSegRange().end) <= 0) {
                        tobe.remove(i);
                    } else {
                        tobe.remove(j);
                    }
                    continue;
                } else {
                    // otherwise, favor the new segment
                    if (isNew(is) && is.equals(newSegment)) {
                        tobe.remove(j);
                        continue;
                    } else if (js.equals(newSegment)) {
                        tobe.remove(i);
                        continue;
                    }
                }
            }

            // if i, j in sequence
            if (is.getSegRange().end.compareTo(js.getSegRange().start) <= 0) {
                i++;
                j++;
                continue;
            }

            // js can be covered by is
            if (is.equals(newSegment)) {
                // seems j not fitting
                tobe.remove(j);
                continue;
            } else {
                i++;
                j++;
                continue;
            }

        }

        return tobe;
    }

    private boolean isReady(ISegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    private boolean isNew(ISegment seg) {
        return seg.getStatus() == SegmentStatusEnum.NEW;
    }

    private T getLast() {
        assert this.size() != 0;
        return this.get(this.size() - 1);
    }

    private T getFirst() {
        assert this.size() != 0;
        return this.get(0);
    }

    private Segments<T> getSubList(int from, int to) {
        Segments<T> result = new Segments<>();
        for (T seg : this.subList(from, to)) {
            result.add(seg);
        }
        return result;
    }

    /**
     * Validates:
     * - consistent isOffsetCube()
     * - for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
     * - for all new segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
     * - for all new segments, sourceOffset SHOULD fit/connect another segments
     * - dateRange does not matter any more
     */
    public void validate() {
        if (this.isEmpty())
            return;

        // make a copy, don't modify existing list
        Segments<T> all = new Segments<>(this);
        Collections.sort(all);

        // check consistent isOffsetCube()
        boolean isOffsetCube = all.get(0).isOffsetCube();
        for (ISegment seg : all) {
            seg.validate();
            if (seg.isOffsetCube() != isOffsetCube)
                throw new IllegalStateException("Inconsistent isOffsetsOn for segment " + seg);
        }

        Segments<T> ready = all.getSegments(SegmentStatusEnum.READY);
        Segments<T> news = all.getSegments(SegmentStatusEnum.NEW);
        validateReadySegs(ready);
        validateNewSegs(ready, news);
        validateOthers(all, news);
    }

    private void validateReadySegs(Segments<T> ready) {
        // for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
        ISegment pre = null;
        for (ISegment seg : ready) {
            if (pre != null) {
                if (pre.getSegRange().overlaps(seg.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
                if (pre.getSegRange().apartBefore(seg.getSegRange()))
                    logger.info("Hole between adjacent READY segments " + pre + " and " + seg);
            }
            pre = seg;
        }
    }

    private void validateNewSegs(Segments<T> ready, Segments<T> news) {
        // for all other segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
        ISegment pre = null;
        for (ISegment seg : news) {
            if (pre != null) {
                if (pre.getSegRange().overlaps(seg.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
            }
            pre = seg;

            for (ISegment aReady : ready) {
                if (seg.getSegRange().overlaps(aReady.getSegRange())
                        && !seg.getSegRange().contains(aReady.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + aReady + " and " + seg);
            }
        }
    }

    private void validateOthers(Segments<T> all, Segments<T> news) {
        // for all other segments, sourceOffset SHOULD fit/connect other segments
        for (ISegment seg : news) {
            Pair<Boolean, Boolean> pair = all.fitInSegments(seg);
            boolean startFit = pair.getFirst();
            boolean endFit = pair.getSecond();

            if (!startFit)
                logger.info("NEW segment start does not fit/connect with other segments: {}", seg);
            if (!endFit)
                logger.info("NEW segment end does not fit/connect with other segments: {}", seg);
        }
    }

    private Pair<Boolean, Boolean> fitInSegments(ISegment newOne) {
        Preconditions.checkState(!this.isEmpty());

        ISegment first = this.get(0);
        ISegment last = this.get(this.size() - 1);
        boolean startFit = false;
        boolean endFit = false;
        for (ISegment sss : this) {
            if (sss == newOne)
                continue;
            startFit = startFit || (newOne.getSegRange().startStartMatch(sss.getSegRange())
                    || newOne.getSegRange().startEndMatch(sss.getSegRange()));
            endFit = endFit || (newOne.getSegRange().endEndMatch(sss.getSegRange())
                    || sss.getSegRange().startEndMatch((newOne.getSegRange())));
        }
        if (!startFit && endFit && newOne == first)
            startFit = true;
        if (!endFit && startFit && newOne == last)
            endFit = true;

        return Pair.newPair(startFit, endFit);
    }

    // given all segments in cube, checks whether specified segment is operative (not under processing)
    public boolean isOperative(ISegment seg) {
        if (seg.getStatus() != SegmentStatusEnum.READY)
            return false;

        for (ISegment other : this) {
            if (other == seg)
                continue;

            if (other.getSegRange().overlaps(seg.getSegRange()))
                return false;
        }
        return true;
    }

    public static String makeSegmentName(SegmentRange segRange) {
        if (segRange == null || segRange.isInfinite()) {
            return "FULL_BUILD";
        }

        if (segRange instanceof SegmentRange.TimePartitionedSegmentRange) {
            // using time
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            return dateFormat.format(segRange.getStart()) + "_" + dateFormat.format(segRange.getEnd());
        } else {
            return segRange.getStart() + "_" + segRange.getEnd();
        }

    }

    public Segments getSegmentsByRange(SegmentRange range) {
        val result = new Segments<T>();
        for (val seg : this) {
            if (seg.getSegRange().overlaps(range)) {
                result.add(seg);
            }
        }
        return result;
    }

    public SegmentStatusEnumToDisplay getSegmentStatusToDisplay(T segment) {
        if (segment.getStatus().equals(SegmentStatusEnum.READY)) {
            return SegmentStatusEnumToDisplay.ONLINE;
        }
        Segments<T> overlapSegs = getSegmentsByRange(segment.getSegRange());
        overlapSegs.remove(segment);
        if (CollectionUtils.isEmpty(overlapSegs)) {
            return SegmentStatusEnumToDisplay.LOCKED;
        } else {
            if (overlapSegs.get(0).getSegRange().entireOverlaps(segment.getSegRange())) {
                return SegmentStatusEnumToDisplay.REFRESHING;
            } else {
                return SegmentStatusEnumToDisplay.MERGING;
            }
        }
    }
}
