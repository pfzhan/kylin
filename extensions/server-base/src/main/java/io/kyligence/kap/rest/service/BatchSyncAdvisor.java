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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TimePartitionedSegmentRange;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.rest.request.KapSyncRequest;

public class BatchSyncAdvisor {

    public static List<KapJobBuildRequest> buildJobBuildRequests(String cubeName, List<KapSyncRequest> reqList)
            throws IOException {

        Map<String, Set<TimePartitionedSegmentRange>> combinedRangeMap = combineBuildRanges(reqList);

        List<KapJobBuildRequest> kapJobList = buildKapJobRequests(cubeName, combinedRangeMap);

        return kapJobList;
    }

    private static List<KapJobBuildRequest> buildKapJobRequests(String cubeName,
            Map<String, Set<TimePartitionedSegmentRange>> combinedRangeMap) throws IOException {
        List<KapJobBuildRequest> kapJobList = Lists.newArrayList();

        for (Map.Entry<String, Set<TimePartitionedSegmentRange>> entry : combinedRangeMap.entrySet()) {
            CubeInstance cube = MPCubeManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .convertToMPCubeIfNeeded(cubeName, new String[] { entry.getKey() });
            Segments<CubeSegment> segments = cube.getSegments();
            List<TimePartitionedSegmentRange> existingRanges = Lists.newArrayList();
            for (CubeSegment seg : segments) {
                SegmentRange segRange = seg.getSegRange();
                Preconditions.checkState(segRange instanceof TimePartitionedSegmentRange);
                existingRanges.add((TimePartitionedSegmentRange) segRange);
            }

            List<TimePartitionedSegmentRange> inputRanges = Lists.newArrayList(entry.getValue());

            List<TimePartitionedSegmentRange> toSyncRanges = matchWithExistingRanges(inputRanges, existingRanges);

            for (TimePartitionedSegmentRange sRange : toSyncRanges) {
                boolean isExisting = existingRanges.contains(sRange);
                CubeBuildTypeEnum buildType = isExisting ? CubeBuildTypeEnum.REFRESH : CubeBuildTypeEnum.BUILD;
                kapJobList.add(new KapJobBuildRequest(cube.getName(), sRange, buildType.toString()));
            }
        }

        return kapJobList;
    }

    private static Map<String, Set<TimePartitionedSegmentRange>> combineBuildRanges(List<KapSyncRequest> reqList) {
        Map<String, Set<TimePartitionedSegmentRange>> csMap = Maps.newHashMap();

        for (KapSyncRequest kapSyncReq : reqList) {
            Set<TimePartitionedSegmentRange> mergeSet = mergePointAndRange(kapSyncReq.getPointList(),
                    kapSyncReq.getRangeList());
            Set<TimePartitionedSegmentRange> csSet = csMap.get(kapSyncReq.getMpValues());
            csSet = resetRanges(mergeSet, csSet);
            csMap.put(kapSyncReq.getMpValues(), csSet);
        }

        return csMap;
    }

    private static Set<TimePartitionedSegmentRange> mergePointAndRange(List<Long> pointList, List<Long[]> rangeList) {
        Set<TimePartitionedSegmentRange> pointSegSet = pointToSegRange(pointList);
        Set<TimePartitionedSegmentRange> rangeSegSet = rangeToSegRange(rangeList);
        pointSegSet.addAll(rangeSegSet);
        return pointSegSet;
    }

    private static Set<TimePartitionedSegmentRange> pointToSegRange(List<Long> pointList) {
        Set<TimePartitionedSegmentRange> rangeSet = Sets.newHashSet();
        for (Long point : pointList) {
            TimePartitionedSegmentRange range = new TimePartitionedSegmentRange(point, point + 1);
            rangeSet.add(range);
        }
        return rangeSet;
    }

    private static Set<TimePartitionedSegmentRange> rangeToSegRange(List<Long[]> rangeList) {
        Set<TimePartitionedSegmentRange> rangeSet = Sets.newHashSet();
        for (Long[] rg : rangeList) {
            TimePartitionedSegmentRange range = new TimePartitionedSegmentRange(rg[0], rg[1] + 1);
            rangeSet.add(range);
        }
        return rangeSet;
    }

    private static Set<TimePartitionedSegmentRange> resetRanges(Set<TimePartitionedSegmentRange> mergeSet,
            Set<TimePartitionedSegmentRange> rgSet) {
        if (rgSet == null) {
            rgSet = Sets.newHashSet();
        }
        for (TimePartitionedSegmentRange range : mergeSet) {
            resetRange(range, rgSet);
        }

        return rgSet;
    }

    private static Set<TimePartitionedSegmentRange> resetRange(TimePartitionedSegmentRange range,
            Set<TimePartitionedSegmentRange> rgSet) {

        Set<TimePartitionedSegmentRange> mergeRangeSet = Sets.newHashSet();

        List<TimePartitionedSegmentRange> rangeList = Lists.newArrayList(rgSet);
        Collections.sort(rangeList);

        int point = 0;
        TimePartitionedSegmentRange pointer = null;
        while (point < rgSet.size()) {
            pointer = rangeList.get(point);
            if (range.overlaps(pointer)) {
                if (range.contains(pointer)) {
                    point++;
                } else if (range.getStart().compareTo(pointer.getStart()) < 0) {
                    range = new TimePartitionedSegmentRange(range.getStart(), pointer.getEnd());
                } else if (range.getStart().compareTo(pointer.getStart()) > 0) {
                    range = new TimePartitionedSegmentRange(pointer.getStart(), range.getEnd());
                    point++;
                }
            } else {
                mergeRangeSet.add(pointer);
                point++;
            }
        }

        mergeRangeSet.add(range);

        rgSet.clear();
        rgSet.addAll(mergeRangeSet);

        return rgSet;
    }

    private static List<TimePartitionedSegmentRange> matchWithExistingRanges(
            List<TimePartitionedSegmentRange> inputRanges, List<TimePartitionedSegmentRange> existingRanges) {
        List<TimePartitionedSegmentRange> toSyncRanges = Lists.newArrayList();

        if (existingRanges.isEmpty()) {
            toSyncRanges = inputRanges;
            return toSyncRanges;
        }

        inputRanges = Lists.newArrayList(inputRanges); // copy before changing input list
        Collections.sort(inputRanges);
        // existing ranges are already sorted // Collections.sort(existsRanges);

        int inPointer = 0;
        int existingPointer = 0;
        TimePartitionedSegmentRange inRange = null;
        TimePartitionedSegmentRange existingRange = null;

        // like merge sort, match two sorted list
        while (inPointer < inputRanges.size()) {

            if (existingPointer >= existingRanges.size()) {
                toSyncRanges.add(inputRanges.get(inPointer));
                inPointer++;
                continue;
            }

            inRange = inputRanges.get(inPointer);
            existingRange = existingRanges.get(existingPointer);

            if (inRange.overlaps(existingRange)) {
                if (!toSyncRanges.contains(existingRange)) {
                    toSyncRanges.add(existingRange);
                }

                if (inRange.getStart().compareTo(existingRange.getStart()) < 0) {
                    toSyncRanges.add(new TimePartitionedSegmentRange(inRange.getStart(), existingRange.getStart()));
                }

                int endComp = inRange.getEnd().compareTo(existingRange.getEnd());
                if (endComp < 0) {
                    inPointer++;
                } else if (endComp == 0) {
                    inPointer++;
                    existingPointer++;
                } else {
                    inputRanges.set(inPointer,
                            new TimePartitionedSegmentRange(existingRange.getEnd(), inRange.getEnd()));
                    existingPointer++;
                }
            } else {
                if (inRange.getStart().compareTo(existingRange.getStart()) < 0) {
                    toSyncRanges.add(inRange);
                    inPointer++;
                } else {
                    existingPointer++;
                }
            }
        }

        return toSyncRanges;
    }

    public static class KapJobBuildRequest {

        private String cubeName;

        private TimePartitionedSegmentRange tsRange;

        private String buildType;

        public KapJobBuildRequest(String cubeName, TimePartitionedSegmentRange tsRange, String buildType) {
            this.cubeName = cubeName;
            this.tsRange = tsRange;
            this.buildType = buildType;
        }

        public TimePartitionedSegmentRange getTsRange() {
            return tsRange;
        }

        public void setTsRange(TimePartitionedSegmentRange tsRange) {
            this.tsRange = tsRange;
        }

        public String getCubeName() {
            return cubeName;
        }

        public void setCubeName(String cubeName) {
            this.cubeName = cubeName;
        }

        public String getBuildType() {
            return buildType;
        }

        public void setBuildType(String buildType) {
            this.buildType = buildType;
        }
    }
}
