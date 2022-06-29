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
package org.apache.kylin.job.common;

import static org.apache.kylin.job.execution.JobTypeEnum.INDEX_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SUB_PARTITION_BUILD;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentUtil {

    public static Segments<NDataSegment> getSegmentsExcludeRefreshingAndMerging(Segments<NDataSegment> segments) {
        Segments<NDataSegment> result = new Segments<>();
        for (val seg : segments) {
            val status = getSegmentStatusToDisplay(segments, seg, null);
            if (!(Objects.equals(SegmentStatusEnumToDisplay.REFRESHING, status)
                    || Objects.equals(SegmentStatusEnumToDisplay.MERGING, status))) {
                result.add(seg);
            }
        }
        return result;
    }

    public static <T extends ISegment> SegmentStatusEnumToDisplay getSegmentStatusToDisplay(Segments segments,
            T segment, List<AbstractExecutable> executables) {
        Segments<T> overlapSegs = segments.getSegmentsByRange(segment.getSegRange());
        overlapSegs.remove(segment);
        if (SegmentStatusEnum.NEW == segment.getStatus()) {
            if (CollectionUtils.isEmpty(overlapSegs)) {
                return SegmentStatusEnumToDisplay.LOADING;
            }

            if (overlapSegs.get(0).getSegRange().entireOverlaps(segment.getSegRange())) {
                return SegmentStatusEnumToDisplay.REFRESHING;
            }

            return SegmentStatusEnumToDisplay.MERGING;
        }

        if (isAnyPartitionLoading(segment)) {
            return SegmentStatusEnumToDisplay.LOADING;
        }

        if (isAnyPartitionRefreshing(segment)) {
            return SegmentStatusEnumToDisplay.REFRESHING;
        }

        if (CollectionUtils.isNotEmpty(overlapSegs)) {
            Preconditions.checkState(CollectionUtils.isNotEmpty(overlapSegs.getSegments(SegmentStatusEnum.NEW)));
            return SegmentStatusEnumToDisplay.LOCKED;
        }

        if (anyIndexJobRunning(segment, executables)) {
            return SegmentStatusEnumToDisplay.LOADING;
        }

        if (SegmentStatusEnum.WARNING == segment.getStatus()) {
            return SegmentStatusEnumToDisplay.WARNING;
        }

        return SegmentStatusEnumToDisplay.ONLINE;
    }

    protected static <T extends ISegment> boolean anyIndexJobRunning(T segment) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExecutableManager execManager = ExecutableManager.getInstance(kylinConfig, segment.getModel().getProject());
        List<ExecutablePO> executablePOS = JobMetadataInvoker.getInstance().listExecPOByJobTypeAndStatus(
                segment.getModel().getProject(), "isRunning", INDEX_BUILD, SUB_PARTITION_BUILD);
        return executablePOS.stream().map(execManager::fromPO)
                .anyMatch(task -> task.getSegmentIds().contains(segment.getId()));
    }

    protected static <T extends ISegment> boolean anyIndexJobRunning(T segment, List<AbstractExecutable> executables) {
        if (Objects.isNull(executables)) {
            return anyIndexJobRunning(segment);
        } else {
            return executables.stream().anyMatch(task -> task.getSegmentIds().contains(segment.getId()));
        }
    }

    private static <T extends ISegment> boolean isAnyPartitionLoading(T segment) {
        Preconditions.checkArgument(segment instanceof NDataSegment);
        val partitions = ((NDataSegment) segment).getMultiPartitions();

        if (CollectionUtils.isEmpty(partitions))
            return false;
        val loadingPartition = partitions.stream() //
                .filter(partition -> PartitionStatusEnum.NEW == partition.getStatus()) //
                .findAny().orElse(null);
        return loadingPartition != null;
    }

    private static <T extends ISegment> boolean isAnyPartitionRefreshing(T segment) {
        Preconditions.checkArgument(segment instanceof NDataSegment);
        val partitions = ((NDataSegment) segment).getMultiPartitions();

        if (CollectionUtils.isEmpty(partitions))
            return false;
        val refreshPartition = partitions.stream()
                .filter(partition -> PartitionStatusEnum.REFRESH == partition.getStatus()).findAny().orElse(null);
        return refreshPartition != null;
    }

    /**
     * Valid segment：
     * 1. SegmentStatusEnum is READY or WARNING.
     * 2. Time doesn't overlap with running segments.
     */
    public static Segments<NDataSegment> getValidSegments(String modelId, String project) {
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        val executables = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        val runningSegs = new Segments<NDataSegment>();
        executables.stream().filter(e -> e.getTargetSegments() != null) //
                .flatMap(e -> e.getTargetSegments().stream()) //
                .distinct() //
                .filter(segId -> df.getSegment(segId) != null) //
                .forEach(segId -> runningSegs.add(df.getSegment(segId)));
        return getSegmentsByTime(df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING), runningSegs);
    }

    public static Segments<NDataSegment> getSegmentsByTime(Segments<NDataSegment> targetSegments,
            Segments<NDataSegment> checkSegments) {
        val filterSegs = new Segments<NDataSegment>();
        for (NDataSegment targetSeg : targetSegments) {
            boolean isOverLap = false;
            for (NDataSegment relatedSeg : checkSegments) {
                if (targetSeg.getSegRange().overlaps(relatedSeg.getSegRange())) {
                    isOverLap = true;
                    break;
                }
            }
            if (!isOverLap) {
                filterSegs.add(targetSeg);
            }
        }
        return filterSegs;
    }

    public static Set<Long> intersectionLayouts(Segments<NDataSegment> segments) {
        HashSet<Long> layoutIds = Sets.newHashSet();
        if (segments.isEmpty()) {
            return layoutIds;
        }
        layoutIds = new HashSet<>(segments.get(0).getLayoutsMap().keySet());
        for (NDataSegment segment : segments) {
            if (segment.getLayoutsMap().size() == 0) {
                layoutIds.clear();
                break;
            }
            layoutIds.retainAll(segment.getLayoutsMap().keySet());
        }
        return layoutIds;
    }
}
