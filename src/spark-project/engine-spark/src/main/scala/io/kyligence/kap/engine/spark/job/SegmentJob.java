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

package io.kyligence.kap.engine.spark.job;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.spark.tracker.BuildContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.scheduler.JobRuntime;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.job.JobBucket;

public abstract class SegmentJob extends SparkApplication {

    private static final Logger logger = LoggerFactory.getLogger(SegmentJob.class);

    private static final String COMMA = ",";

    protected IndexPlan indexPlan;

    protected String dataflowId;

    protected Set<LayoutEntity> readOnlyLayouts;

    protected Set<NDataSegment> readOnlySegments;

    // Resource detection results output path
    protected Path rdSharedPath;

    public JobRuntime runtime;

    private boolean partialBuild = false;

    protected BuildContext buildContext;

    public boolean isPartialBuild() {
        return partialBuild;
    }

    public Set<LayoutEntity> getReadOnlyLayouts() {
        return readOnlyLayouts;
    }

    @Override
    protected final void extraInit() {
        partialBuild = Boolean.parseBoolean(getParam(NBatchConstants.P_PARTIAL_BUILD));
        Set<String> segmentIDs = Arrays.stream(getParam(NBatchConstants.P_SEGMENT_IDS).split(COMMA))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<Long> layoutIDs = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        rdSharedPath = config.getJobTmpShareDir(project, jobId);
        indexPlan = dataflowManager.getDataflow(dataflowId).getIndexPlan();

        readOnlyLayouts = Collections.unmodifiableSet(NSparkCubingUtil.toLayouts(indexPlan, layoutIDs));

        final Predicate<NDataSegment> notSkip = (NDataSegment dataSegment) -> !needSkipSegment(dataSegment);

        String excludeTableStr = getParam(NBatchConstants.P_EXCLUDED_TABLES);
        ImmutableSet<String> tables = StringUtils.isBlank(excludeTableStr) //
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(excludeTableStr.split(SegmentJob.COMMA));
        readOnlySegments = Collections.unmodifiableSet((Set<? extends NDataSegment>) segmentIDs.stream() //
                .map(segmentId -> {
                    NDataSegment dataSegment = getSegment(segmentId);
                    dataSegment.setExcludedTables(tables);
                    return dataSegment;
                }).filter(notSkip) //
                .collect(Collectors.toCollection(LinkedHashSet::new)));
        runtime = new JobRuntime(config.getSegmentExecMaxThreads());
    }

    @Override
    protected void extraDestroy() {
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    public String getDataflowId() {
        return dataflowId;
    }

    protected Path getRdSharedPath() {
        return rdSharedPath;
    }

    public Set<JobBucket> getReadOnlyBuckets() {
        return Collections.unmodifiableSet(ExecutableParams.getBuckets(getParam(NBatchConstants.P_BUCKETS)));
    }

    public NDataflow getDataflow(String dataflowId) {
        return getDataflowManager().getDataflow(dataflowId);
    }

    public NDataSegment getSegment(String segmentId) {
        // Always get the latest data segment.
        return getDataflowManager().getDataflow(dataflowId).getSegment(segmentId);
    }

    public final List<NDataSegment> getUnmergedSegments(NDataSegment merged) {
        List<NDataSegment> unmerged = getDataflowManager().getDataflow(dataflowId).getMergingSegments(merged);
        Preconditions.checkNotNull(unmerged);
        Preconditions.checkState(!unmerged.isEmpty());
        Collections.sort(unmerged);
        return unmerged;
    }

    public boolean needBuildSnapshots() {
        String s = getParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS);
        if (StringUtils.isBlank(s)) {
            return true;
        }
        return Boolean.parseBoolean(s);
    }

    protected boolean isPartitioned() {
        return Objects.nonNull(indexPlan.getModel().getMultiPartitionDesc());
    }

    private boolean needSkipSegment(NDataSegment dataSegment) {
        if (Objects.isNull(dataSegment)) {
            logger.info("Skip segment: NULL.");
            return true;
        }
        if (Objects.isNull(dataSegment.getSegRange()) || Objects.isNull(dataSegment.getModel())
                || Objects.isNull(dataSegment.getIndexPlan())) {
            logger.info("Skip segment: {}, range: {}, model: {}, index plan: {}", dataSegment.getId(),
                    dataSegment.getSegRange(), dataSegment.getModel(), dataSegment.getIndexPlan());
            return true;
        }
        return false;
    }

    private NDataflowManager getDataflowManager() {
        return NDataflowManager.getInstance(config, project);
    }

    public BuildContext getBuildContext() {
        return buildContext;
    }
}
