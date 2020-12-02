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
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
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

    protected NSpanningTree spanningTree;

    protected String dataflowId;

    protected NDataflowManager dataflowManager;

    protected Set<LayoutEntity> readOnlyLayouts;

    protected Set<NDataSegment> readOnlySegments;

    // Resource detection results output path
    protected Path rdSharedPath;

    @Override
    protected final void extraInit() {
        Set<String> segmentIDs = Arrays.stream(getParam(NBatchConstants.P_SEGMENT_IDS).split(COMMA))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<Long> layoutIDs = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        dataflowManager = NDataflowManager.getInstance(config, project);
        rdSharedPath = config.getJobTmpShareDir(project, jobId);
        indexPlan = dataflowManager.getDataflow(dataflowId).getIndexPlan();

        readOnlyLayouts = Collections.unmodifiableSet(NSparkCubingUtil.toLayouts(indexPlan, layoutIDs));

        spanningTree = NSpanningTreeFactory.fromLayouts(readOnlyLayouts, dataflowId);

        final Predicate<NDataSegment> notSkip = (NDataSegment dataSegment) -> !needSkipSegment(dataSegment);

        readOnlySegments = Collections.unmodifiableSet((Set<? extends NDataSegment>) segmentIDs.stream() //
                .map(this::getSegment) //
                .filter(notSkip) //
                .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    protected KylinConfig getConfig() {
        return config;
    }

    protected SparkSession getSparkSession() {
        return ss;
    }

    protected String getDataflowId() {
        return dataflowId;
    }

    protected NDataflowManager getDataflowManager() {
        return dataflowManager;
    }

    protected NSpanningTree getSpanningTree() {
        return spanningTree;
    }

    protected Path getRDSharedPath() {
        return rdSharedPath;
    }

    protected Set<JobBucket> getReadOnlyBuckets() {
        return Collections.unmodifiableSet(ExecutableParams.getBuckets(getParam(NBatchConstants.P_BUCKETS)));
    }

    protected String getJobId() {
        return jobId;
    }

    protected NDataflow getDataflow(String dataflowId) {
        return dataflowManager.getDataflow(dataflowId);
    }

    protected NDataSegment getSegment(String segmentId) {
        // Always get the latest data segment.
        return dataflowManager.getDataflow(dataflowId).getSegment(segmentId);
    }

    protected boolean needBuildSnapshots() {
        String s = getParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS);
        if (StringUtils.isBlank(s)) {
            return true;
        }
        return Boolean.parseBoolean(s);
    }

    protected boolean isMLP() {
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
}
