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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.builder.SnapshotBuilder;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;

public class SegmentBuildJob extends SegmentJob {
    private static final Logger logger = LoggerFactory.getLogger(SegmentBuildJob.class);

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    @Override
    protected final void doExecute() throws Exception {
        tryRefreshSnapshots();
        if (isMLP()) {
            buildMLP();
        } else {
            build();
        }
        updateSegmentSourceBytesSize();
    }

    @Override // Copied from DFBuildJob
    protected final String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            String maxLeafTasksNums = maxLeafTasksNums();
            int factor = config.getSparkEngineTaskCoreFactor();
            int requiredCore = (int) Double.parseDouble(maxLeafTasksNums) / factor;
            logger.info("The maximum number of tasks required to run the job is {}, require cores: {}",
                    maxLeafTasksNums, requiredCore);
            return String.valueOf(requiredCore);
        } else {
            return SparkJobConstants.DEFAULT_REQUIRED_CORES;
        }
    }

    // Copied from DFBuildJob
    private String maxLeafTasksNums() throws IOException {
        if (Objects.isNull(rdSharedPath)) {
            rdSharedPath = config.getJobTmpShareDir(project, jobId);
        }
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(rdSharedPath,
                path -> path.toString().endsWith(ResourceDetectUtils.maxLeaveTaskNumsSuffix()));
        return ResourceDetectUtils.selectMaxValueInFiles(fileStatuses);
    }

    private void buildMLP() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            MLPBuildExec exec = new MLPBuildExec(this, dataSegment);
            buildSegment(dataSegment, exec);
        }
    }

    private void build() throws IOException {
        for (NDataSegment dataSegment : readOnlySegments) {
            SegmentBuildExec exec = new SegmentBuildExec(this, dataSegment);
            buildSegment(dataSegment, exec);
        }
    }

    private void buildSegment(NDataSegment dataSegment, SegmentBuildExec exec) throws IOException {
        logger.info("Encoding data skew {} segment {}", //
                dataSegment.isEncodingDataSkew(), dataSegment.getId());
        KylinBuildEnv.get().setEncodingDataSkew(dataSegment.isEncodingDataSkew());
        exec.buildSegment();
    }

    // Copied from DFBuildJob
    protected void tryRefreshSnapshots() throws IOException {
        SnapshotBuilder snapshotBuilder = new SnapshotBuilder();
        if (config.isSnapshotManualManagementEnabled()) {
            logger.info("Skip snapshot build in snapshot manual mode, dataflow: {}, only calculate total rows",
                    dataflowId);
            snapshotBuilder.calculateTotalRows(ss, getDataflow(dataflowId).getModel(), getIgnoredSnapshotTables());
            return;
        } else if (!needBuildSnapshots()) {
            logger.info("Skip snapshot build, dataflow {}, only calculate total rows", dataflowId);
            snapshotBuilder.calculateTotalRows(ss, getDataflow(dataflowId).getModel(), getIgnoredSnapshotTables());
            return;
        }
        logger.info("Refresh SNAPSHOT.");
        //snapshot building
        snapshotBuilder.buildSnapshot(ss, getDataflow(dataflowId).getModel(), //
                //
                getIgnoredSnapshotTables());
        logger.info("Finished SNAPSHOT.");
    }

    // Copied from DFBuildJob
    private void updateSegmentSourceBytesSize() {
        Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(rdSharedPath);
        NDataflow dataflow = getDataflow(dataflowId);
        NDataflow newDF = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getUuid());
        List<NDataSegment> nDataSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : segmentSourceSize.entrySet()) {
            NDataSegment segment = newDF.getSegment(entry.getKey());
            segment.setSourceBytesSize((Long) entry.getValue());
            if (KylinBuildEnv.get().encodingDataSkew()) {
                segment.setEncodingDataSkew(true);
            }
            nDataSegments.add(segment);
        }
        update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
        getDataflowManager().updateDataflow(update);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> copyForWrite //
                .setLayoutBucketNumMapping(indexPlanManager.getIndexPlan(dataflowId).getLayoutBucketNumMapping()));
    }

    public static void main(String[] args) {
        SegmentBuildJob segmentBuildJob = new SegmentBuildJob();
        segmentBuildJob.execute(args);
    }
}
