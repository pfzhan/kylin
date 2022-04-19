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

import static io.kyligence.kap.engine.spark.job.StageType.BUILD_DICT;
import static io.kyligence.kap.engine.spark.job.StageType.BUILD_LAYER;
import static io.kyligence.kap.engine.spark.job.StageType.GATHER_FLAT_TABLE_STATS;
import static io.kyligence.kap.engine.spark.job.StageType.GENERATE_FLAT_TABLE;
import static io.kyligence.kap.engine.spark.job.StageType.MATERIALIZED_FACT_TABLE;
import static io.kyligence.kap.engine.spark.job.StageType.REFRESH_COLUMN_BYTES;
import static io.kyligence.kap.engine.spark.job.StageType.REFRESH_SNAPSHOTS;
import static io.kyligence.kap.engine.spark.job.StageType.WAITE_FOR_RESOURCE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.tracker.BuildContext;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.builder.SnapshotBuilder;
import io.kyligence.kap.engine.spark.job.exec.BuildExec;
import io.kyligence.kap.engine.spark.job.stage.BuildParam;
import io.kyligence.kap.engine.spark.job.stage.StageExec;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentBuildJob extends SegmentJob {

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    @Override
    protected void waiteForResourceSuccess() throws Exception {
        if (config.isBuildCheckPartitionColEnabled()) {
            checkDateFormatIfExist(project, dataflowId);
        }
        val waiteForResource = WAITE_FOR_RESOURCE.create(this, null, null);
        waiteForResource.onStageFinished(true);
        infos.recordStageId("");
    }

    @Override
    protected final void doExecute() throws Exception {

        REFRESH_SNAPSHOTS.create(this, null, null).toWork();

        buildContext = new BuildContext(getSparkSession().sparkContext(), config);
        buildContext.appStatusTracker().startMonitorBuildResourceState();

        try {
            build();
        } finally {
            buildContext.stop();
        }

        updateSegmentSourceBytesSize();
    }

    @Override // Copied from DFBuildJob
    protected final String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            String maxLeafTasksNums = maxLeafTasksNums();
            int factor = config.getSparkEngineTaskCoreFactor();
            int requiredCore = (int) Double.parseDouble(maxLeafTasksNums) / factor;
            log.info("The maximum number of tasks required to run the job is {}, require cores: {}", maxLeafTasksNums,
                    requiredCore);
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
                path -> path.toString().endsWith(ResourceDetectUtils.cubingDetectItemFileSuffix()));
        return ResourceDetectUtils.selectMaxValueInFiles(fileStatuses);
    }

    protected void build() throws IOException {
        Stream<NDataSegment> segmentStream = config.isSegmentParallelBuildEnabled() ? //
                readOnlySegments.parallelStream() : readOnlySegments.stream();
        AtomicLong finishedSegmentCount = new AtomicLong(0);
        val segmentsCount = readOnlySegments.size();
        segmentStream.forEach(seg -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                infos.clearCuboidsNumPerLayer(seg.getId());

                val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val exec = new BuildExec(jobStepId);

                val buildParam = new BuildParam();
                MATERIALIZED_FACT_TABLE.createStage(this, seg, buildParam, exec);
                BUILD_DICT.createStage(this, seg, buildParam, exec);
                GENERATE_FLAT_TABLE.createStage(this, seg, buildParam, exec);
                GATHER_FLAT_TABLE_STATS.createStage(this, seg, buildParam, exec);
                BUILD_LAYER.createStage(this, seg, buildParam, exec);

                buildSegment(seg, exec);

                val refreshColumnBytes = REFRESH_COLUMN_BYTES.createStage(this, seg, buildParam, exec);
                refreshColumnBytes.toWorkWithoutFinally();
                if (finishedSegmentCount.incrementAndGet() < segmentsCount) {
                    refreshColumnBytes.onStageFinished(true);
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }

    private void buildSegment(NDataSegment dataSegment, BuildExec exec) throws IOException {
        log.info("Encoding segment {}", dataSegment.getId());
        exec.buildSegment();
    }

    // Copied from DFBuildJob
    public void tryRefreshSnapshots(StageExec stageExec) throws Exception {
        SnapshotBuilder snapshotBuilder = new SnapshotBuilder(getJobId());
        if (config.isSnapshotManualManagementEnabled()) {
            log.info("Skip snapshot build in snapshot manual mode, dataflow: {}, only calculate total rows",
                    dataflowId);
            snapshotBuilder.calculateTotalRows(getSparkSession(), getDataflow(dataflowId).getModel(),
                    getIgnoredSnapshotTables());
            stageExec.onStageSkipped();
            return;
        } else if (!needBuildSnapshots()) {
            log.info("Skip snapshot build, dataflow {}, only calculate total rows", dataflowId);
            snapshotBuilder.calculateTotalRows(getSparkSession(), getDataflow(dataflowId).getModel(),
                    getIgnoredSnapshotTables());
            stageExec.onStageSkipped();
            return;
        }
        log.info("Refresh SNAPSHOT.");
        //snapshot building
        snapshotBuilder.buildSnapshot(getSparkSession(), getDataflow(dataflowId).getModel(), //
                getIgnoredSnapshotTables());
        if (config.isSnapshotSpecifiedSparkConf()) {
            // exchange sparkSession for maintained sparkConf
            log.info("exchange sparkSession using maintained sparkConf");
            exchangeSparkSession();
        }
        log.info("Finished SNAPSHOT.");
    }

    // Copied from DFBuildJob
    private void updateSegmentSourceBytesSize() {
        Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(rdSharedPath);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
            NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
            NDataflow newDF = dataflow.copy();
            val update = new NDataflowUpdate(dataflow.getUuid());
            List<NDataSegment> nDataSegments = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : segmentSourceSize.entrySet()) {
                NDataSegment segment = newDF.getSegment(entry.getKey());
                segment.setSourceBytesSize((Long) entry.getValue());
                nDataSegments.add(segment);
            }
            update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
            dataflowManager.updateDataflow(update);

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
            indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> copyForWrite //
                    .setLayoutBucketNumMapping(indexPlanManager.getIndexPlan(dataflowId).getLayoutBucketNumMapping()));
            return null;
        }, project);
    }

    public static void main(String[] args) {
        SegmentBuildJob segmentBuildJob = new SegmentBuildJob();
        segmentBuildJob.execute(args);
    }
}
