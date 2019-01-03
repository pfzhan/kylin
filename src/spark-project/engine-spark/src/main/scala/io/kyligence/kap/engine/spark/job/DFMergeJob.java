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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.kyligence.kap.engine.spark.utils.RepartitionHelper;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.builder.DFLayoutMergeAssist;
import io.kyligence.kap.engine.spark.builder.NDataflowJob;
import io.kyligence.kap.engine.spark.utils.JobMetrics;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.Metrics;
import io.kyligence.kap.engine.spark.utils.QueryExecutionCache;

public class DFMergeJob extends NDataflowJob {
    protected static final Logger logger = LoggerFactory.getLogger(DFMergeJob.class);

    @Override
    protected Options getOptions() {
        return super.getOptions();
    }

    protected void doExecute(OptionsHelper optionsHelper) throws Exception {
        String dfName = optionsHelper.getOptionValue(OPTION_DATAFLOW_NAME);
        String newSegmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_IDS);
        Set<Long> layoutIds = getLayoutsFromPath(optionsHelper.getOptionValue(OPTION_LAYOUT_ID_PATH));
        project = optionsHelper.getOptionValue(OPTION_PROJECT_NAME);

        mergeSnapshot(dfName, newSegmentId);

        //merge and save segments
        mergeSegments(dfName, newSegmentId, layoutIds);
    }

    private void mergeSnapshot(String dataflowName, String segmentId) {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowName);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Collections.sort(mergingSegments);

        NDataflow flowCopy = dataflow.copy();
        NDataSegment segCopy = flowCopy.getSegment(segmentId);

        makeSnapshotForNewSegment(segCopy, mergingSegments);

        NDataflowUpdate update = new NDataflowUpdate(dataflowName);
        update.setToUpdateSegs(segCopy);
        mgr.updateDataflow(update);

    }

    private void makeSnapshotForNewSegment(NDataSegment newSeg, List<NDataSegment> mergingSegments) {
        NDataSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    private void mergeSegments(String dataflowName, String segmentId, Set<Long> specifiedCuboids) throws IOException {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowName);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        // collect layouts need to merge
        Map<Long, DFLayoutMergeAssist> mergeCuboidsAsssit = Maps.newConcurrentMap();
        for (NDataSegment seg : mergingSegments) {
            for (NDataCuboid cuboid : seg.getSegDetails().getCuboids()) {
                long layoutId = cuboid.getCuboidLayoutId();

                DFLayoutMergeAssist assist = mergeCuboidsAsssit.get(layoutId);
                if (assist == null) {
                    assist = new DFLayoutMergeAssist();
                    assist.addCuboid(cuboid);
                    assist.setSs(ss);
                    assist.setNewSegment(mergedSeg);
                    assist.setLayout(cuboid.getCuboidLayout());
                    assist.setToMergeSegments(mergingSegments);
                    mergeCuboidsAsssit.put(layoutId, assist);
                } else
                    assist.addCuboid(cuboid);
            }
        }

        for (DFLayoutMergeAssist assist : mergeCuboidsAsssit.values()) {
            Dataset<Row> afterMerge = assist.merge();
            NCuboidLayout layout = assist.getLayout();
            if (layout.getCuboidDesc().getId() > NCuboidDesc.TABLE_INDEX_START_ID) {
                Dataset<Row> afterSort = afterMerge
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getSortByColumns()));
                saveAndUpdateCuboid(afterSort, mergedSeg, layout, assist);
            } else {
                Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
                Dataset<Row> afterAgg = CuboidAggregator.agg(ss, afterMerge, layout.getOrderedDimensions().keySet(),
                        layout.getOrderedMeasures(), mergedSeg);
                Dataset<Row> afterSort = afterAgg.sortWithinPartitions(dimsCols);
                saveAndUpdateCuboid(afterSort, mergedSeg, layout, assist);
            }
        }
    }

    private void saveAndUpdateCuboid(Dataset<Row> dataset, NDataSegment seg, NCuboidLayout layout,
            DFLayoutMergeAssist assist) throws IOException {
        long layoutId = layout.getId();
        long sourceCount = 0L;

        for (NDataCuboid cuboid : assist.getCuboids()) {
            sourceCount += cuboid.getSourceRows();
        }

        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(seg.getDataflow(), seg.getId(), layoutId);

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);

        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = NSparkCubingUtil.getStoragePath(dataCuboid);
        String tempPath = path + DFBuildJob.tempDirSuffix;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        dataCuboid.setRows(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT()));
        dataCuboid.setSourceRows(sourceCount);
        dataCuboid.setBuildJobId(jobId);

        FileSystem fs = HadoopUtil.getReadFileSystem();
        if (fs.exists(new Path(tempPath))) {
            ContentSummary summary = fs.getContentSummary(new Path(tempPath));
            RepartitionHelper helper = new RepartitionHelper(KapConfig.wrap(config).getParquetStorageShardSize(),
                    KapConfig.wrap(config).getParquetStorageRepartitionThresholdSize(),
                    summary, layout.getShardByColumns());
            DFBuildJob.repartition(storage, path, ss, helper);
        } else {
            throw new RuntimeException(String.format(
                    "Temp path does not exist before repartition. Temp path: %s.", tempPath));
        }

        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);

        DFBuildJob.fillCuboid(dataCuboid);

        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
        update.setToAddOrUpdateCuboids(dataCuboid);
        NDataflowManager.getInstance(config, project).updateDataflow(update);
    }

    public static void main(String[] args) {
        DFMergeJob nDataflowBuildJob = new DFMergeJob();
        nDataflowBuildJob.execute(args);
    }
}
