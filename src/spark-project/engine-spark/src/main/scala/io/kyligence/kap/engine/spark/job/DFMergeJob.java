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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.apache.spark.sql.datasource.storage.WriteTaskStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.DFLayoutMergeAssist;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class DFMergeJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFMergeJob.class);
    protected BuildLayoutWithUpdate buildLayoutWithUpdate;

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        String newSegmentId = getParam(NBatchConstants.P_SEGMENT_IDS);
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        mergeSnapshot(dataflowId, newSegmentId);

        //merge flat table
        mergeFlatTable(dataflowId, newSegmentId);

        //merge and save segments
        mergeSegments(dataflowId, newSegmentId, layoutIds);
    }

    private void mergeSnapshot(String dataflowId, String segmentId) {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Collections.sort(mergingSegments);
        infos.clearMergingSegments();
        infos.recordMergingSegments(mergingSegments);

        NDataflow flowCopy = dataflow.copy();
        NDataSegment segCopy = flowCopy.getSegment(segmentId);

        makeSnapshotForNewSegment(segCopy, mergingSegments);
        mergeColumnSizeForNewSegment(segCopy, mergingSegments);
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToUpdateSegs(segCopy);
        mgr.updateDataflow(update);

    }

    private void mergeColumnSizeForNewSegment(NDataSegment segCopy, List<NDataSegment> mergingSegments) {
        SourceUsageManager usageManager = SourceUsageManager.getInstance(config);
        Map<String, Long> result = Maps.newHashMap();
        for (val seg : mergingSegments) {
            Map<String, Long> newByteSizeMap = MapUtils.isEmpty(seg.getColumnSourceBytes())
                    ? usageManager.calcAvgColumnSourceBytes(seg)
                    : seg.getColumnSourceBytes();
            mergeByteSizeMap(result, newByteSizeMap);
        }
        segCopy.setColumnSourceBytes(result);
    }

    private void mergeByteSizeMap(Map<String, Long> result, Map<String, Long> newByteSizeMap) {
        for (Map.Entry<String, Long> entry : newByteSizeMap.entrySet()) {
            val oriSize = result.getOrDefault(entry.getKey(), 0L);
            result.put(entry.getKey(), oriSize + entry.getValue());
        }
    }

    private void makeSnapshotForNewSegment(NDataSegment newSeg, List<NDataSegment> mergingSegments) {
        NDataSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    protected List<NDataSegment> getMergingSegments(NDataflow dataflow, NDataSegment mergedSeg) {
        return dataflow.getMergingSegments(mergedSeg);
    }

    protected void mergeSegments(String dataflowId, String segmentId, Set<Long> specifiedCuboids) throws IOException {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = getMergingSegments(dataflow, mergedSeg);

        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = generateMergeAssist(mergingSegments, ss, mergedSeg);
        for (DFLayoutMergeAssist assist : mergeCuboidsAssist.values()) {

            Dataset<Row> afterMerge = assist.merge();
            LayoutEntity layout = assist.getLayout();
            Dataset<Row> afterSort;
            if (IndexEntity.isTableIndex(layout.getIndex().getId())) {
                afterSort = afterMerge
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet()));
            } else {
                Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
                Dataset<Row> afterAgg = CuboidAggregator.agg(ss, afterMerge, layout.getOrderedDimensions().keySet(),
                        layout.getOrderedMeasures(), mergedSeg, null);
                afterSort = afterAgg.sortWithinPartitions(dimsCols);
            }
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public String getName() {
                    return "merge-layout-" + layout.getId();
                }

                @Override
                public List<NDataLayout> build() throws IOException {
                    return Lists.newArrayList(saveAndUpdateCuboid(afterSort, mergedSeg, layout, assist));
                }
            }, config);
        }

        buildLayoutWithUpdate.updateLayout(mergedSeg, config, project);
    }

    public static Map<Long, DFLayoutMergeAssist> generateMergeAssist(List<NDataSegment> mergingSegments,
            SparkSession ss, NDataSegment mergedSeg) {
        // collect layouts need to merge
        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = Maps.newConcurrentMap();
        for (NDataSegment seg : mergingSegments) {
            for (NDataLayout cuboid : seg.getSegDetails().getLayouts()) {
                long layoutId = cuboid.getLayoutId();

                DFLayoutMergeAssist assist = mergeCuboidsAssist.get(layoutId);
                if (assist == null) {
                    assist = new DFLayoutMergeAssist();
                    assist.addCuboid(cuboid);
                    assist.setSs(ss);
                    assist.setNewSegment(mergedSeg);
                    assist.setLayout(cuboid.getLayout());
                    assist.setToMergeSegments(mergingSegments);
                    mergeCuboidsAssist.put(layoutId, assist);
                } else
                    assist.addCuboid(cuboid);
            }
        }
        return mergeCuboidsAssist;
    }

    private NDataLayout saveAndUpdateCuboid(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout,
            DFLayoutMergeAssist assist) throws IOException {
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "merge");
        long layoutId = layout.getId();
        long sourceCount = 0L;

        for (NDataLayout cuboid : assist.getCuboids()) {
            sourceCount += cuboid.getSourceRows();
        }
        NDataLayout dataLayout = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
        val path = NSparkCubingUtil.getStoragePath(seg, layoutId);
        int storageType = layout.getModel().getStorageType();
        StorageStore storage = StorageStoreFactory.create(storageType);
        ss.sparkContext().setJobDescription("Merge layout " + layoutId);
        WriteTaskStats taskStats = storage.save(layout, new Path(path), KapConfig.wrap(config), dataset);
        ss.sparkContext().setJobDescription(null);
        dataLayout.setBuildJobId(jobId);
        long rowCount = taskStats.numRows();
        if (rowCount == -1) {
            KylinBuildEnv.get().buildJobInfos().recordAbnormalLayouts(layout.getId(),
                    "Job metrics seems null, use count() to collect cuboid rows.");
            logger.info("Can not get cuboid row cnt.");
        }
        dataLayout.setRows(rowCount);
        dataLayout.setSourceRows(sourceCount);
        dataLayout.setPartitionNum(taskStats.numBucket());
        dataLayout.setPartitionValues(taskStats.partitionValues());
        dataLayout.setFileCount(taskStats.numFiles());
        dataLayout.setByteSize(taskStats.numBytes());
        return dataLayout;
    }

    private List<String> predicatedSegments(Predicate<NDataSegment> predicate, final List<NDataSegment> sources) {
        return sources.stream().filter(predicate).map(NDataSegment::getId).collect(Collectors.toList());
    }

    private List<Path> getSegmentFlatTables(String dataFlowId, List<NDataSegment> segments) {
        // check flat table ready
        List<String> notReadies = predicatedSegments((NDataSegment segment) -> !segment.isFlatTableReady(), segments);
        if (CollectionUtils.isNotEmpty(notReadies)) {
            final String logStr = String.join(",", notReadies);
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] Plan to merge segments' flat table, "
                    + "but found that some's flat table were not ready like [{}]", logStr);
            return Lists.newArrayList();
        }

        // check flat table exists
        final FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<String> notExists = predicatedSegments((NDataSegment segment) -> {
            try {
                Path p = config.getFlatTableDir(project, dataFlowId, segment.getId());
                return !fs.exists(p);
            } catch (IOException ioe) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] When checking segment's flat table exists, segment id: {}",
                        segment.getId(), ioe);
                return true;
            }
        }, segments);
        if (CollectionUtils.isNotEmpty(notExists)) {
            final String logStr = String.join(",", notExists);
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] Plan to merge segments' flat table, "
                    + "but found that some's flat table were not exists like [{}]", logStr);
            return Lists.newArrayList();
        }

        return segments.stream().map(segment -> config.getFlatTableDir(project, dataFlowId, segment.getId()))
                .collect(Collectors.toList());
    }

    private List<String> getSelectedColumns(List<NDataSegment> segments) {
        List<String> selectedColumns = null;
        for (NDataSegment segment : segments) {
            if (Objects.isNull(selectedColumns)) {
                selectedColumns = segment.getSelectedColumns();
            } else {
                selectedColumns = segment.getSelectedColumns().stream()
                        .filter(Sets.newHashSet(selectedColumns)::contains).collect(Collectors.toList());
            }

            if (CollectionUtils.isEmpty(selectedColumns)) {
                logger.warn("Segments intersected selected columns is empty when merging segment {}", segment.getId());
                return selectedColumns;
            }
        }
        return selectedColumns;
    }

    private void mergeFlatTable(String dataFlowId, String segmentId) {
        if (!config.isPersistFlatTableEnabled()) {
            logger.info("project {} flat table persisting is not enabled.", project);
            return;
        }
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataFlow = dfMgr.getDataflow(dataFlowId);
        final NDataSegment mergedSeg = dataFlow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = getMergingSegments(dataFlow, mergedSeg);
        if (mergingSegments.size() < 1) {
            return;
        }

        Collections.sort(mergingSegments);

        // check flat table paths
        List<Path> flatTables = getSegmentFlatTables(dataFlowId, mergingSegments);
        if (CollectionUtils.isEmpty(flatTables)) {
            return;
        }

        // selected cols must be all the same
        Set<String> selectedCols = new HashSet<>(mergingSegments.get(0).getSelectedColumns());
        for (int i = 1; i < mergingSegments.size(); i++) {
            if (selectedCols.size() != mergingSegments.get(i).getSelectedColumns().size()
                    || !selectedCols.containsAll(mergingSegments.get(i).getSelectedColumns())) {
                return;
            }
        }
        List<String> selectedColumns = new ArrayList<>(mergingSegments.get(0).getSelectedColumns());

        Dataset<Row> flatTableDs = null;
        for (Path p : flatTables) {
            Dataset<Row> newDs = ss.read().parquet(p.toString());
            flatTableDs = Objects.isNull(flatTableDs) ? newDs : flatTableDs.union(newDs);
        }

        if (Objects.isNull(flatTableDs)) {
            return;
        }

        // persist storage
        Path newPath = config.getFlatTableDir(project, dataFlowId, segmentId);
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "merge");
        ss.sparkContext().setJobDescription("Persist flat table.");
        flatTableDs.write().mode(SaveMode.Overwrite).parquet(newPath.toString());

        final String selectedColumnsStr = String.join(",", selectedColumns);
        logger.info("Persist merged flat tables to path {} with selected columns [{}], "
                + "new segment id: {}, dataFlowId: {}", newPath, selectedColumnsStr, segmentId, dataFlowId);

        NDataflow dfCopied = dataFlow.copy();
        NDataSegment segmentCopied = dfCopied.getSegment(segmentId);
        segmentCopied.setFlatTableReady(true);
        segmentCopied.setSelectedColumns(selectedColumns);

        NDataflowUpdate update = new NDataflowUpdate(dataFlowId);
        update.setToUpdateSegs(segmentCopied);
        dfMgr.updateDataflow(update);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    public static void main(String[] args) {
        DFMergeJob nDataflowBuildJob = new DFMergeJob();
        nDataflowBuildJob.execute(args);
    }

}
