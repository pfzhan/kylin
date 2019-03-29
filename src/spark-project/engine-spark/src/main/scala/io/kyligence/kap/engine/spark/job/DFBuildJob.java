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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.engine.spark.utils.JobMetrics;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.Metrics;
import io.kyligence.kap.engine.spark.utils.QueryExecutionCache;
import io.kyligence.kap.engine.spark.utils.RepartitionHelper;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModel;

public class DFBuildJob extends SparkApplication {
    public static final Long FLAT_TABLE_FLAG = -1L;
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    public static final String tempDirSuffix = "_temp";
    protected volatile NSpanningTree nSpanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));

        try {
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream()
                    .filter(Objects::nonNull).collect(Collectors.toSet());
            nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);

            for (String segId : segmentIds) {
                NDataSegment seg = dfMgr.getDataflow(dataflowId).getSegment(segId);

                // choose source
                DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, ss, config, true);
                datasetChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();

                // note segment (source count, dictionary etc) maybe updated as a result of source select
                seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
                if (buildFromFlatTable != null) {
                    buildFromFlatTable.getDataset().cache();
                    buildFromFlatTable.setSegment(seg);
                    sources.add(buildFromFlatTable);

                    // build cuboids from flat table
                    for (IndexEntity cuboid : buildFromFlatTable.getToBuildCuboids()) {
                        recursiveBuildCuboid(seg, cuboid, buildFromFlatTable.getDataset(),
                                getToBuildMeas(nSpanningTree, cuboid, indexPlan), nSpanningTree);
                    }
                }

                sources.addAll(buildFromLayouts.values());
                // build cuboids from reused layouts
                for (NBuildSourceInfo source : buildFromLayouts.values()) {
                    for (IndexEntity root : source.getToBuildCuboids()) {
                        source.setSegment(seg);
                        recursiveBuildCuboid(seg, root, source.getDataset(),
                                indexPlan.getCuboidLayout(source.getLayoutId()).getOrderedMeasures(), nSpanningTree);
                    }
                }
                if (buildFromFlatTable != null) {
                    buildFromFlatTable.getDataset().unpersist();
                }
            }

        } finally {
            logger.info("Finish build take" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private void recursiveBuildCuboid(NDataSegment seg, IndexEntity cuboid, Dataset<Row> parent,
            Map<Integer, NDataModel.Measure> measures, NSpanningTree nSpanningTree) throws IOException {
        if (cuboid.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getSortByColumns()));
                saveAndUpdateCuboid(afterSort, seg, layout);
            }
            for (IndexEntity child : nSpanningTree.getSpanningIndexEntities(cuboid)) {
                recursiveBuildCuboid(seg, child, afterPrj, measures, nSpanningTree);
            }
        } else {
            Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, measures, seg);
            Set<Integer> meas = cuboid.getEffectiveMeasures().keySet();
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterAgg.select(NSparkCubingUtil.getColumns(rowKeys, meas))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));
                saveAndUpdateCuboid(afterSort, seg, layout);
            }
            for (IndexEntity child : nSpanningTree.getSpanningIndexEntities(cuboid)) {
                recursiveBuildCuboid(seg, child, parent, measures, nSpanningTree);
            }
        }
    }

    private void saveAndUpdateCuboid(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout) throws IOException {
        long layoutId = layout.getId();

        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);

        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = NSparkCubingUtil.getStoragePath(dataCuboid);
        String tempPath = path + tempDirSuffix;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        dataCuboid.setBuildJobId(jobId);
        dataCuboid.setRows(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT()));
        dataCuboid.setSourceRows(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT()));

        FileSystem fs = HadoopUtil.getReadFileSystem();
        if (fs.exists(new Path(tempPath))) {
            ContentSummary summary = fs.getContentSummary(new Path(tempPath));
            RepartitionHelper helper = new RepartitionHelper(KapConfig.wrap(config).getParquetStorageShardSize(),
                    KapConfig.wrap(config).getParquetStorageRepartitionThresholdSize(), summary,
                    layout.getShardByColumns());
            repartition(storage, path, ss, helper);
        } else {
            throw new RuntimeException(
                    String.format("Temp path does not exist before repartition. Temp path: %s.", tempPath));
        }

        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);

        fillCuboid(dataCuboid);

        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getUuid());
        update.setToAddOrUpdateCuboids(dataCuboid);
        NDataflowManager.getInstance(config, project).updateDataflow(update);
    }

    public static void fillCuboid(NDataLayout cuboid) throws IOException {
        String strPath = NSparkCubingUtil.getStoragePath(cuboid);
        FileSystem fs = HadoopUtil.getReadFileSystem();
        if (fs.exists(new Path(strPath))) {
            ContentSummary cs = fs.getContentSummary(new Path(strPath));
            cuboid.setFileCount(cs.getFileCount());
            cuboid.setByteSize(cs.getLength());
        } else {
            cuboid.setFileCount(0);
            cuboid.setByteSize(0);
        }
    }

    public static void repartition(NSparkCubingEngine.NSparkCubingStorage storage, String path, SparkSession ss,
            RepartitionHelper helper) throws IOException {
        String tempPath = path + tempDirSuffix;
        Path tempResourcePath = new Path(tempPath);

        FileSystem readFileSystem = HadoopUtil.getReadFileSystem();
        if (helper.needRepartition()) {
            // repartition and write to target path
            logger.info("Start repartition and rewrite");
            long start = System.currentTimeMillis();
            Dataset<Row> data;
            if (helper.needRepartitionForShardByColumns()) {
                data = storage.getFrom(tempPath, ss).repartition(helper.getRepartitionNum(),
                        NSparkCubingUtil.getColumns(helper.getShardByColumns()));
            } else {
                // repartition for single file size is too small
                data = storage.getFrom(tempPath, ss).repartition(helper.getRepartitionNum());
            }
            storage.saveTo(path, data, ss);
            if (readFileSystem.delete(tempResourcePath, true)) {
                logger.info("Delete temp cuboid path successful. Temp path: {}.", tempPath);
            } else {
                logger.error("Delete temp cuboid path wrong, leave garbage. Temp path: {}.", tempPath);
            }
            long end = System.currentTimeMillis();
            logger.info("Repartition and rewrite ends. Cost: {} ms.", end - start);
        } else {
            Path goalPath = new Path(path);
            if (readFileSystem.exists(goalPath)) {
                logger.info("Path {} is exists, delete it.", goalPath);
                readFileSystem.delete(goalPath, true);
            }
            if (readFileSystem.rename(new Path(tempPath), goalPath)) {
                logger.info("Rename temp path to target path successfully. Temp path: {}, target path: {}.", tempPath,
                        path);
            } else {
                throw new RuntimeException(String.format(
                        "Rename temp path to target path wrong. Temp path: %s, target path: %s.", tempPath, path));
            }
        }
    }

    public static ImmutableBiMap<Integer, NDataModel.Measure> getToBuildMeas(NSpanningTree nSpanningTree,
            IndexEntity indexEntity, IndexPlan indexPlan) {
        ImmutableBiMap.Builder<Integer, NDataModel.Measure> measuresBuilder = ImmutableBiMap.builder();

        Set<Integer> measures = nSpanningTree.retrieveAllMeasures(indexEntity);
        BiMap<Integer, NDataModel.Measure> meaMap = indexPlan.getEffectiveMeasures();

        for (Integer meaId : measures) {
            measuresBuilder.put(meaId, meaMap.get(meaId));
        }
        return measuresBuilder.build();
    }

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }
}
