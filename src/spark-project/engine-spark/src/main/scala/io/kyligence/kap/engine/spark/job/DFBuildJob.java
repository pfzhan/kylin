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
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.engine.spark.builder.NDataflowJob;
import io.kyligence.kap.engine.spark.builder.NDatasetChooser;
import io.kyligence.kap.engine.spark.builder.NSizeEstimator;
import io.kyligence.kap.metadata.model.NDataModel;

public class DFBuildJob extends NDataflowJob {
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    protected volatile NSpanningTree nSpanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();

    @Override
    protected Options getOptions() {
        return super.getOptions();
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        super.execute(optionsHelper);
        String dfName = optionsHelper.getOptionValue(OPTION_DATAFLOW_NAME);
        project = optionsHelper.getOptionValue(OPTION_PROJECT_NAME);
        Set<Integer> segmentIds = NSparkCubingUtil.str2Ints(optionsHelper.getOptionValue(OPTION_SEGMENT_IDS));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(optionsHelper.getOptionValue(OPTION_LAYOUT_IDS));

        try {
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            NCubePlan cubePlan = dfMgr.getDataflow(dfName).getCubePlan();
            Set<NCuboidLayout> cuboids = NSparkCubingUtil.toLayouts(cubePlan, layoutIds);
            nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(cuboids, dfName);

            for (int segId : segmentIds) {
                NDataSegment seg = dfMgr.getDataflow(dfName).getSegment(segId);

                // choose source
                DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, ss, config);
                datasetChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();

                // note segment (source count, dictionary etc) maybe updated as a result of source select
                seg = dfMgr.getDataflow(dfName).getSegment(segId);
                if (buildFromFlatTable != null) {
                    buildFromFlatTable.getDataset().cache();
                    buildFromFlatTable.setSegment(seg);
                    sources.add(buildFromFlatTable);
                    // build cuboids from flat table
                    for (NCuboidDesc cuboid : buildFromFlatTable.getToBuildCuboids()) {
                        recursiveBuildCuboid(seg, cuboid, buildFromFlatTable.getDataset(),
                                cubePlan.getEffectiveMeasures(), nSpanningTree);
                    }
                }

                sources.addAll(buildFromLayouts.values());
                // build cuboids from reused layouts
                for (NBuildSourceInfo source : buildFromLayouts.values()) {
                    for (NCuboidDesc root : source.getToBuildCuboids()) {
                        source.setSegment(seg);
                        recursiveBuildCuboid(seg, root, source.getDataset(),
                                cubePlan.getCuboidLayout(source.getLayoutId()).getOrderedMeasures(), nSpanningTree);
                    }
                }
                if (buildFromFlatTable != null)
                    buildFromFlatTable.getDataset().unpersist();
            }

        } finally {
            KylinConfig.removeKylinConfigThreadLocal();
            logger.info("Finish build take" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private void recursiveBuildCuboid(NDataSegment seg, NCuboidDesc cuboid, Dataset<Row> parent,
            Map<Integer, NDataModel.Measure> measures, NSpanningTree nSpanningTree) throws IOException {
        if (cuboid.getId() >= NCuboidDesc.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().size() == 0);
            Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            long cuboidRowCnt = afterPrj.count();
            // TODO: shard number should respect the shard column defined in cuboid
            int partition = estimatePartitions(afterPrj, config);
            for (NCuboidLayout layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> intermediate = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims));
                intermediate = repartitionDataSet(intermediate, partition, layout.getShardByColumns());
                Dataset<Row> afterSort = intermediate
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getSortByColumns()));
                saveAndUpdateCuboid(afterSort, cuboidRowCnt, seg, layout);
            }
            for (NCuboidDesc child : nSpanningTree.getSpanningCuboidDescs(cuboid)) {
                recursiveBuildCuboid(seg, child, afterPrj, measures, nSpanningTree);
            }
        } else {
            Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, measures, seg);
            long cuboidRowCnt = afterAgg.count();

            int partition = estimatePartitions(afterAgg, config);
            Set<Integer> meas = cuboid.getEffectiveMeasures().keySet();
            for (NCuboidLayout layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();
                Dataset<Row> intermediate = afterAgg.select(NSparkCubingUtil.getColumns(rowKeys, meas));
                intermediate = repartitionDataSet(intermediate, partition, layout.getShardByColumns());
                Dataset<Row> afterSort = intermediate.sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));
                saveAndUpdateCuboid(afterSort, cuboidRowCnt, seg, layout);
            }
            for (NCuboidDesc child : nSpanningTree.getSpanningCuboidDescs(cuboid)) {
                recursiveBuildCuboid(seg, child, afterAgg, measures, nSpanningTree);
            }
            //            afterAgg.unpersist();
        }
    }

    private void saveAndUpdateCuboid(Dataset<Row> dataset, long cuboidRowCnt, NDataSegment seg, NCuboidLayout layout)
            throws IOException {
        long layoutId = layout.getId();
        NCuboidDesc root = nSpanningTree.getRootCuboidDesc(layout.getCuboidDesc());

        NBuildSourceInfo sourceInfo = NDatasetChooser.getDataSourceByCuboid(sources, root, seg);
        long sourceByteSize = sourceInfo.getByteSize();
        long sourceCount = sourceInfo.getCount();

        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(seg.getDataflow(), seg.getId(), layoutId);
        dataCuboid.setRows(cuboidRowCnt);
        dataCuboid.setSourceByteSize(sourceByteSize);
        dataCuboid.setSourceRows(sourceCount);
        dataCuboid.setBuildJobId(jobId);
        dataCuboid.setStatus(SegmentStatusEnum.READY);

        StorageFactory.createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                .saveCuboidData(dataCuboid, dataset, ss);
        fillCuboid(dataCuboid);

        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
        update.setToAddOrUpdateCuboids(dataCuboid);
        NDataflowManager.getInstance(config, project).updateDataflow(update);
    }

    public static Dataset<Row> repartitionDataSet(Dataset<Row> ds, int repartitionNum, List<Integer> shardByColumnIds) {
        if (shardByColumnIds == null || shardByColumnIds.size() == 0) {
            return ds.repartition(repartitionNum);
        } else {
            return ds.repartition(repartitionNum, NSparkCubingUtil.getColumns(shardByColumnIds));
        }
    }

    public static void fillCuboid(NDataCuboid cuboid) throws IOException {
        String strPath = NSparkCubingUtil.getStoragePath(cuboid);
        FileSystem fs = new Path(strPath).getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (fs.exists(new Path(strPath))) {
            ContentSummary cs = fs.getContentSummary(new Path(strPath));
            cuboid.setFileCount(cs.getFileCount());
            cuboid.setByteSize(cs.getLength());
        } else {
            cuboid.setFileCount(0);
            cuboid.setByteSize(0);
        }
    }

    public static int estimatePartitions(Dataset<Row> ds, KylinConfig config) {
        int sizeMB = (int) (NSizeEstimator.estimate(ds, 0.1f) / (1024 * 1024));
        int partition = sizeMB / KapConfig.wrap(config).getParquetStorageShardSize();
        if (partition == 0)
            partition = 1;
        return partition;
    }

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }
}
