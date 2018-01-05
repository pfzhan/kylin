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

package io.kyligence.kap.engine.spark.builder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;

public class NDataflowBuilder extends AbstractApplication {
    protected static final Logger logger = LoggerFactory.getLogger(NDataflowBuilder.class);

    @SuppressWarnings("static-access")
    public static final Option OPTION_DATAFLOW_NAME = OptionBuilder.withArgName(NBatchConstants.P_DATAFLOW_NAME)
            .hasArg().isRequired(true).withDescription("DataFlow Name").create(NBatchConstants.P_DATAFLOW_NAME);
    @SuppressWarnings("static-access")
    public static final Option OPTION_SEGMENT_IDS = OptionBuilder.withArgName(NBatchConstants.P_SEGMENT_IDS).hasArg()
            .isRequired(true).withDescription("Segment indexes").create(NBatchConstants.P_SEGMENT_IDS);
    @SuppressWarnings("static-access")
    public static final Option OPTION_LAYOUT_IDS = OptionBuilder.withArgName(NBatchConstants.P_CUBOID_LAYOUT_IDS)
            .hasArg().isRequired(true).withDescription("Layout indexes").create(NBatchConstants.P_CUBOID_LAYOUT_IDS);
    @SuppressWarnings("static-access")
    public static final Option OPTION_META_URL = OptionBuilder.withArgName(NBatchConstants.P_DIST_META_URL).hasArg()
            .isRequired(true).withDescription("Cubing metadata url").create(NBatchConstants.P_DIST_META_URL);

    @SuppressWarnings("static-access")
    public static final Option OPTION_JOB_ID = OptionBuilder.withArgName(NBatchConstants.P_JOB_ID).hasArg()
            .isRequired(true).withDescription("Current job id").create(NBatchConstants.P_JOB_ID);

    private volatile KylinConfig config;
    private volatile NSpanningTree nSpanningTree;
    private volatile String jobId;
    private volatile Map<NCuboidDesc, NDatasetChooser.DataSource> sources;
    private SparkSession ss;

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DATAFLOW_NAME);
        options.addOption(OPTION_SEGMENT_IDS);
        options.addOption(OPTION_LAYOUT_IDS);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_JOB_ID);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String dfName = optionsHelper.getOptionValue(OPTION_DATAFLOW_NAME);
        Set<Integer> segmentIds = NSparkCubingUtil.str2Ints(optionsHelper.getOptionValue(OPTION_SEGMENT_IDS));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(optionsHelper.getOptionValue(OPTION_LAYOUT_IDS));
        String hdfsMetalUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);

        config = AbstractHadoopJob.loadKylinConfigFromHdfs(hdfsMetalUrl);
        KylinConfig.setKylinConfigThreadLocal(config);
        try {
            NDataflowManager dfMgr = NDataflowManager.getInstance(config);
            NCubePlan cubePlan = dfMgr.getDataflow(dfName).getCubePlan();
            Set<NCuboidLayout> cuboids = NSparkCubingUtil.toLayouts(cubePlan, layoutIds);
            nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(cuboids, dfName);
            ss = SparkSession.builder().enableHiveSupport().getOrCreate();

            for (int segId : segmentIds) {
                NDataSegment seg = dfMgr.getDataflow(dfName).getSegment(segId);

                // choose source
                NDatasetChooser datasetChooser = new NDatasetChooser(nSpanningTree, seg, ss, config);
                sources = datasetChooser.decideSources();

                // note segment (source count, dictionary etc) maybe updated as a result of source select
                seg = dfMgr.getDataflow(dfName).getSegment(segId);

                for (NCuboidDesc root : nSpanningTree.getRootCuboidDescs()) {
                    recursiveBuildCuboid(seg, root, sources.get(root).ds, cubePlan.getEffectiveMeasures(),
                            nSpanningTree);
                }
                unpersist();
            }

        } finally {
            KylinConfig.removeKylinConfigThreadLocal();
        }
    }

    private void unpersist() {
        Set<Dataset> roots = new HashSet<>();
        for (NDatasetChooser.DataSource source : sources.values()) {
            roots.add(source.ds);
        }

        for (Dataset ds : roots) {
            ds.unpersist();
        }
    }

    private void recursiveBuildCuboid(NDataSegment seg, NCuboidDesc cuboid, Dataset<Row> parent,
            Map<Integer, NDataModel.Measure> measures, NSpanningTree nSpanningTree) throws IOException {

        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        Column[] selectedColumns = NSparkCubingUtil.getColumns(dimIndexes, measures.keySet());
        Dataset<Row> afterPrj = parent.select(selectedColumns);
        Dataset<Row> afterAgg = new NCuboidAggregator(ss, afterPrj, dimIndexes, measures).aggregate().persist();
        long cuboidRowCnt = afterAgg.count();

        int partition = estimatePartitions(afterAgg);
        Set<Integer> meas = cuboid.getOrderedMeasures().keySet();
        for (NCuboidLayout layout : nSpanningTree.getLayouts(cuboid)) {
            Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();
            Dataset<Row> afterSort = afterAgg.select(NSparkCubingUtil.getColumns(rowKeys, meas)).repartition(partition)
                    .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));
            saveAndUpdateCuboid(afterSort, cuboidRowCnt, seg, layout);
        }
        for (NCuboidDesc child : nSpanningTree.getSpanningCuboidDescs(cuboid)) {
            recursiveBuildCuboid(seg, child, afterAgg, measures, nSpanningTree);
        }
        afterAgg.unpersist();
    }

    private void saveAndUpdateCuboid(Dataset<Row> dataset, long cuboidRowCnt, NDataSegment seg, NCuboidLayout layout)
            throws IOException {
        long layoutId = layout.getId();
        NCuboidDesc root = nSpanningTree.getRootCuboidDesc(layout.getCuboidDesc());
        long sizeKB = sources.get(root).sizeKB;

        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(seg.getDataflow(), seg.getId(), layoutId);
        dataCuboid.setRows(cuboidRowCnt);
        dataCuboid.setSourceKB(sizeKB);
        dataCuboid.setBuildJobId(jobId);
        dataCuboid.setStatus(SegmentStatusEnum.READY);

        StorageFactory.createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                .saveCuboidData(dataCuboid, dataset, ss);
        fillCuboid(dataCuboid);

        NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getName());
        update.setToAddOrUpdateCuboids(dataCuboid);
        NDataflowManager.getInstance(config).updateDataflow(update);
    }

    private void fillCuboid(NDataCuboid cuboid) throws IOException {
        String strPath = NSparkCubingUtil.getStoragePath(cuboid);
        FileSystem fs = new Path(strPath).getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (fs.exists(new Path(strPath))) {
            ContentSummary cs = fs.getContentSummary(new Path(strPath));
            cuboid.setFileCount(cs.getFileCount());
            cuboid.setSizeKB(cs.getLength() / 1024);
        } else {
            cuboid.setFileCount(0);
            cuboid.setSizeKB(0);
        }
    }

    private int estimatePartitions(Dataset<Row> ds) {
        int sizeMB = (int) (NSizeEstimator.estimate(ds, 0.1f) / (1024 * 1024));
        int partition = sizeMB / KapConfig.wrap(config).getParquetStorageShardSize();
        if (partition == 0)
            partition = 1;
        return partition;
    }

    public static void main(String[] args) {
        NDataflowBuilder nDataflowBuilder = new NDataflowBuilder();
        nDataflowBuilder.execute(args);
    }
}
