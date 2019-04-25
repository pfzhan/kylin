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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.engine.spark.utils.BuildUtils;
import io.kyligence.kap.engine.spark.utils.JobMetrics;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.Metrics;
import io.kyligence.kap.engine.spark.utils.QueryExecutionCache;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;

public class DFBuildJob extends SparkApplication {
    public static final Long FLAT_TABLE_FLAG = -1L;
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private NDataflowManager dfMgr;

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        dfMgr = NDataflowManager.getInstance(config, project);

        try {
            IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream()
                    .filter(Objects::nonNull).collect(Collectors.toSet());

            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);
                NDataSegment seg = getSegment(segId);

                // choose source
                DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, ss, config, true);
                datasetChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();

                // build cuboids from flat table
                if (buildFromFlatTable != null) {
                    build(Collections.singletonList(buildFromFlatTable), segId, nSpanningTree);
                }

                // build cuboids from reused layouts
                build(buildFromLayouts.values(), segId, nSpanningTree);
                KylinBuildEnv.get().seg2SpanningTree().put(segId, nSpanningTree);
            }
        } finally {
            logger.info("Finish build take" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private NDataSegment getSegment(String segId) {
        // ensure the seg is the latest.
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return dfMgr.getDataflow(dataflowId).getSegment(segId);
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, String segId, NSpanningTree st) throws IOException {

        val theFirstLevelBuildInfos = buildLayer(buildSourceInfos, segId, st);
        val queue = new LinkedList<List<NBuildSourceInfo>>();

        if (!theFirstLevelBuildInfos.isEmpty()) {
            queue.offer(theFirstLevelBuildInfos);
        }

        while (!queue.isEmpty()) {
            val buildInfos = queue.poll();
            val theNextLayer = buildLayer(buildInfos, segId, st);
            if (!theNextLayer.isEmpty()) {
                queue.offer(theNextLayer);
            }
        }

    }

    // build current layer and return the next layer to be built.
    private List<NBuildSourceInfo> buildLayer(Collection<NBuildSourceInfo> buildSourceInfos, String segId, NSpanningTree st) throws IOException {
        val seg = getSegment(segId);

        // build current layer
        List<IndexEntity> allIndexesInCurrentLayer = new ArrayList<>();
        for (NBuildSourceInfo info : buildSourceInfos) {
            Collection<IndexEntity> toBuildCuboids = info.getToBuildCuboids();
            Preconditions.checkState(!toBuildCuboids.isEmpty(), "To be built cuboids is empty.");
            Dataset<Row> parentDS = info.getParentDS();

            boolean needCache = toBuildCuboids.size() >= KylinConfig.getInstanceFromEnv().getBuildingCacheThreshold();
            if (needCache) {
                parentDS.cache();
            }

            for (IndexEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildIndex(seg, index, parentDS, st);
                allIndexesInCurrentLayer .add(index);
            }

            if (needCache) {
                parentDS.unpersist();
            }
        }

        // decided the next layer by current layer's all indexes.
        st.decideTheNextLayer(allIndexesInCurrentLayer , getSegment(segId));

        return constructTheNextLayerBuildInfos(st, seg, allIndexesInCurrentLayer );
    }

    // decided and construct the next layer.
    private List<NBuildSourceInfo> constructTheNextLayerBuildInfos( //
            NSpanningTree st, //
            NDataSegment seg, //
            Collection<IndexEntity> allIndexesInCurrentLayer) { //

        val childrenBuildSourceInfos = new ArrayList<NBuildSourceInfo>();
        for (IndexEntity index : allIndexesInCurrentLayer) {
            val children = st.getChildrenByIndexPlan(index);

            if (!children.isEmpty()) {
                val theRootLevelBuildInfos = new NBuildSourceInfo();
                theRootLevelBuildInfos.setSparkSession(ss);
                String path = NSparkCubingUtil.getStoragePath(getDataCuboid(seg, index.getLastLayout().getId()));
                theRootLevelBuildInfos.setParentStoragePath(path);
                theRootLevelBuildInfos.setToBuildCuboids(children);
                childrenBuildSourceInfos.add(theRootLevelBuildInfos);
            }
        }
        // return the next to be built layer.
        return childrenBuildSourceInfos;
    }

    private void buildIndex(NDataSegment seg, IndexEntity cuboid, Dataset<Row> parent, NSpanningTree nSpanningTree) throws IOException {
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.getId());
        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        if (cuboid.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getSortByColumns()));
                saveAndUpdateLayout(afterSort, seg, layout);
            }
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getEffectiveMeasures(), seg);
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();

                Dataset<Row> afterSort = afterAgg
                        .select(NSparkCubingUtil.getColumns(rowKeys, layout.getOrderedMeasures().keySet()))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));

                saveAndUpdateLayout(afterSort, seg, layout);
            }
        }

    }

    private void saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout) throws IOException {
        long layoutId = layout.getId();

        NDataLayout dataCuboid = getDataCuboid(seg, layoutId);

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);

        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = NSparkCubingUtil.getStoragePath(dataCuboid);
        String tempPath = path + TEMP_DIR_SUFFIX;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        dataCuboid.setBuildJobId(jobId);
        dataCuboid.setRows(metrics.getMetrics(Metrics.CUBOID_ROWS_CNT()));
        dataCuboid.setSourceRows(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT()));
        BuildUtils.repartitionIfNeed(layout, dataCuboid, storage, path, tempPath, KapConfig.wrap(config), ss);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(dataCuboid);
        BuildUtils.updateDataFlow(seg, dataCuboid, config, project);
    }

    private NDataLayout getDataCuboid(NDataSegment seg, long layoutId) {
        return NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
    }

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }
}
