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
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.application.NoRetryException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.apache.spark.sql.datasource.storage.StorageStoreUtils;
import org.apache.spark.sql.datasource.storage.WriteTaskStats;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.pushdown.SparkSqlClient;
import lombok.val;

public class DFBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private NDataflowManager dfMgr;
    private BuildLayoutWithUpdate buildLayoutWithUpdate;

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        dfMgr = NDataflowManager.getInstance(config, project);
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        checkDateFormatIfExist(project, dataflowId);
        try {
            IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream()
                    .filter(Objects::nonNull).collect(Collectors.toSet());

            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);
                NDataSegment seg = getSegment(segId);
                if (seg.isEncodingDataSkew()) {
                    logger.info("Encoding data skew , set it to true");
                    KylinBuildEnv.get().setEncodingDataSkew(true);
                }
                if (seg == null || seg.getSegRange() == null || seg.getModel() == null || seg.getIndexPlan() == null) {  // vivo
                    logger.info("Skip segment {}", segId);
                    if (seg != null)
                        logger.info("Args is {} {} {}", seg.getSegRange(), seg.getModel(), seg.getIndexPlan());
                    continue;
                }

                // choose source
                DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, true);
                datasetChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();
                infos.clearCuboidsNumPerLayer(segId);
                // build cuboids from flat table
                if (buildFromFlatTable != null) {
                    val path = datasetChooser.persistFlatTableIfNecessary();
                    if (!path.isEmpty()) {
                        persistedFlatTable.add(path);
                    }
                    if (!org.apache.commons.lang3.StringUtils.isBlank(buildFromFlatTable.getViewFactTablePath())) {
                        persistedViewFactTable.add(buildFromFlatTable.getViewFactTablePath());
                    }
                    build(Collections.singletonList(buildFromFlatTable), segId, nSpanningTree);
                }

                // build cuboids from reused layouts
                if (!buildFromLayouts.isEmpty()) {
                    build(buildFromLayouts.values(), segId, nSpanningTree);
                }
                infos.recordSpanningTree(segId, nSpanningTree);
            }
            Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(shareDir);
            updateSegmentSourceBytesSize(dataflowId, segmentSourceSize);
        } finally {
            val fs = HadoopUtil.getWorkingFileSystem();
            for (String viewPath : persistedViewFactTable) {
                fs.delete(new Path(viewPath), true);
                logger.info("Delete persisted view fact table: {}.", viewPath);
            }
            for (String path : persistedFlatTable) {
                fs.delete(new Path(path), true);
                logger.info("Delete persisted flat table: {}.", path);
            }
            logger.info("Building job takes {} ms", (System.currentTimeMillis() - start));
        }
    }

    private void updateSegmentSourceBytesSize(String dataflowId, Map<String, Object> toUpdateSegmentSourceSize) {
        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
        NDataflow newDF = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getUuid());
        List<NDataSegment> nDataSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            NDataSegment segment = newDF.getSegment(entry.getKey());
            segment.setSourceBytesSize((Long) entry.getValue());
            segment.setLastBuildTime(System.currentTimeMillis());
            if (KylinBuildEnv.get().encodingDataSkew()) {
                segment.setEncodingDataSkew(true);
            }
            nDataSegments.add(segment);
        }
        update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> {
            copyForWrite
                    .setLayoutBucketNumMapping(indexPlanManager.getIndexPlan(dataflowId).getLayoutBucketNumMapping());
        });
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            Path shareDir = config.getJobTmpShareDir(project, jobId);
            String maxLeafTasksNums = maxLeafTasksNums(shareDir);
            logger.info("The maximum number of tasks required to run the job is {}", maxLeafTasksNums);
            val config = KylinConfig.getInstanceFromEnv();
            val factor = config.getSparkEngineTaskCoreFactor();
            int i = Double.valueOf(maxLeafTasksNums).intValue() / factor;
            logger.info("require cores: " + i);
            return String.valueOf(i);
        } else {
            return SparkJobConstants.DEFAULT_REQUIRED_CORES;
        }
    }

    private String maxLeafTasksNums(Path shareDir) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(shareDir,
                path -> path.toString().endsWith(ResourceDetectUtils.cubingDetectItemFileSuffix()));
        return ResourceDetectUtils.selectMaxValueInFiles(fileStatuses);
    }

    private NDataSegment getSegment(String segId) {
        // ensure the seg is the latest.
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return dfMgr.getDataflow(dataflowId).getSegment(segId);
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, String segId, NSpanningTree st)
            throws IOException {

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
    private List<NBuildSourceInfo> buildLayer(Collection<NBuildSourceInfo> buildSourceInfos, String segId,
            NSpanningTree st) throws IOException {
        val seg = getSegment(segId);
        int cuboidsNumInLayer = 0;

        // build current layer
        List<IndexEntity> allIndexesInCurrentLayer = new ArrayList<>();
        for (NBuildSourceInfo info : buildSourceInfos) {
            Collection<IndexEntity> toBuildCuboids = info.getToBuildCuboids();
            infos.recordParent2Children(seg.getLayout(info.getLayoutId()),
                    toBuildCuboids.stream().map(IndexEntity::getId).collect(Collectors.toList()));
            cuboidsNumInLayer += toBuildCuboids.size();
            Preconditions.checkState(!toBuildCuboids.isEmpty(), "To be built cuboids is empty.");
            Dataset<Row> parentDS = info.getParentDS();

            for (IndexEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                    @Override
                    public String getName() {
                        return "build-index-" + index.getId();
                    }

                    @Override
                    public List<NDataLayout> build() throws IOException {
                        return buildIndex(seg, index, parentDS, st, info.getLayoutId());
                    }
                }, config);
                allIndexesInCurrentLayer.add(index);
            }
        }

        infos.recordCuboidsNumPerLayer(segId, cuboidsNumInLayer);
        buildLayoutWithUpdate.updateLayout(seg, config, project);

        // decided the next layer by current layer's all indexes.
        st.decideTheNextLayer(allIndexesInCurrentLayer, getSegment(segId));
        return constructTheNextLayerBuildInfos(st, seg, allIndexesInCurrentLayer);
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
                LayoutEntity layout = new ArrayList<>(st.getLayouts(index)).get(0);
                theRootLevelBuildInfos.setLayoutId(layout.getId());
                theRootLevelBuildInfos.setParentStorageDF(StorageStoreUtils.toDF(seg, layout, ss));
                theRootLevelBuildInfos.setToBuildCuboids(children);
                childrenBuildSourceInfos.add(theRootLevelBuildInfos);
            }
        }
        // return the next to be built layer.
        return childrenBuildSourceInfos;
    }

    private List<NDataLayout> buildIndex(NDataSegment seg, IndexEntity cuboid, Dataset<Row> parent,
            NSpanningTree nSpanningTree, long parentId) throws IOException {
        String parentName = String.valueOf(parentId);
        if (parentId == DFChooser.FLAT_TABLE_FLAG()) {
            parentName = "flat table";
        }
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.getId());
        LinkedList<NDataLayout> layouts = Lists.newLinkedList();
        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        if (cuboid.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
                ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(orderedDims));
                layouts.add(saveAndUpdateLayout(afterSort, seg, layout));
            }
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getEffectiveMeasures(), seg,
                    nSpanningTree);
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
                ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
                Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();

                Dataset<Row> afterSort = afterAgg
                        .select(NSparkCubingUtil.getColumns(rowKeys, layout.getOrderedMeasures().keySet()))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));

                layouts.add(saveAndUpdateLayout(afterSort, seg, layout));
            }
        }
        ss.sparkContext().setJobDescription(null);
        logger.info("Finished Build index :{}, in segment:{}", cuboid.getId(), seg.getId());
        return layouts;
    }

    private NDataLayout saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout)
            throws IOException {
        long layoutId = layout.getId();
        NDataLayout dataLayout = getDataLayout(seg, layoutId);
        val path = NSparkCubingUtil.getStoragePath(seg, layoutId);
        int storageType = layout.getModel().getStorageType();
        StorageStore storage = StorageStoreFactory.create(storageType);
        WriteTaskStats taskStats = storage.save(layout, new Path(path), KapConfig.wrap(config), dataset);
        dataLayout.setBuildJobId(jobId);
        long rowCount = taskStats.numRows();
        if (rowCount == -1) {
            KylinBuildEnv.get().buildJobInfos().recordAbnormalLayouts(layout.getId(),
                    "Job metrics seems null, use count() to collect cuboid rows.");
            logger.info("Can not get cuboid row cnt.");
        }
        dataLayout.setRows(rowCount);
        dataLayout.setSourceRows(taskStats.sourceRows());
        dataLayout.setPartitionNum(taskStats.numBucket());
        dataLayout.setPartitionValues(taskStats.partitionValues());
        dataLayout.setFileCount(taskStats.numFiles());
        dataLayout.setByteSize(taskStats.numBytes());
        return dataLayout;
    }

    private NDataLayout getDataLayout(NDataSegment seg, long layoutId) {
        return NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }

    private void checkDateFormatIfExist(String project, String modelId) throws NoRetryException {
        if (config.isUTEnv()) {
            return;
        }
        val modelManager = NDataModelManager.getInstance(config, project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        val partitionDesc = modelDesc.getPartitionDesc();
        if (partitionDesc == null || org.apache.commons.lang.StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
                || org.apache.commons.lang.StringUtils.isEmpty(partitionDesc.getPartitionDateFormat()))
            return;

        String partitionColumn = modelDesc.getPartitionDesc().getPartitionDateColumnRef().getExpressionInSourceDB();

        String sql = String.format("select %s from %s where %s is not null limit 1", partitionColumn,
                modelDesc.getRootFactTableName(), partitionColumn);
        val res = SparkSqlClient.executeSql(ss, sql, UUID.randomUUID());
        if (CollectionUtils.isEmpty(res.getFirst())) {
            return;
        }
        try {
            val dateString = res.getFirst().get(0).get(0);
            val sdf = new SimpleDateFormat(modelDesc.getPartitionDesc().getPartitionDateFormat());
            val date = sdf.parse(dateString);
            if (date == null || !dateString.equals(sdf.format(date))) {
                throw new NoRetryException("date format not match");
            }
        } catch (ParseException | NoRetryException e) {
            throw new NoRetryException("date format not match");
        }
    }
}
