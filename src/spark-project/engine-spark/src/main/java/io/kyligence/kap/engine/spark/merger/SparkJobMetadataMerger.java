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

package io.kyligence.kap.engine.spark.merger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.LayoutPartition;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.metadata.cube.model.SegmentPartition;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Getter;
import lombok.val;

public abstract class SparkJobMetadataMerger extends MetadataMerger {
    private static final Logger log = LoggerFactory.getLogger(SparkJobMetadataMerger.class);
    @Getter
    private final String project;

    protected SparkJobMetadataMerger(KylinConfig config, String project) {
        super(config);
        this.project = project;
    }

    public KylinConfig getProjectConfig(ResourceStore remoteStore) throws IOException {
        val globalConfig = KylinConfig.createKylinConfig(
                KylinConfig.streamToProps(remoteStore.getResource("/kylin.properties").getByteSource().openStream()));
        val projectConfig = JsonUtil
                .readValue(remoteStore.getResource("/_global/project/" + project + ".json").getByteSource().read(),
                        ProjectInstance.class)
                .getOverrideKylinProps();
        return KylinConfigExt.createInstance(globalConfig, projectConfig);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds, //
            ResourceStore remoteResourceStore, JobTypeEnum jobType, Set<Long> partitions) {
        return new NDataLayout[0];
    }

    public void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids) {
        // make sure call this method in the last step, if 4th step is added, please modify the logic accordingly
        String model = buildTask.getTargetSubject();
        // get end time from current task instead of parent job，since parent job is in running state at this time
        long buildEndTime = buildTask.getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = 0;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            byteSize += dataCuboid.getByteSize();
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);
        // update
        NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
        executableManager.updateJobOutput(buildTask.getParentId(), null, null, null, null, byteSize);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig,
                buildTask.getProject());
        jobStatisticsManager.updateStatistics(startOfDay, model, duration, byteSize, 0);
    }

    protected void mergeSnapshotMeta(NDataflow dataflow, ResourceStore remoteResourceStore) {
        if (!isSnapshotManualManagementEnabled(remoteResourceStore)) {
            val remoteTblMgr = NTableMetadataManager.getInstance(remoteResourceStore.getConfig(), getProject());
            val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            dataflow.getModel().getLookupTables().forEach(remoteTableRef -> {
                val tableName = remoteTableRef.getTableIdentity();
                val localTbDesc = localTblMgr.getTableDesc(tableName);
                val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
                if (remoteTbDesc == null) {
                    return;
                }

                val copy = localTblMgr.copyForWrite(localTbDesc);
                copy.setLastSnapshotPath(remoteTbDesc.getLastSnapshotPath());
                localTblMgr.updateTableDesc(copy);
            });
        }
    }

    protected void mergeTableExtMeta(NDataflow dataflow, ResourceStore remoteResourceStore) {
        val remoteTblMgr = NTableMetadataManager.getInstance(remoteResourceStore.getConfig(), getProject());
        val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        dataflow.getModel().getLookupTables().forEach(remoteTableRef -> {
            val tableName = remoteTableRef.getTableIdentity();
            val localTbDesc = localTblMgr.getTableDesc(tableName);
            val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
            if (remoteTbDesc == null) {
                return;
            }

            val remoteTblExtDesc = remoteTblMgr.getOrCreateTableExt(remoteTbDesc);
            val copyExt = localTblMgr.copyForWrite(localTblMgr.getOrCreateTableExt(localTbDesc));
            if (remoteTblExtDesc.getOriginalSize() != -1) {
                copyExt.setOriginalSize(remoteTblExtDesc.getOriginalSize());
            }
            copyExt.setTotalRows(remoteTblExtDesc.getTotalRows());
            localTblMgr.saveTableExt(copyExt);
        });
    }

    protected boolean isSnapshotManualManagementEnabled(ResourceStore configStore) {
        try {
            val projectConfig = getProjectConfig(configStore);
            if (!projectConfig.isSnapshotManualManagementEnabled()) {
                return false;
            }
        } catch (IOException e) {
            log.error("Fail to get project config.");
        }
        return true;
    }

    // Note: DO NOT copy max bucketId in segment.
    public NDataSegment upsertSegmentPartition(NDataSegment localSegment, NDataSegment newSegment, //
            Set<Long> partitionIds) {
        localSegment.getMultiPartitions().removeIf(partition -> partitionIds.contains(partition.getPartitionId()));
        List<SegmentPartition> upsertPartitions = newSegment.getMultiPartitions().stream() //
                .filter(partition -> partitionIds.contains(partition.getPartitionId())).collect(Collectors.toList());
        final long lastBuildTime = System.currentTimeMillis();
        upsertPartitions.forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.READY);
            partition.setLastBuildTime(lastBuildTime);
        });
        localSegment.getMultiPartitions().addAll(upsertPartitions);
        List<SegmentPartition> partitions = localSegment.getMultiPartitions();
        localSegment.setSourceCount(partitions.stream() //
                .mapToLong(SegmentPartition::getSourceCount).sum());
        final Map<String, Long> merged = Maps.newHashMap();
        partitions.stream().map(SegmentPartition::getColumnSourceBytes) //
                .forEach(item -> //
                item.forEach((k, v) -> //
                merged.put(k, v + merged.getOrDefault(k, 0L))));
        localSegment.setColumnSourceBytes(merged);
        // KE-18417 snapshot management.
        localSegment.setLastBuildTime(newSegment.getLastBuildTime());
        localSegment.setSourceBytesSize(newSegment.getSourceBytesSize());
        localSegment.setLastBuildTime(lastBuildTime);
        return localSegment;
    }

    public NDataLayout upsertLayoutPartition(NDataLayout localLayout, NDataLayout newLayout, Set<Long> partitionIds) {
        if (localLayout == null) {
            return newLayout;
        }
        localLayout.getMultiPartition().removeIf(partition -> partitionIds.contains(partition.getPartitionId()));
        List<LayoutPartition> upsertLayouts = newLayout.getMultiPartition().stream() //
                .filter(partition -> partitionIds.contains(partition.getPartitionId())).collect(Collectors.toList());
        localLayout.getMultiPartition().addAll(upsertLayouts);
        List<LayoutPartition> partitions = localLayout.getMultiPartition();
        localLayout.setRows(partitions.stream() //
                .mapToLong(LayoutPartition::getRows).sum());
        localLayout.setSourceRows(partitions.stream() //
                .mapToLong(LayoutPartition::getSourceRows).sum());
        localLayout.setFileCount(partitions.stream() //
                .mapToLong(LayoutPartition::getFileCount).sum());
        localLayout.setByteSize(partitions.stream() //
                .mapToLong(LayoutPartition::getByteSize).sum());
        return localLayout;
    }

    public Set<Long> getAvailableLayoutIds(NDataflow dataflow, Set<Long> layoutIds) {
        val layoutInCubeIds = dataflow.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toList());
        return layoutIds.stream().filter(layoutInCubeIds::contains).collect(Collectors.toSet());
    }

    public void updateIndexPlan(String dfId, ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        IndexPlan remoteIndexPlan = remoteDataflowManager.getDataflow(dfId).getIndexPlan();
        IndexPlan indexPlan = localDataflowManager.getDataflow(dfId).getIndexPlan();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.setLayoutBucketNumMapping(remoteIndexPlan.getLayoutBucketNumMapping());
        });
    }
}
