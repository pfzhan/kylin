/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service.merger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.cube.model.SegmentPartition;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.val;

public abstract class MetadataMerger {
    private static final Logger log = LoggerFactory.getLogger(MetadataMerger.class);
    @Getter
    private final String project;
    @Getter
    private final KylinConfig config;

    protected MetadataMerger(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    public KylinConfig getProjectConfig(ResourceStore remoteStore) throws IOException {
        val globalConfig = KylinConfig.createKylinConfig(
                KylinConfig.streamToProps(remoteStore.getResource("/kylin.properties").getByteSource().openStream()));
        val projectConfig = JsonUtil
                .readValue(remoteStore.getResource("/_global/project/" + project + ".json").getByteSource().read(),
                        ProjectInstance.class)
                .getLegalOverrideKylinProps();
        return KylinConfigExt.createInstance(globalConfig, projectConfig);
    }

    @TestOnly
    public void merge(AbstractExecutable abstractExecutable) {
        MergerInfo.TaskMergeInfo taskMergeInfo = new MergerInfo.TaskMergeInfo(abstractExecutable,
                ExecutableUtils.needBuildSnapshots(abstractExecutable));
        merge(taskMergeInfo);
    }

    public abstract <T> T merge(MergerInfo.TaskMergeInfo info);

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
                copy.setLastSnapshotSize(remoteTbDesc.getLastSnapshotSize());
                copy.setSnapshotLastModified(remoteTbDesc.getSnapshotLastModified());
                copy.setSnapshotTotalRows(remoteTbDesc.getSnapshotTotalRows());
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
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        IndexPlan remoteIndexPlan = remoteDataflowManager.getDataflow(dfId).getIndexPlan();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        indexPlanManager.updateIndexPlan(dfId, copyForWrite -> {
            copyForWrite.setLayoutBucketNumMapping(remoteIndexPlan.getLayoutBucketNumMapping());
        });
    }

    public static MetadataMerger createMetadataMerger(String project, ExecutableHandler.HandlerType type) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        switch (type) {
        case MERGE_OR_REFRESH:
            return new AfterMergeOrRefreshResourceMerger(config, project);
        case ADD_CUBOID:
        case ADD_SEGMENT:
            return new AfterBuildResourceMerger(config, project);
        case SAMPLING:
            return new AfterSamplingMerger(config, project);
        case SNAPSHOT:
            return new AfterSnapshotMerger(config, project);
        default:
            throw new IllegalArgumentException("Unknown HandlerType: " + type);
        }
    }
}