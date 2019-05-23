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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.SegmentUtils;
import io.kyligence.kap.engine.spark.cleanup.SnapshotChecker;
import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;

@Slf4j
public class AfterBuildResourceMerger extends SparkJobMetadataMerger {

    private JobTypeEnum jobTypeEnum;

    public AfterBuildResourceMerger(KylinConfig config, String project, JobTypeEnum jobTypeEnum) {
        super(config, project);
        this.jobTypeEnum = jobTypeEnum;
    }

    @Override
    public NDataLayout[] merge(String flowName, Set<String> segmentId, Set<Long> layoutIds, ResourceStore remoteStore) {
        switch (jobTypeEnum) {
            case INDEX_BUILD:
                return mergeAfterCatchup(flowName, segmentId, layoutIds, remoteStore);
            case INC_BUILD:
                Preconditions.checkArgument(segmentId.size() == 1);
                return mergeAfterIncrement(flowName, segmentId.iterator().next(), layoutIds, remoteStore);
            default:
                throw new UnsupportedOperationException("Error job type: " + jobTypeEnum);
        }
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), abstractExecutable)) {
            val dataFlowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val segmentIds = ExecutableUtils.getSegmentIds(abstractExecutable);
            val layoutIds = ExecutableUtils.getLayoutIds(abstractExecutable);
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore);
            recordDownJobStats(abstractExecutable, nDataLayouts);
            abstractExecutable.notifyUserIfNecessary(nDataLayouts);
        }
    }

    public NDataLayout[] mergeAfterIncrement(String flowName, String segmentId, Set<Long> layoutIds,
                                             ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dfUpdate = new NDataflowUpdate(flowName);
        val availableLayoutIds = intersectionWithLastSegment(localDataflow, layoutIds);
        val theSeg = remoteDataflow.getSegment(segmentId);
        updateSnapshotTableIfNeed(theSeg);
        theSeg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(theSeg);
        dfUpdate.setToAddOrUpdateLayouts(theSeg.getSegDetails().getLayouts().stream()
                .filter(c -> availableLayoutIds.contains(c.getLayoutId())).toArray(NDataLayout[]::new));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateLayouts();
    }

    private void updateSnapshotTableIfNeed(NDataSegment segment) {
        try {
            Map<Path, SnapshotChecker> snapshotCheckerMap = new HashMap<>();
            List<TableDesc> needUpdateTableDescs = new ArrayList<>();
            Map<String, String> snapshots = segment.getSnapshots();
            NTableMetadataManager manager = NTableMetadataManager.getInstance(getConfig(), segment.getProject());
            KylinConfig segmentConf = segment.getConfig();
            String workingDirectory = KapConfig.wrap(segmentConf).getReadHdfsWorkingDirectory();
            for (Map.Entry<String, String> entry : snapshots.entrySet()) {
                TableDesc tableDesc = manager.getTableDesc(entry.getKey());
                Path snapshotPath = FileNames.snapshotFileWithWorkingDir(tableDesc, workingDirectory);
                FileStatus lastFile = HDFSUtils.findLastFile(snapshotPath);
                FileStatus segmentFile = HDFSUtils.getFileStatus(new Path(workingDirectory + entry.getValue()));

                FileStatus currentFile = null;
                if (tableDesc.getLastSnapshotPath() != null) {
                    currentFile = HDFSUtils.getFileStatus(new Path(workingDirectory + tableDesc.getLastSnapshotPath()));
                }

                if (lastFile.getModificationTime() <= segmentFile.getModificationTime()) {

                    log.info("Update snapshot table " + entry.getKey() + " : " + "from " + (currentFile == null ? 0L : currentFile.getModificationTime()) + " to " + lastFile.getModificationTime())
                    ;
                    log.info("Update snapshot table " + entry.getKey() + " : " + "from " + (currentFile == null ? "null" : currentFile.getPath().toString()) + " to " + segmentFile.getPath().toString());
                    TableDesc copyDesc = manager.copyForWrite(tableDesc);
                    copyDesc.setLastSnapshotPath(entry.getValue());
                    needUpdateTableDescs.add(copyDesc);
                    snapshotCheckerMap.put(snapshotPath,
                            new SnapshotChecker(segmentConf.getSnapshotMaxVersions(), segmentConf.getSnapshotVersionTTL(), segmentFile.getModificationTime()));
                } else {
                    log.info("Skip update snapshot table because current segment snapshot table is to old. Current segment snapshot table ts is : " + segmentFile.getModificationTime());
                }
            }
            UnitOfWork.doInTransactionWithRetry(() -> {
                NTableMetadataManager updateManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), segment.getProject());
                for (TableDesc tableDesc : needUpdateTableDescs) {
                    updateManager.updateTableDesc(tableDesc);
                }
                return null;
            }, segment.getProject(), 1);
            for (Map.Entry<Path, SnapshotChecker> entry : snapshotCheckerMap.entrySet()) {
                HDFSUtils.deleteFilesWithCheck(entry.getKey(), entry.getValue());
            }
        } catch (Throwable th) {
            log.error("Error for update snapshot table", th);
        }
    }

    public NDataLayout[] mergeAfterCatchup(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
                                           ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dataflow = localDataflowManager.getDataflow(flowName);
        val dfUpdate = new NDataflowUpdate(flowName);
        val addCuboids = Lists.<NDataLayout>newArrayList();

        val layoutInCubeIds = dataflow.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toList());
        val availableLayoutIds = layoutIds.stream().filter(layoutInCubeIds::contains).collect(Collectors.toSet());
        for (String segId : segmentIds) {
            val localSeg = localDataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId);
            // ignore if local segment is not ready
            if (localSeg == null || localSeg.getStatus() != SegmentStatusEnum.READY) {
                continue;
            }
            for (long layoutId : availableLayoutIds) {
                NDataLayout dataCuboid = remoteSeg.getLayout(layoutId);
                Preconditions.checkNotNull(dataCuboid);
                addCuboids.add(dataCuboid);
            }
            dfUpdate.setToUpdateSegs(remoteSeg);
        }
        dfUpdate.setToAddOrUpdateLayouts(addCuboids.toArray(new NDataLayout[0]));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateLayouts();
    }


    private Set<Long> intersectionWithLastSegment(NDataflow dataflow, Collection<Long> layoutIds) {
        val layoutInSegmentIds = SegmentUtils.getToBuildLayouts(dataflow).stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        return layoutIds.stream().filter(layoutInSegmentIds::contains).collect(Collectors.toSet());
    }

}
                                            