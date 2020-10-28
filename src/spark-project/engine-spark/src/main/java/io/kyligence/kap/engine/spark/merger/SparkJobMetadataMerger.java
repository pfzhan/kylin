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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.cleanup.SnapshotChecker;
import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
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

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType) {
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

    protected void updateSnapshotTableIfNeed(NDataSegment segment) {
        try {
            log.info("Check snapshot for segment: {}", segment);
            Map<Path, SnapshotChecker> snapshotCheckerMap = new HashMap<>();
            List<TableDesc> needUpdateTableDescs = new ArrayList<>();
            Map<String, String> snapshots = segment.getSnapshots();
            for (Map.Entry<String, String> entry : snapshots.entrySet()) {
                collectNeedUpdateSnapshotTable(segment, entry, snapshotCheckerMap, needUpdateTableDescs);
            }
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NTableMetadataManager updateManager = NTableMetadataManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), segment.getProject());
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

    private void collectNeedUpdateSnapshotTable(NDataSegment segment, Map.Entry<String, String> snapshot,
            Map<Path, SnapshotChecker> snapshotCheckerMap, List<TableDesc> needUpdateTableDescs) {
        NTableMetadataManager manager = NTableMetadataManager.getInstance(getConfig(), segment.getProject());
        KylinConfig segmentConf = segment.getConfig();
        val timeMachineEnabled = segmentConf.getTimeMachineEnabled();
        long survivalTimeThreshold = timeMachineEnabled ? segmentConf.getStorageResourceSurvivalTimeThreshold()
                : segmentConf.getSnapshotVersionTTL();
        String workingDirectory = KapConfig.wrap(segmentConf).getMetadataWorkingDirectory();
        log.info("Update snapshot table {}", snapshot.getKey());
        TableDesc tableDesc = manager.getTableDesc(snapshot.getKey());
        Path snapshotPath = FileNames.snapshotFileWithWorkingDir(tableDesc, workingDirectory);
        if (HDFSUtils.listSortedFileFrom(snapshotPath).isEmpty()) {
            throw new RuntimeException("Snapshot path is empty :" + snapshotPath);
        }
        FileStatus lastFile = HDFSUtils.findLastFile(snapshotPath);
        HDFSUtils.listSortedFileFrom(snapshotPath);
        Path segmentFilePath = new Path(workingDirectory + snapshot.getValue());
        FileStatus segmentFile = HDFSUtils.getFileStatus(segmentFilePath);
        FileStatus currentFile = null;
        if (tableDesc.getLastSnapshotPath() != null) {
            currentFile = HDFSUtils.getFileStatus(new Path(workingDirectory + tableDesc.getLastSnapshotPath()));
        }
        val currentModificationTime = currentFile == null ? 0L : currentFile.getModificationTime();
        val currentPath = currentFile == null ? "null" : currentFile.getPath().toString();
        TableDesc copyDesc = manager.copyForWrite(tableDesc);
        if (segmentFile != null && lastFile.getModificationTime() <= segmentFile.getModificationTime()) {
            log.info("Update snapshot table {} : from {} to {}", snapshot.getKey(), currentModificationTime,
                    lastFile.getModificationTime());
            log.info("Update snapshot table {} : from {} to {}", snapshot.getKey(), currentPath, segmentFile.getPath());
            copyDesc.setLastSnapshotPath(snapshot.getValue());
            needUpdateTableDescs.add(copyDesc);
            snapshotCheckerMap.put(snapshotPath, new SnapshotChecker(segmentConf.getSnapshotMaxVersions(),
                    survivalTimeThreshold, segmentFile.getModificationTime()));
        } else if (copyDesc.getLastSnapshotPath() == null
                && System.currentTimeMillis() - segment.getLastBuildTime() < survivalTimeThreshold) {
            /**
             * when tableDesc's lastSnapshot is null and snapshot within survival time, force update snapshot table.
             * see https://olapio.atlassian.net/browse/KE-17343
             */
            log.info("update snapshot table {} when tableDesc's lastSnapshot is null and snapshot within survival time",
                    snapshot.getKey());
            copyDesc.setLastSnapshotPath(snapshot.getValue());
            needUpdateTableDescs.add(copyDesc);
        } else {
            if (segmentFile != null) {
                log.info(
                        "Skip update snapshot table because current segment snapshot table is too old. Current segment snapshot table ts is: {}",
                        segmentFile.getModificationTime());
            } else {
                log.info("{}'s FileStatus is null, Skip update.", segmentFilePath);
            }
        }
    }
}
