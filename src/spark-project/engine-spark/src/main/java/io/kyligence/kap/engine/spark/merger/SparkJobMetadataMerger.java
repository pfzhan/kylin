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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.Getter;

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

    protected void updateSnapshotTableIfNeed(NDataflow dataflow, ResourceStore configStore) {
        if (isSnapshotManualManagementEnabled(configStore)) {
            return;
        }
        long start = System.currentTimeMillis();
        log.info("Check snapshot for dataflow: {}", dataflow);
        try {
            KylinConfig config = getConfig();
            String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
            NTableMetadataManager manager = NTableMetadataManager.getInstance(config, dataflow.getProject());
            Set<String> tables = dataflow.getModel().getLookupTables().stream().map(TableRef::getTableIdentity)
                    .collect(Collectors.toSet());
            List<TableDesc> needUpdateTables = new ArrayList<>();
            for (String table : tables) {
                TableDesc tableDesc = manager.getTableDesc(table);
                Path path = FileNames.snapshotFileWithWorkingDir(tableDesc, workingDir);
                if (!HDFSUtils.exists(path) && config.isUTEnv()) {
                    continue;
                }
                FileStatus lastFile = HDFSUtils.findLastFile(path);
                TableDesc copyDesc = manager.copyForWrite(tableDesc);
                copyDesc.setLastSnapshotPath(FileNames.snapshotFile(tableDesc) + "/" + lastFile.getPath().getName());
                needUpdateTables.add(copyDesc);
            }
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NTableMetadataManager updateManager = NTableMetadataManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), dataflow.getProject());
                for (TableDesc tableDesc : needUpdateTables) {
                    updateManager.updateTableDesc(tableDesc);
                }
                return null;
            }, dataflow.getProject(), 1);
        } catch (Throwable th) {
            log.error("Error for update snapshot table", th);
        }
        log.info("Update snapshot table for dataflow {} cost: {} ms.", dataflow.getUuid(),
                (System.currentTimeMillis() - start));
    }

    private boolean isSnapshotManualManagementEnabled(ResourceStore configStore) {
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
}
