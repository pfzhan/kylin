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

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 */
@NoArgsConstructor
public class NSparkSnapshotBuildingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkSnapshotBuildingStep.class);

    public NSparkSnapshotBuildingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SNAPSHOT);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(config, getProject());
        final TableDesc tableDesc = tblMgr.getTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        final TableExtDesc tableExtDesc = tblMgr.getTableExtIfExists(tableDesc);
        if (tableExtDesc != null) {
            dumpList.add(tableExtDesc.getResourcePath());
        }
        dumpList.add(tableDesc.getResourcePath());
        dumpList.add(projectInstance.getResourcePath());

        return dumpList;
    }

    public static class Mockup {
        public static void main(String[] args) {
            String msg = String.format(Locale.ROOT, "%s.main() invoked, args: %s", Mockup.class, Arrays.toString(args));
            logger.info(msg);
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        ExecuteResult result = super.doWork(context);
        if (!result.succeed()) {
            return result;
        }
        wrapWithCheckQuit(this::mergeRemoteMetaAfterBuilding);
        return result;
    }

    private void mergeRemoteMetaAfterBuilding() {

        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String selectPartCol = getParam(NBatchConstants.P_SELECTED_PARTITION_COL);
        boolean incrementBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));

        try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), this)) {

            val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), getProject());
            val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            val localTbDesc = localTblMgr.getTableDesc(tableName);
            val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
            val copy = localTblMgr.copyForWrite(localTbDesc);
            val copyExt = localTblMgr.copyForWrite(localTblMgr.getOrCreateTableExt(localTbDesc));
            TableExtDesc remoteTblExtDesc = remoteTblMgr.getOrCreateTableExt(remoteTbDesc);
            copyExt.setOriginalSize(remoteTblExtDesc.getOriginalSize());

            if (selectPartCol == null) {
                copy.setLastSnapshotPath(remoteTbDesc.getLastSnapshotPath());
                copyExt.setOriginalSize(remoteTblMgr.getOrCreateTableExt(remoteTbDesc).getOriginalSize());
                copy.setSnapshotPartitionCol(null);
                copy.resetSnapshotPartitions(Sets.newHashSet());
            } else {
                if (!incrementBuild) {
                    copy.setLastSnapshotPath(remoteTbDesc.getTempSnapshotPath());
                }
                // ?
                copyExt.setOriginalSize(remoteTbDesc.getSnapshotPartitions().values().stream().mapToLong(i -> i).sum());
                copy.setSnapshotPartitionCol(selectPartCol);
                copy.setSnapshotPartitions(remoteTbDesc.getSnapshotPartitions());
            }

            copyExt.setTotalRows(remoteTblExtDesc.getTotalRows());
            copy.setLastSnapshotSize(remoteTbDesc.getLastSnapshotSize());
            localTblMgr.saveTableExt(copyExt);
            localTblMgr.updateTableDesc(copy);
        }
    }
}
