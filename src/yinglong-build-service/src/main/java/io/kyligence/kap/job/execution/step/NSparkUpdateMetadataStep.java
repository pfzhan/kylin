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

package io.kyligence.kap.job.execution.step;

import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.engine.spark.cleanup.SnapshotChecker;
import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;

public class NSparkUpdateMetadataStep extends AbstractExecutable {

    public NSparkUpdateMetadataStep() {
        this.setName(ExecutableConstants.STEP_UPDATE_METADATA);
    }

    public NSparkUpdateMetadataStep(Object notSetId) {
        super(notSetId);
    }

    private static final Logger logger = LoggerFactory.getLogger(NSparkUpdateMetadataStep.class);

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {
        val parent = getParent();
        Preconditions.checkArgument(parent instanceof DefaultChainedExecutableOnModel);
        val handler = ((DefaultChainedExecutableOnModel) parent).getHandler();
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                handler.handleFinished();
                return null;
            }, handler.getProject());
            cleanExpiredSnapshot();
            return ExecuteResult.createSucceed();
        } catch (Throwable throwable) {
            logger.warn("");
            return ExecuteResult.createError(throwable);
        }
    }

    private void cleanExpiredSnapshot() {
        try {
            long startDelete = System.currentTimeMillis();
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
            long survivalTimeThreshold = config.getTimeMachineEnabled()
                    ? config.getStorageResourceSurvivalTimeThreshold()
                    : config.getSnapshotVersionTTL();
            String dfId = ExecutableUtils.getDataflowId(this);
            NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(dfId);
            Set<TableRef> tables = dataflow.getModel().getLookupTables();
            for (TableRef table : tables) {
                if (table.getTableDesc().getLastSnapshotPath() == null) {
                    continue;
                }

                Path path = FileNames.snapshotFileWithWorkingDir(project, table.getTableIdentity(), workingDir);
                if (!HDFSUtils.exists(path) && config.isUTEnv()) {
                    continue;
                }
                FileStatus lastFile = HDFSUtils.findLastFile(path);
                HDFSUtils.deleteFilesWithCheck(path, new SnapshotChecker(config.getSnapshotMaxVersions(),
                        survivalTimeThreshold, lastFile.getModificationTime()));
            }
            logger.info("Delete expired snapshot table for dataflow {} cost: {} ms.", dfId,
                    (System.currentTimeMillis() - startDelete));
        } catch (Exception e) {
            logger.error("error happen in cleaning expired snapshot ", e);
        }
    }
}
