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

import static io.kyligence.kap.engine.spark.source.NSparkCubingSourceInput.generateDropTableStatement;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;

public class NSparkCleanupTransactionalTableStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCleanupTransactionalTableStep.class);

    public NSparkCleanupTransactionalTableStep() {
        this.setName(ExecutableConstants.STEP_INTERMEDIATE_TABLE_CLEANUP);
    }

    public NSparkCleanupTransactionalTableStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String jobId = getParam(NBatchConstants.P_JOB_ID);
        String dir = config.getJobTmpTransactionalTableDir(getProject(), jobId);
        logger.info("should clean dir : " + dir);
        Path path = new Path(dir);
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                for (FileStatus fileStatus : fs.listStatus(path)) {
                    final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(config);
                    String tableName = fileStatus.getPath().getName();
                    hiveCmdBuilder.addStatement(generateDropTableStatement(tableName));
                    final String cmd = hiveCmdBuilder.toString();
                    CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
                    try {
                        CliCommandExecutor.CliCmdExecResult result = cliCommandExecutor.execute(cmd, null);
                        if (result.getCode() != 0) {
                            logger.error("execute drop intermediate table return fail, table : " + tableName
                                    + ", code : " + result.getCode());
                        } else {
                            logger.info("execute drop intermediate table succeeded, table : " + tableName);
                        }
                    } catch (ShellException e) {
                        logger.error("execute drop intermediate table error, table : " + tableName, e);
                    }
                }
                fs.delete(path, true);
            }
        } catch (IOException e) {
            throw new ExecuteException("Can not delete intermediate table");
        }

        return ExecuteResult.createSucceed();
    }

}
