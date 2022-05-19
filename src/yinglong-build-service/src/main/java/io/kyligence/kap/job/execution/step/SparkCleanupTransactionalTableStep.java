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

import static io.kyligence.kap.engine.spark.utils.HiveTransactionTableHelper.generateDropTableStatement;
import static io.kyligence.kap.engine.spark.utils.HiveTransactionTableHelper.generateHiveInitStatements;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.scheduler.JobContext;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;

public class SparkCleanupTransactionalTableStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SparkCleanupTransactionalTableStep.class);

    public SparkCleanupTransactionalTableStep() {
        this.setName(ExecutableConstants.STEP_INTERMEDIATE_TABLE_CLEANUP);
    }

    public SparkCleanupTransactionalTableStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String jobId = getParam(NBatchConstants.P_JOB_ID);
        String dir = config.getJobTmpTransactionalTableDir(getProject(), jobId);
        logger.info("should clean dir :{} ", dir);
        Path path = new Path(dir);
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                for (FileStatus fileStatus : fs.listStatus(path)) {
                    final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(config);
                    String tableFullName = fileStatus.getPath().getName();
                    hiveCmdBuilder.addStatement(generateDropTableCommand(tableFullName));
                    final String cmd = hiveCmdBuilder.toString();
                    doExecuteCliCommand(tableFullName, cmd);
                }
                fs.delete(path, true);
            }
        } catch (IOException e) {
            logger.error("Can not delete intermediate table.", e);
            throw new ExecuteException("Can not delete intermediate table");
        }

        return ExecuteResult.createSucceed();
    }

    private void doExecuteCliCommand(String tableFullName, String cmd) {
        if(StringUtils.isEmpty(cmd)) {
            return;
        }
        try {
            CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult result = cliCommandExecutor.execute(cmd, null);
            if (result.getCode() != 0) {
                logger.error("execute drop intermediate table return fail, table : {}, code :{}",
                        tableFullName, result.getCode());
            } else {
                logger.info("execute drop intermediate table succeeded, table :{} ", tableFullName);
            }
        } catch (ShellException e) {
            logger.error(String.format(Locale.ROOT, "execute drop intermediate table error, table :%s ", tableFullName), e);
        }
    }

    public static String generateDropTableCommand(String tableFullName) {
        // By default, the tableFullName information obtained is the full path of the table，
        // eg: TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544
        if(StringUtils.isEmpty(tableFullName)) {
            logger.info("The table name is empty.");
            return "";
        }

        String tableName = tableFullName;
        if(tableFullName.contains(".") && !tableFullName.endsWith(".")) {
            String database = tableFullName.substring(0, tableFullName.indexOf("."));
            tableName = tableFullName.substring(tableFullName.indexOf(".") + 1);
            return generateHiveInitStatements(database) + generateDropTableStatement(tableName);
        }
        return generateDropTableStatement(tableName);
    }

}
