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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.kyligence.kap.cluster.ClusterManagerFactory;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static io.kyligence.kap.engine.spark.job.NSparkExecutable.SPARK_MASTER;

public class DefaultSparkBuildJobHandler implements ISparkJobHandler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSparkBuildJobHandler.class);

    private final String SLASH = "/";
    private static final String SPACE = " ";
    private static final String SUBMIT_LINE_FORMAT = " \\\n";

    @Override
    public void killOrphanApplicationIfExists(String jobStepId, KylinConfig config, Map<String, String> sparkConf) {
        try {
            val sparkMaster = sparkConf.getOrDefault(SPARK_MASTER, "local");
            if (sparkMaster.startsWith("local")) {
                logger.info("Skip kill orphan app for spark.master={}", sparkMaster);
                return;
            }
            final IClusterManager cm = ClusterManagerFactory.create(config);
            cm.killApplication(jobStepId);
        } catch (UncheckedTimeoutException e) {
            logger.warn("Kill orphan app timeout {}", e.getMessage());
        }
    }

    @Override
    public void checkApplicationJar(KylinConfig config, String path) throws ExecuteException {
        if (config.isUTEnv()) {
            return;
        }
        // Application-jar:
        // Path to a bundled jar including your application and all dependencies.
        // The URL must be globally visible inside of your cluster,
        // for instance, an hdfs:// path or a file:// path that is present on all nodes.
        try {
            final String failedMsg = "Application jar should be only one bundled jar.";
            URI uri = new URI(path);
            if (Objects.isNull(uri.getScheme()) || uri.getScheme().startsWith("file:/")) {
                Preconditions.checkState(new File(path).exists(), failedMsg);
                return;
            }

            Path path0 = new Path(path);
            FileSystem fs = HadoopUtil.getFileSystem(path0);
            Preconditions.checkState(fs.exists(path0), failedMsg);
        } catch (URISyntaxException | IOException e) {
            throw new ExecuteException("Failed to check application jar.", e);
        }
    }

    @Override
    public String createArgsFileOnRemoteFileSystem(KylinConfig config, String project, String jobId, Map<String, String> params)
            throws ExecuteException {
        val fs = HadoopUtil.getWorkingFileSystem();
        Path path = fs.makeQualified(new Path(config.getJobTmpArgsDir(project, jobId)));
        try (FSDataOutputStream out = fs.create(path)) {
            out.write(JsonUtil.writeValueAsBytes(params));
        } catch (IOException e) {
            try {
                fs.delete(path, true);
            } catch (IOException e1) {
                throw new ExecuteException("Write spark args failed! Error for delete file: " + path.toString(), e1);
            }
            throw new ExecuteException("Write spark args failed: ", e);
        }
        return path.toString();
    }

    @Override
    public String runSparkSubmit(Object cmd, BufferedLogger patternedLogger, String parentId, String project, ExecutableState status)
            throws ExecuteException {
        try {
            CliCommandExecutor exec = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult r = exec.execute((String) cmd, patternedLogger, parentId);
            if (StringUtils.isNotEmpty(r.getProcessId())) {
                try {
                    Map<String, String> updateInfo = Maps.newHashMap();
                    updateInfo.put("process_id", r.getProcessId());
                    EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                                .updateJobOutput(parentId, status, updateInfo, null, null);
                        return null;
                    }, project);
                } catch (Exception e) {
                    logger.warn("failed to record process id.");
                }
            }
            return r.getCmd();
        } catch (Exception e) {
            logger.warn("failed to execute spark submit command.");
            throw new ExecuteException(e);
        }
    }
}
