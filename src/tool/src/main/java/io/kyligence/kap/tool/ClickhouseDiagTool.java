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
package io.kyligence.kap.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public class ClickhouseDiagTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    //{ck_node_name}_{ip}_{port}
    private static final String CK_NODE_PATH_FORMAT = "%s_%s_%s";

    private static final String SECOND_DATE_FORMAT = "yyyy.MM.dd HH:mm:ss.SSSSSS";

    private static final KylinLogTool.ExtractLogByRangeTool LOG_EXTRACT =
            new KylinLogTool.ExtractLogByRangeTool(
                    "^([0-9]{4}\\.[0-9]{2}\\.[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6})",
                    SECOND_DATE_FORMAT);

    public static final String SUB_DIR = "tieredStorage";

    private String project;

    public ClickhouseDiagTool(String project) {
        this.project = project;
    }

    private boolean isFullDiag() {
        return project == null;
    }

    public void dumpClickHouseServerLog(File exportDir, long startTime, long endTime) {

        try {
            SecondStorage.init(false);

            if (!SecondStorage.enabled()) {
                logger.error("TieredStorage is not enabled. Skip to fetch diag log.");
            }

            File ckLogsDir = new File(exportDir, SUB_DIR);
            FileUtils.forceMkdir(ckLogsDir);
            extractCkLogFile(ckLogsDir, startTime, endTime);
        } catch (Exception e) {
            logger.error("TieredStorage is enabled. But extract error,", e);
        }

    }

    private void extractCkLogFile(File exportDir, long startTime, long endTime) {
        val cluster = SecondStorage.configLoader().getCluster();

        if (cluster.emptyCluster()) {
            logger.error("TieredStorage cluster is empty. Skip to fetch diag log.");
            return;
        }

        if (StringUtils.isEmpty(cluster.getLogPath())) {
            logger.error("TieredStorage log path is empty. Skip to fetch diag log.");
            return;
        }

        val kylinConfig = KylinConfig.getInstanceFromEnv();

        String diagLogMatcher = kylinConfig.getSecondStorageDiagLogMatcher();
        File remoteFilePath = new File(cluster.getLogPath(), diagLogMatcher);

        int maxCompressedFile = kylinConfig.getSecondStorageDiagMaxCompressedFile();
        File remoteHistoryGZPath = maxCompressedFile > 0
                ? new File(cluster.getLogPath(), getCompressedFileMatcher(diagLogMatcher, maxCompressedFile)) : null;


        val timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                new DateTime(endTime).toString(SECOND_DATE_FORMAT));

        val allNodes = isFullDiag() ? cluster.getNodes() : SecondStorageNodeHelper.getALlNodesInProject(project);

        if (CollectionUtils.isEmpty(allNodes)) {
            logger.warn("There is no active node in TieredStorage");
        }

        allNodes.forEach(node -> {
            val cliCommandExecutor = new CliCommandExecutor(node.getIp(), cluster.getUserName(), cluster.getPassword(), kylinConfig.getSecondStorageSshIdentityPath());
            val nodeTargetPath = new File(exportDir, String.format(Locale.ROOT, CK_NODE_PATH_FORMAT, node.getName(), node.getIp(), node.getPort()));
            val nodeTargetTmpPath = new File(exportDir, nodeTargetPath.getName() + "_tmp");

            try {
                FileUtils.forceMkdir(nodeTargetPath);
                FileUtils.forceMkdir(nodeTargetTmpPath);
                cliCommandExecutor.copyRemoteToLocal(remoteFilePath.getAbsolutePath(), nodeTargetTmpPath.getAbsolutePath());

                copyAndUnzipCompressedLogFile(remoteHistoryGZPath, cliCommandExecutor, nodeTargetTmpPath);

                if (!extractCkLogByRange(timeRange, nodeTargetPath, nodeTargetTmpPath)) {
                    return;
                }

                cleanEmptyFile(nodeTargetPath);

            } catch (IOException e) {
                logger.error("gather clickhouse log failed,{},", nodeTargetTmpPath.getAbsolutePath(), e);
            } finally {
                FileUtils.deleteQuietly(nodeTargetTmpPath);
            }

        });
    }

    private void copyAndUnzipCompressedLogFile(File remoteHistoryGZPath, CliCommandExecutor cliCommandExecutor, File nodeTargetTmpPath) {
        if (remoteHistoryGZPath == null) {
            return;
        }

        try {
            cliCommandExecutor.copyRemoteToLocal(remoteHistoryGZPath.getAbsolutePath(), nodeTargetTmpPath.getAbsolutePath());
            unzipLogFile(nodeTargetTmpPath);
        } catch (IOException e) {
            logger.error("copy remote compressed file failed,{},", nodeTargetTmpPath.getAbsolutePath(), e);
        }

    }

    private boolean extractCkLogByRange(Pair<String, String> timeRange, File nodeTargetPath, File nodeTargetTmpPath) {
        val serverLogFiles = nodeTargetTmpPath.listFiles((dir, name) -> name.endsWith(".log"));

        if (ArrayUtils.isEmpty(serverLogFiles)) {
            logger.error("{} dir is empty", nodeTargetTmpPath.getAbsolutePath());
            return false;
        }

        Arrays.stream(serverLogFiles).forEach(file -> {
            try {
                LOG_EXTRACT.extractLogByRange(file, timeRange, nodeTargetPath);
            } catch (IOException e) {
                logger.error("extract file:{} log error", file, e);
            }
        });
        return true;
    }

    private void cleanEmptyFile(File filePath) {
        val fileList = filePath.listFiles();

        if (ArrayUtils.isEmpty(fileList)) {
            return;
        }

        Arrays.stream(fileList).filter(file -> FileUtils.sizeOf(file) == 0).forEach(file -> {

            if (FileUtils.deleteQuietly(file)) {
                logger.debug("{} size is 0, clean it success", file.getName());
                return;
            }
            logger.debug("{} size is 0, clean it failed", file.getName());
        });

    }

    private void unzipLogFile(File nodeTargetPath) {
        val gzFileList = nodeTargetPath.listFiles((dir, name) -> name.endsWith(".gz"));

        if (ArrayUtils.isEmpty(gzFileList)) {
            logger.warn("{} dont have gz file, skit unzip it", nodeTargetPath.getName());
            return;
        }

        Arrays.stream(gzFileList).filter(file -> FileUtils.sizeOf(file) > 0).forEach(file -> {
            try {

                GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
                String fileName = file.getName().substring(0, file.getName().lastIndexOf("."));
                FileOutputStream fileOutputStream = new FileOutputStream(new File(nodeTargetPath, fileName + ".log"));

                byte[] buffer = new byte[1024];
                int len;
                while ((len = gzipInputStream.read(buffer)) > 0) {
                    fileOutputStream.write(buffer, 0, len);
                }

                gzipInputStream.close();
                fileOutputStream.close();

                logger.info("Extracted " + fileName);
            } catch (IOException e) {
                logger.error("extract gz file:{} log error", file, e);
            }
        });

    }


    private static String getCompressedFileMatcher(String filePrefix, int maxCompressedFile) {
        Preconditions.checkState(maxCompressedFile > 0, "max file count should > 0," + maxCompressedFile);

        // /var/log/clickhouse-server/{*-server.log.0.gz,*-server.log.1.gz}
        return IntStream.range(0, maxCompressedFile).mapToObj(index -> filePrefix + "." + index + ".gz")
                .collect(Collectors.joining(",", "{", "}"));
    }

}
