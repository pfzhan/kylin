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

import com.google.common.base.Preconditions;
import io.kyligence.kap.tool.util.ToolUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class KylinLogTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    public static final long DAY = 24 * 3600 * 1000L;

    private KylinLogTool() {
    }

    /**
     * extract the specified log from kylin logs dir.
     * @param exportDir
     */
    public static void extractOtherLogs(File exportDir) {
        File destDir = new File(exportDir, "logs");

        String[] includeLogs = { "access_log.", "kylin.gc.", "shell.", "kylin.out", "diag.log" };
        try {
            FileUtils.forceMkdir(destDir);
            File kylinLogDir = new File(ToolUtil.getLogFolder());
            if (kylinLogDir.exists()) {
                File[] logFiles = kylinLogDir.listFiles();
                if (null == logFiles) {
                    logger.error("Failed to list kylin logs dir: {}", kylinLogDir);
                    return;
                }

                for (File logFile : logFiles) {
                    for (String includeLog : includeLogs) {
                        if (logFile.getName().startsWith(includeLog)) {
                            FileUtils.copyFileToDirectory(logFile, destDir);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract the logs from kylin logs dir, ", e);
        }
    }

    /**
     * extract kylin log by jobId
     * for job diagnosis
     * @param exportDir
     * @param jobId
     */
    public static void extractKylinLog(File exportDir, String jobId) {
        extractKylinLog(exportDir, jobId, null, null);
    }

    /**
     * extract kylin log by time range
     * for full diagnosis
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractKylinLog(File exportDir, long startTime, long endTime) {
        extractKylinLog(exportDir, null, startTime, endTime);
    }

    /**
     * extract kylin log
     * @param exportDir
     * @param jobId
     * @param startTime
     * @param endTime
     */
    private static void extractKylinLog(File exportDir, String jobId, Long startTime, Long endTime) {
        File destLogDir = new File(exportDir, "logs");

        try {
            FileUtils.forceMkdir(destLogDir);

            String logBinFile = "bin/log-extract-tool.sh";
            if(KylinConfig.getInstanceFromEnv().isUTEnv()) {
                logBinFile = "../../../build/" + logBinFile;
            }

            File logExtractTool = new File(ToolUtil.getKylinHome() + File.separator + logBinFile);
            if (!logExtractTool.exists()) {
                logger.error("Can not found the log extract tool: {}", logExtractTool.getAbsolutePath());
                return;
            }

            File logsDir = new File(ToolUtil.getKylinHome() + File.separator + "logs");
            if (!logsDir.exists()) {
                logger.error("Can not found the logs dir: {}", logsDir);
                return;
            }

            File[] kylinLogs = logsDir.listFiles(pathname -> pathname.getName().startsWith("kylin.log"));
            if (null == kylinLogs) {
                logger.error("Can not found the kylin.log file!");
                return;
            }

            CliCommandExecutor cmdExecutor = new CliCommandExecutor();
            for (File logFile : kylinLogs) {
                String cmd;
                if (null != jobId) {
                    cmd = String.format("%s %s -job %s", logExtractTool.getAbsolutePath(), logFile.getAbsolutePath(),
                            jobId);
                } else {
                    Preconditions.checkArgument(startTime <= endTime);
                    cmd = String.format("%s %s -startTime %s -endTime %s", logExtractTool.getAbsolutePath(),
                            logFile.getAbsolutePath(), startTime, endTime);
                }

                try {
                    Pair<Integer, String> result = cmdExecutor.execute(cmd, null);
                    if (result.getFirst() != 0) {
                        logger.info("Failed to execute the cmd: {}", cmd);
                    }

                    if (null != result.getSecond()) {
                        FileUtils.write(new File(destLogDir, logFile.getName()), result.getSecond());
                    }
                } catch (ShellException se) {
                    logger.error("Failed to extract kylin log file: {}", logFile.getAbsolutePath(), se);
                    FileUtils.write(new File(destLogDir, logFile.getName()), se.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract kylin.log, ", e);
        }
    }

    /**
     * extract the spark executor log by project and jobId.
     * for job diagnosis
     * @param exportDir
     * @param project
     * @param jobId
     */
    public static void extractSparkLog(File exportDir, String project, String jobId) {
        File sparkLogsDir = new File(exportDir, "spark_logs");

        try {
            FileUtils.forceMkdir(sparkLogsDir);
            String hdfsPath = ToolUtil.getSparkLogsDir(project);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            String jobPath = hdfsPath + "/*/" + jobId;
            FileStatus[] fileStatuses = fs.globStatus(new Path(jobPath));
            if (null == fileStatuses || fileStatuses.length == 0) {
                logger.error("Can not found the spark logs: {}", jobPath);
                return;
            }

            for (FileStatus fileStatus : fileStatuses) {
                fs.copyToLocalFile(fileStatus.getPath(), new Path(sparkLogsDir.getAbsolutePath()));
            }
        } catch (Exception e) {
            logger.error("Failed to extract spark log, ", e);
        }
    }

    /**
     * extract the job tmp by project and jobId.
     * for job diagnosis.
     * @param exportDir
     * @param project
     * @param jobId
     */
    public static void extractJobTmp(File exportDir, String project, String jobId) {
        File jobTmpDir = new File(exportDir, "job_tmp");

        try {
            FileUtils.forceMkdir(jobTmpDir);

            String hdfsPath = ToolUtil.getJobTmpDir(project, jobId);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(new Path(hdfsPath))) {
                logger.error("Can not found the job tmp: {}", hdfsPath);
                return;
            }

            fs.copyToLocalFile(new Path(hdfsPath), new Path(jobTmpDir.getAbsolutePath()));
        } catch (Exception e) {
            logger.error("Failed to extract job_tmp, ", e);
        }
    }

    /**
     * extract the sparder log range by startime and endtime
     * for full diagnosis
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractSparderLog(File exportDir, long startTime, long endTime) {
        File sparkLogsDir = new File(exportDir, "spark_logs");

        try {
            FileUtils.forceMkdir(sparkLogsDir);
            if (endTime < startTime) {
                logger.error("endTime: {} < startTime: {}", endTime, startTime);
                return;
            }

            long days = (endTime - startTime) / DAY;
            if (days > 30) {
                logger.error("time range is too large, startTime: {}, endTime: {}, days: {}", startTime, endTime, days);
                return;
            }

            DateTime date = new DateTime(startTime).withTimeAtStartOfDay();
            while (date.getMillis() <= (endTime + DAY - 1)) {
                String hdfsPath = ToolUtil.getSparderLogsDir() + '/' + date.toString("yyyy-MM-dd");
                FileSystem fs = HadoopUtil.getWorkingFileSystem();
                if (fs.exists(new Path(hdfsPath))) {
                    fs.copyToLocalFile(new Path(hdfsPath), new Path(sparkLogsDir.getAbsolutePath()));
                }

                date = date.plusDays(1);
            }

        } catch (Exception e) {
            logger.error("Failed to extract sparder log, ", e);
        }
    }
}
