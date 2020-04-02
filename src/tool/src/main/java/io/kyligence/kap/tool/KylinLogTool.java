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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.tool.util.ToolUtil;

public class KylinLogTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    public static final long DAY = 24 * 3600 * 1000L;

    public static final String SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String LOG_TIME_PATTERN = "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})";

    // 2019-11-11 09:30:04,628 INFO  [FetchJobWorker(project:test_fact)-p-94-t-94] threadpool.NDefaultScheduler : start check project test_fact
    private static final String LOG_PATTERN = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) ([^ ]*)[ ]+\\[(.*)\\] ([^: ]*) :([\\n\\r. ]*)";

    private static final int EXTRA_LINES = 100;

    private static final String ROLL_LOG_DIR_NAME_PREFIX = "eventlog_v2_";

    private static final String ROLL_LOG_FILE_NAME_PREFIX = "events";

    // 2019-11-11 03:24:52,342 DEBUG [JobWorker(prj:doc_smart,jobid:8a13964c)-965] job.NSparkExecutable : Copied metadata to the target metaUrl, delete the temp dir: /tmp/kylin_job_meta204633716010108932
    private static String getJobLogPattern(String jobId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(jobId));
        return String.format("%s(.*JobWorker.*jobid:%s.*)", LOG_TIME_PATTERN, jobId.substring(0, 8));
    }

    // 2019-11-11 09:30:04,004 INFO  [Query 4e3350d5-1cd9-450f-ac7e-5859939bedf1-125] service.QueryService : The original query: select * from ssb.SUPPLIER
    private static String getQueryLogPattern(String queryId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(queryId));
        return String.format("%s(.*Query %s.*)", LOG_TIME_PATTERN, queryId);
    }

    private KylinLogTool() {
    }

    /**
     * extract the specified log from kylin logs dir.
     * @param exportDir
     */

    public static void extractOtherLogs(File exportDir, long start, long end) {

        File destDir = new File(exportDir, "logs");
        SimpleDateFormat logFormat = new SimpleDateFormat("yyyy-MM-dd");
        String startDate = logFormat.format(new Date(start));
        String endDate = logFormat.format(new Date(end));
        logger.debug("logs startDate : {}, endDate : {}", startDate, endDate);
        String[] allIncludeLogs = { "kylin.gc.", "shell.", "kylin.out", "diag.log" };
        String[] partIncludeLogByDay = { "access_log." };
        String[] partIncludeLogByMs = { "jstack.timed.log" };
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
                    for (String includeLog : allIncludeLogs) {
                        if (logFile.getName().startsWith(includeLog)) {
                            FileUtils.copyFileToDirectory(logFile, destDir);
                        }
                    }
                    for (String includeLog : partIncludeLogByDay) {
                        if (logFile.getName().startsWith(includeLog)) {
                            String date = logFile.getName().split("\\.")[1];
                            if (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0) {
                                FileUtils.copyFileToDirectory(logFile, destDir);
                            }
                        }
                    }
                    for (String includeLog : partIncludeLogByMs) {
                        if (logFile.getName().startsWith(includeLog)) {
                            long time = Long.parseLong(logFile.getName().split("\\.")[3]);
                            if (time >= start && time <= end) {
                                FileUtils.copyFileToDirectory(logFile, destDir);
                            }
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

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File logFile) {
        return getTimeRangeFromLogFileByJobId(jobId, logFile, false);
    }

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File logFile,
            boolean onlyStartTime) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(logFile);

        String dateStart = null;
        String dateEnd = null;
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            Pattern pattern = Pattern.compile(getJobLogPattern(jobId));

            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    if (Objects.isNull(dateStart)) {
                        dateStart = matcher.group(1);
                        if (onlyStartTime) {
                            return new Pair<>(dateStart, dateStart);
                        }
                    }
                    dateEnd = matcher.group(1);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get time range from logFile:{}, jobId:{}, onlyStartTime: {}",
                    logFile.getAbsolutePath(), jobId, onlyStartTime, e);
        }
        return new Pair<>(dateStart, dateEnd);
    }

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File[] kylinLogs) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(kylinLogs);

        Pair<String, String> timeRangeResult = new Pair<>();
        if (0 == kylinLogs.length) {
            return timeRangeResult;
        }

        List<File> logFiles = Stream.of(kylinLogs).sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());

        int i = 0;
        while (i < logFiles.size()) {
            Pair<String, String> timeRange = getTimeRangeFromLogFileByJobId(jobId, logFiles.get(i++));
            if (null != timeRange.getFirst() && null != timeRange.getSecond()) {
                timeRangeResult.setFirst(timeRange.getFirst());
                timeRangeResult.setSecond(timeRange.getSecond());
                break;
            }
        }

        while (i < logFiles.size()) {
            String dateStart = getTimeRangeFromLogFileByJobId(jobId, logFiles.get(i++), true).getFirst();
            if (null == dateStart) {
                break;
            }
            if (dateStart.compareTo(timeRangeResult.getFirst()) < 0) {
                timeRangeResult.setFirst(dateStart);
            }
        }

        return timeRangeResult;
    }

    public static String getFirstTimeByLogFile(File logFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            Pattern pattern = Pattern.compile(LOG_TIME_PATTERN);
            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get first time by log file: {}", logFile.getAbsolutePath(), e);
        }
        return null;
    }

    public static void extractLogByTimeRange(File logFile, Pair<String, String> timeRange, File distFile) {
        Preconditions.checkNotNull(logFile);
        Preconditions.checkNotNull(timeRange);
        Preconditions.checkNotNull(distFile);
        Preconditions.checkArgument(timeRange.getFirst().compareTo(timeRange.getSecond()) <= 0);

        try (BufferedReader br = new BufferedReader(new FileReader(logFile));
                BufferedWriter bw = new BufferedWriter(new FileWriter(distFile))) {

            int extraLines = EXTRA_LINES;
            boolean extract = false;
            boolean stdLogNotFound = true;
            Pattern pattern = Pattern.compile(LOG_PATTERN);
            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);

                if (matcher.find()) {
                    stdLogNotFound = false;
                    String logDate = matcher.group(1);
                    if (logDate.compareTo(timeRange.getSecond()) > 0 && --extraLines < 1) {
                        break;
                    }

                    if (extract || logDate.compareTo(timeRange.getFirst()) >= 0) {
                        extract = true;
                        bw.write(log);
                        bw.write('\n');
                    }
                } else if (extract || stdLogNotFound) {
                    bw.write(log);
                    bw.write('\n');
                }
            }
        } catch (IOException e) {
            logger.error("Failed to extract log from {} to {}", logFile.getAbsolutePath(), distFile.getAbsolutePath(),
                    e);
        }
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

            File logsDir = new File(ToolUtil.getKylinHome() + File.separator + "logs");
            if (!logsDir.exists()) {
                logger.error("Can not find the logs dir: {}", logsDir);
                return;
            }

            File[] kylinLogs = logsDir.listFiles(pathname -> pathname.getName().startsWith("kylin.log"));
            if (null == kylinLogs || 0 == kylinLogs.length) {
                logger.error("Can not find the kylin.log file!");
                return;
            }

            Pair<String, String> timeRange;
            if (null != jobId) {
                timeRange = getTimeRangeFromLogFileByJobId(jobId, kylinLogs);
                Preconditions.checkArgument(
                        null != timeRange.getFirst() && null != timeRange.getSecond()
                                && timeRange.getFirst().compareTo(timeRange.getSecond()) <= 0,
                        "Can not get time range from log files by jobId: {}", jobId);
            } else {
                timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                        new DateTime(endTime).toString(SECOND_DATE_FORMAT));
            }

            logger.info("Extract kylin log from {} to {} .", timeRange.getFirst(), timeRange.getSecond());

            for (File logFile : kylinLogs) {
                String lastDate = new DateTime(logFile.lastModified()).toString(SECOND_DATE_FORMAT);
                if (lastDate.compareTo(timeRange.getFirst()) < 0) {
                    continue;
                }

                String logFirstTime = getFirstTimeByLogFile(logFile);
                if (Objects.isNull(logFirstTime) || logFirstTime.compareTo(timeRange.getSecond()) > 0) {
                    continue;
                }

                if (logFirstTime.compareTo(timeRange.getFirst()) >= 0
                        && lastDate.compareTo(timeRange.getSecond()) <= 0) {
                    FileUtils.copyFileToDirectory(logFile, destLogDir);
                } else {
                    extractLogByTimeRange(logFile, timeRange, new File(destLogDir, logFile.getName()));
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
                logger.error("Can not find the spark logs: {}", jobPath);
                return;
            }

            for (FileStatus fileStatus : fileStatuses) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("spark log task is interrupt");
                }
                fs.copyToLocalFile(false, fileStatus.getPath(), new Path(sparkLogsDir.getAbsolutePath()), true);
            }
        } catch (Exception e) {
            logger.error("Failed to extract spark log, ", e);
        }
    }

    /**
     * Extract the sparder history event log for job diagnosis. Sparder conf must set "spark.eventLog.rolling.enabled=true"
     * otherwise it always increases.
     */
    public static void extractSparderEventLog(File exportDir, long startTime, long endTime,
            Map<String, String> sparderConf, String appId) {
        try {
            String logDir = sparderConf.get("spark.eventLog.dir");
            boolean rollEnabled = Boolean.parseBoolean(sparderConf.get("spark.eventLog.rolling.enabled").trim());

            File sparkLogsDir = new File(exportDir, "sparder_history");
            FileUtils.forceMkdir(sparkLogsDir);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            FileStatus[] fileStatuses = fs.listStatus(new Path(logDir));
            if (null == fileStatuses || fileStatuses.length == 0) {
                logger.error("Can not find the sparder history event log: {}", logDir);
                return;
            }

            if (!rollEnabled) {
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.getPath().getName().contains(appId)) {
                        fs.copyToLocalFile(false, fileStatus.getPath(), new Path(sparkLogsDir.getAbsolutePath()), true);
                    }
                }
                return;
            }

            for (FileStatus fileStatus : fileStatuses) {
                String fileAppId = fileStatus.getPath().getName().replace(ROLL_LOG_DIR_NAME_PREFIX, "");
                File localFile = new File(sparkLogsDir, fileAppId);
                if (appId.equals(fileAppId)) {
                    copyValidLog(appId, startTime, endTime, fileStatus, fs, localFile);
                    break;
                }
            }

        } catch (Exception e) {
            logger.error("Failed to extract spark log,please check if set spark.eventLog.rolling.enabled=true ", e);
        }
    }

    private static void copyValidLog(String appId, long startTime, long endTime, FileStatus fileStatus, FileSystem fs,
            File localFile) throws IOException {
        FileStatus[] eventStatuses = fs.listStatus(new Path(fileStatus.getPath().toUri()));
        for (FileStatus status : eventStatuses) {

            boolean valid = false;
            String[] att = status.getPath().getName().replace("_" + appId, "").split("_");
            if (att.length >= 3 && ROLL_LOG_FILE_NAME_PREFIX.equals(att[0])) {

                long begin = Long.parseLong(att[2]);
                long end = System.currentTimeMillis();
                if (att.length == 4) {
                    end = Long.parseLong(att[3]);
                }

                boolean isTimeValid = begin <= endTime && end >= startTime;
                boolean isFirstLogFile = "1".equals(att[1]);
                if (isTimeValid || isFirstLogFile) {
                    valid = true;
                }
            }

            if (valid) {
                if (!localFile.exists()) {
                    FileUtils.forceMkdir(localFile);
                }
                fs.copyToLocalFile(false, status.getPath(), new Path(localFile.getAbsolutePath()), true);
            }
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
                logger.error("Can not find the job tmp: {}", hdfsPath);
                return;
            }

            fs.copyToLocalFile(false, new Path(hdfsPath), new Path(jobTmpDir.getAbsolutePath()), true);
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
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("sparder log task is interrupt");
                }
                String hdfsPath = ToolUtil.getSparderLogsDir() + '/' + date.toString("yyyy-MM-dd");
                FileSystem fs = HadoopUtil.getWorkingFileSystem();
                if (fs.exists(new Path(hdfsPath))) {
                    fs.copyToLocalFile(false, new Path(hdfsPath), new Path(sparkLogsDir.getAbsolutePath()), true);
                }

                date = date.plusDays(1);
            }

        } catch (Exception e) {
            logger.error("Failed to extract sparder log, ", e);
        }
    }
}
