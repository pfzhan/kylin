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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.kyligence.kap.query.util.ILogExtractor;
import io.kyligence.kap.tool.util.ToolUtil;
import lombok.val;
import scala.collection.JavaConversions;

public class KylinLogTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String CHARSET_NAME = Charset.defaultCharset().name();

    public static final long DAY = 24 * 3600 * 1000L;

    public static final String SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String LOG_TIME_PATTERN = "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})";

    // 2019-11-11 09:30:04,628 INFO  [FetchJobWorker(project:test_fact)-p-94-t-94] threadpool.NDefaultScheduler : start check project test_fact
    private static final String LOG_PATTERN = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) ([^ ]*)[ ]+\\[(.*)\\] ([^: ]*) :([\\n\\r. ]*)";

    private static final int EXTRA_LINES = 100;

    private static final String ROLL_LOG_FILE_NAME_PREFIX = "events";

    // 2019-11-11 03:24:52,342 DEBUG [JobWorker(prj:doc_smart,jobid:8a13964c)-965] job.NSparkExecutable : Copied metadata to the target metaUrl, delete the temp dir: /tmp/kylin_job_meta204633716010108932
    @VisibleForTesting
    public static String getJobLogPattern(String jobId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(jobId));
        return String.format(Locale.ROOT, "%s(.*JobWorker.*jobid:%s.*)|%s.*%s", LOG_TIME_PATTERN, jobId.substring(0, 8),
                LOG_TIME_PATTERN, jobId);
    }

    // group(1) is first  ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})
    // group(2) is (.*JobWorker.*jobid:%s.*)
    // group(3) is second ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})
    @VisibleForTesting
    public static String getJobTimeString(Matcher matcher) {
        String result = matcher.group(1);
        if (StringUtils.isEmpty(result)) {
            result = matcher.group(3);
        }
        return result;
    }

    // 2019-11-11 09:30:04,004 INFO  [Query 4e3350d5-1cd9-450f-ac7e-5859939bedf1-125] service.QueryService : The original query: select * from ssb.SUPPLIER
    private static String getQueryLogPattern(String queryId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(queryId));
        return String.format(Locale.ROOT, "%s(.*Query %s.*)", LOG_TIME_PATTERN, queryId);
    }

    private KylinLogTool() {
    }

    private static boolean checkTimeoutTask(long timeout, String task) {
        return !KylinConfig.getInstanceFromEnv().getDiagTaskTimeoutBlackList().contains(task)
                && System.currentTimeMillis() > timeout;
    }

    private static void extractAllIncludeLogs(File[] logFiles, File destDir, long timeout) throws IOException {
        String[] allIncludeLogs = { "kylin.gc.", "shell.", "kylin.out", "diag.log" };
        for (File logFile : logFiles) {
            for (String includeLog : allIncludeLogs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:all' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                }
            }
        }
    }

    private static void extractPartIncludeLogByDay(File[] logFiles, String startDate, String endDate, File destDir,
            long timeout) throws IOException {
        String[] partIncludeLogByDay = { "access_log." };
        for (File logFile : logFiles) {
            for (String includeLog : partIncludeLogByDay) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:partByDay' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    String date = logFile.getName().split("\\.")[1];
                    if (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0) {
                        Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                    }
                }
            }
        }
    }

    private static void extractPartIncludeLogByMs(File[] logFiles, long start, long end, File destDir, long timeout)
            throws IOException {
        String[] partIncludeLogByMs = { "jstack.timed.log" };
        for (File logFile : logFiles) {
            for (String includeLog : partIncludeLogByMs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:partByMs' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    long time = Long.parseLong(logFile.getName().split("\\.")[3]);
                    if (time >= start && time <= end) {
                        Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                    }
                }
            }
        }
    }

    /**
     * extract the specified log from kylin logs dir.
     *
     * @param exportDir
     */

    public static void extractOtherLogs(File exportDir, long start, long end) {
        File destDir = new File(exportDir, "logs");
        SimpleDateFormat logFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        String startDate = logFormat.format(new Date(start));
        String endDate = logFormat.format(new Date(end));
        logger.debug("logs startDate : {}, endDate : {}", startDate, endDate);
        long duration = KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000L;
        long timeout = System.currentTimeMillis() + duration;
        try {
            FileUtils.forceMkdir(destDir);
            File kylinLogDir = new File(ToolUtil.getLogFolder());
            if (kylinLogDir.exists()) {
                File[] logFiles = kylinLogDir.listFiles();
                if (null == logFiles) {
                    logger.error("Failed to list kylin logs dir: {}", kylinLogDir);
                    return;
                }
                extractAllIncludeLogs(logFiles, destDir, timeout);
                extractPartIncludeLogByDay(logFiles, startDate, endDate, destDir, timeout);
                extractPartIncludeLogByMs(logFiles, start, end, destDir, timeout);
            }
        } catch (Exception e) {
            logger.error("Failed to extract the logs from kylin logs dir, ", e);
        }
    }

    /**
     * extract kylin log by jobId
     * for job diagnosis
     *
     * @param exportDir
     * @param jobId
     */
    public static void extractKylinLog(File exportDir, String jobId) {
        extractKylinLog(exportDir, jobId, null, null);
    }

    /**
     * extract kylin log by time range
     * for full diagnosis
     *
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
        try (InputStream in = new FileInputStream(logFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, CHARSET_NAME))) {
            Pattern pattern = Pattern.compile(getJobLogPattern(jobId));

            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    if (Objects.isNull(dateStart)) {
                        dateStart = getJobTimeString(matcher);
                        if (onlyStartTime) {
                            return new Pair<>(dateStart, dateStart);
                        }
                    }
                    dateEnd = getJobTimeString(matcher);
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
        try (InputStream in = new FileInputStream(logFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, CHARSET_NAME))) {
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

        final String charsetName = Charset.defaultCharset().name();
        try (InputStream in = new FileInputStream(logFile);
                OutputStream out = new FileOutputStream(distFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, charsetName));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charsetName))) {

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
     *
     * @param exportDir
     * @param jobId
     * @param startTime
     * @param endTime
     */
    private static void extractKylinLog(File exportDir, String jobId, Long startTime, Long endTime) {
        File destLogDir = new File(exportDir, "logs");

        try {
            FileUtils.forceMkdir(destLogDir);

            File logsDir = new File(ToolUtil.getKylinHome(), "logs");
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
            long start = System.currentTimeMillis();
            long duration = KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000L;
            long timeout = start + duration;
            for (File logFile : kylinLogs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:kylin.log' task.");
                    break;
                }
                extractLogByRange(logFile, timeRange, destLogDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract kylin.log, ", e);
        }
    }

    /**
     * extract the spark executor log by project and jobId.
     * for job diagnosis
     *
     * @param exportDir
     * @param project
     * @param jobId
     */
    public static void extractSparkLog(File exportDir, String project, String jobId) {
        File sparkLogsDir = new File(exportDir, "spark_logs");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try {
            FileUtils.forceMkdir(sparkLogsDir);
            String sourceLogsPath = SparkLogExtractorFactory.create(kylinConfig).getSparkLogsDir(project, kylinConfig);
            FileSystem fs = HadoopUtil.getFileSystem(sourceLogsPath);
            String jobPath = sourceLogsPath + "/*/" + jobId;
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
            if (kylinConfig.cleanDiagTmpFile()) {
                logger.info("Clean tmp spark logs {}", sourceLogsPath);
                fs.delete(new Path(sourceLogsPath), true);
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
            Map<String, String> sparderConf, ILogExtractor extractTool) {
        val sparkLogsDir = new File(exportDir, "sparder_history");
        val fs = HadoopUtil.getFileSystem(extractTool.getSparderEvenLogDir());
        val validApps = extractTool.getValidSparderApps(startTime, endTime);
        JavaConversions.asJavaCollection(validApps).forEach(app -> {
            try {
                if (!sparkLogsDir.exists()) {
                    FileUtils.forceMkdir(sparkLogsDir);
                }
                String fileAppId = app.getPath().getName().split("#")[0].replace(extractTool.ROLL_LOG_DIR_NAME_PREFIX(),
                        "");
                File localFile = new File(sparkLogsDir, fileAppId);
                copyValidLog(fileAppId, startTime, endTime, app, fs, localFile);
            } catch (Exception e) {
                logger.error("Failed to extract sparder eventLog.", e);
            }
        });
    }

    private static void copyValidLog(String appId, long startTime, long endTime, FileStatus fileStatus, FileSystem fs,
            File localFile) throws IOException, InterruptedException {
        FileStatus[] eventStatuses = fs.listStatus(new Path(fileStatus.getPath().toUri()));
        for (FileStatus status : eventStatuses) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Event log task is interrupt");
            }
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

    public static void extractJobEventLogs(File exportDir, Set<String> appIds, Map<String, String> sparkConf) {
        try {
            String logDir = sparkConf.get("spark.eventLog.dir").trim();
            boolean eventEnabled = Boolean.parseBoolean(sparkConf.get("spark.eventLog.enabled").trim());
            if (!eventEnabled || StringUtils.isBlank(logDir)) {
                return;
            }

            File jobLogsDir = new File(exportDir, "job_history");
            FileUtils.forceMkdir(jobLogsDir);
            FileSystem fs = HadoopUtil.getFileSystem(logDir);
            for (String appId : appIds) {
                if (StringUtils.isNotEmpty(appId)) {
                    copyJobEventLog(fs, appId, logDir, jobLogsDir);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract job eventLog.", e);
        }
    }

    private static void copyJobEventLog(FileSystem fs, String appId, String logDir, File exportDir) throws Exception {
        if (StringUtils.isBlank(appId)) {
            logger.warn("Failed to extract step eventLog due to the appId is empty.");
            return;
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Job eventLog task is interrupt");
        }
        logger.info("Copy appId {}.", appId);
        String eventPath = logDir + "/" + appId;
        FileStatus fileStatus = fs.getFileStatus(new Path(eventPath));
        if (fileStatus != null) {
            fs.copyToLocalFile(false, fileStatus.getPath(), new Path(exportDir.getAbsolutePath()), true);
        }
    }

    /**
     * extract the job tmp by project and jobId.
     * for job diagnosis.
     *
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
     *
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractSparderLog(File exportDir, long startTime, long endTime) {
        File sparkLogsDir = new File(exportDir, "spark_logs");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
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
                String sourceLogsPath = SparkLogExtractorFactory.create(kylinConfig).getSparderLogsDir(kylinConfig)
                        + File.separator + date.toString("yyyy-MM-dd");
                FileSystem fs = HadoopUtil.getFileSystem(sourceLogsPath);
                if (fs.exists(new Path(sourceLogsPath))) {
                    fs.copyToLocalFile(false, new Path(sourceLogsPath), new Path(sparkLogsDir.getAbsolutePath()), true);
                }
                if (kylinConfig.cleanDiagTmpFile()) {
                    fs.delete(new Path(sourceLogsPath), true);
                    logger.info("Clean tmp spark logs {}", sourceLogsPath);
                }
                date = date.plusDays(1);
            }

        } catch (Exception e) {
            logger.error("Failed to extract sparder log, ", e);
        }
    }

    private static void extractLogByRange(File logFile, Pair<String, String> timeRange, File destLogDir)
            throws IOException {
        String lastDate = new DateTime(logFile.lastModified()).toString(SECOND_DATE_FORMAT);
        if (lastDate.compareTo(timeRange.getFirst()) < 0) {
            return;
        }

        String logFirstTime = getFirstTimeByLogFile(logFile);
        if (Objects.isNull(logFirstTime) || logFirstTime.compareTo(timeRange.getSecond()) > 0) {
            return;
        }

        if (logFirstTime.compareTo(timeRange.getFirst()) >= 0 && lastDate.compareTo(timeRange.getSecond()) <= 0) {
            Files.copy(logFile.toPath(), new File(destLogDir, logFile.getName()).toPath());
        } else {
            extractLogByTimeRange(logFile, timeRange, new File(destLogDir, logFile.getName()));
        }
    }

    public static void extractKGLogs(File exportDir, long startTime, long endTime) {
        File kgLogsDir = new File(exportDir, "logs");
        try {
            FileUtils.forceMkdir(kgLogsDir);

            if (endTime < startTime) {
                logger.error("endTime: {} < startTime: {}", endTime, startTime);
                return;
            }

            File logsDir = new File(ToolUtil.getKylinHome(), "logs");
            if (!logsDir.exists()) {
                logger.error("Can not find the logs dir: {}", logsDir);
                return;
            }

            File[] kgLogs = logsDir.listFiles(pathname -> pathname.getName().startsWith("guardian.log"));
            if (null == kgLogs || 0 == kgLogs.length) {
                logger.error("Can not find the guardian.log file!");
                return;
            }

            Pair<String, String> timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                    new DateTime(endTime).toString(SECOND_DATE_FORMAT));

            logger.info("Extract guardian log from {} to {} .", timeRange.getFirst(), timeRange.getSecond());

            for (File logFile : kgLogs) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Kg log task is interrupt");
                }
                extractLogByRange(logFile, timeRange, kgLogsDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract kg log!", e);
        }
    }
}
