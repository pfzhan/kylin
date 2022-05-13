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

import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.AUDIT_LOG;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.BIN;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.CATALOG_INFO;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.CLIENT;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.CONF;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.HADOOP_CONF;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.HADOOP_ENV;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.JSTACK;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.KG_LOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.METADATA;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.MONITOR_METRICS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.REC_CANDIDATE;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARDER_HISTORY;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARK_LOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARK_STREAMING_LOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SYSTEM_METRICS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.TIERED_STORAGE_LOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.USE_INFO;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.common.util.ZipFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.query.util.ExtractFactory;
import io.kyligence.kap.query.util.ILogExtractor;
import io.kyligence.kap.tool.constant.DiagSubTaskEnum;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import io.kyligence.kap.tool.constant.StageEnum;
import io.kyligence.kap.tool.obf.KylinConfObfuscator;
import io.kyligence.kap.tool.obf.MappingRecorder;
import io.kyligence.kap.tool.obf.ObfLevel;
import io.kyligence.kap.tool.obf.ResultRecorder;
import io.kyligence.kap.tool.restclient.RestClient;
import io.kyligence.kap.tool.util.DiagnosticFilesChecker;
import io.kyligence.kap.tool.util.HashFunction;
import io.kyligence.kap.tool.util.ServerInfoUtil;
import io.kyligence.kap.tool.util.ToolUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

public abstract class AbstractInfoExtractorTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    static final Option OPTION_DEST = OptionBuilder.getInstance().withArgName("destDir").hasArg().isRequired(true)
            .withDescription("specify the dest dir to save the related information").create("destDir");

    @SuppressWarnings("static-access")
    static final Option OPTION_START_TIME = OptionBuilder.getInstance().withArgName("startTime").hasArg()
            .isRequired(false).withDescription("specify the start of time range to extract logs. ").create("startTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_END_TIME = OptionBuilder.getInstance().withArgName("endTime").hasArg().isRequired(false)
            .withDescription("specify the end of time range to extract logs. ").create("endTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_CURRENT_TIME = OptionBuilder.getInstance().withArgName("currentTime").hasArg()
            .isRequired(false)
            .withDescription(
                    "specify the current of time from client to fix diff between client and server and timezone problem. ")
            .create("currentTime");

    @SuppressWarnings("static-access")
    static final Option OPTION_COMPRESS = OptionBuilder.getInstance().withArgName("compress").hasArg().isRequired(false)
            .withDescription("specify whether to compress the output with zip. Default true.").create("compress");
    @SuppressWarnings("static-access")
    static final Option OPTION_SUBMODULE = OptionBuilder.getInstance().withArgName("submodule").hasArg()
            .isRequired(false).withDescription("specify whether this is a submodule of other CLI tool")
            .create("submodule");
    @SuppressWarnings("static-access")
    static final Option OPTION_SYSTEM_ENV = OptionBuilder.getInstance().withArgName("systemProp").hasArg()
            .isRequired(false)
            .withDescription("specify whether to include system env and properties to extract. Default false.")
            .create("systemProp");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.getInstance().withArgName("threads").hasArg()
            .isRequired(false).withDescription("Specify number of threads for parallel extraction.").create("threads");

    @SuppressWarnings("static-access")
    static final Option OPTION_DIAGID = OptionBuilder.getInstance().withArgName("diagId").hasArg().isRequired(false)
            .withDescription("Specify whether diag from web").create("diagId");

    static final Option OPTION_OBF_LEVEL = OptionBuilder.getInstance().withArgName("obfLevel").hasArg()
            .isRequired(false)
            .withDescription("specify obfuscate level of the diagnostic package: \nRAW means no obfuscate,\n"
                    + "OBF means obfuscate,\nDefault obfuscate level is OBF.")
            .create("obfLevel");

    private static final String DEFAULT_PACKAGE_TYPE = "base";
    private static final String[] COMMIT_SHA1_FILES = {"commit_SHA1", "commit.sha1"};

    public static final String SLASH = "/";
    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String OPT_COMPRESS = "-compress";

    public static final String OPT_PROJECT = "-project";
    public static final String OPT_DIR = "-dir";

    private static final int DEFAULT_PARALLEL_SIZE = 4;

    protected final Options options;

    @Getter
    @Setter
    private String packageType;

    @Getter
    private File exportDir;
    @Getter
    private KylinConfig kylinConfig;
    @Getter
    private KapConfig kapConfig;
    @Getter
    private String kylinHome;

    @Getter
    private CliCommandExecutor cmdExecutor;

    protected ConcurrentHashMap<DiagSubTaskEnum, Long> taskStartTime;
    protected LinkedBlockingQueue<DiagSubTaskInfo> taskQueue;

    private boolean includeSystemEnv;

    protected StageEnum stage = StageEnum.PREPARE;
    protected ScheduledExecutorService executorService;
    protected ScheduledExecutorService timerExecutorService;
    protected boolean mainTaskComplete;

    public AbstractInfoExtractorTool() {
        options = new Options();
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_SUBMODULE);
        options.addOption(OPTION_SYSTEM_ENV);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_DIAGID);
        options.addOption(OPTION_OBF_LEVEL);

        packageType = DEFAULT_PACKAGE_TYPE;

        kylinConfig = KylinConfig.getInstanceFromEnv();
        kapConfig = KapConfig.wrap(kylinConfig);
        kylinHome = KapConfig.getKylinHomeAtBestEffort().getAbsolutePath();

        cmdExecutor = kylinConfig.getCliCommandExecutor();
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    /**
     * extract the diagnosis package content.
     *
     * @param optionsHelper
     * @param exportDir
     * @throws Exception
     */
    protected abstract void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception;

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        stage = StageEnum.PREPARE;
        TimeZoneUtils.setDefaultTimeZone(kylinConfig);

        String exportDest = optionsHelper.getOptionValue(OPTION_DEST);
        boolean shouldCompress = getBooleanOption(optionsHelper, OPTION_COMPRESS, true);
        boolean submodule = getBooleanOption(optionsHelper, OPTION_SUBMODULE, false);
        includeSystemEnv = getBooleanOption(optionsHelper, OPTION_SYSTEM_ENV, false);

        if (isDiag()) {
            final int threadsNum = getIntOption(optionsHelper, OPTION_THREADS, DEFAULT_PARALLEL_SIZE);
            executorService = Executors.newScheduledThreadPool(threadsNum);
            timerExecutorService = Executors.newScheduledThreadPool(2);
            taskQueue = new LinkedBlockingQueue<>();
            taskStartTime = new ConcurrentHashMap<>();
            if (isDiagFromWeb(optionsHelper)) {
                scheduleDiagProgress(optionsHelper.getOptionValue(OPTION_DIAGID));
            }
            logger.info("Start diagnosis info extraction in {} threads.", threadsNum);
        }

        Preconditions.checkArgument(!StringUtils.isEmpty(exportDest),
                "destDir is not set, exit directly without extracting");

        if (!exportDest.endsWith(SLASH)) {
            exportDest = exportDest + SLASH;
        }

        // create new folder to contain the output
        String packageName = packageType.toLowerCase(Locale.ROOT) + "_"
                + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.getDefault(Locale.Category.FORMAT))
                .format(new Date());
        if (!submodule) {
            exportDest = exportDest + packageName + SLASH;
        }

        exportDir = new File(exportDest);
        FileUtils.forceMkdir(exportDir);

        if (!submodule) {
            dumpBasicDiagInfo();
        }
        stage = StageEnum.EXTRACT;
        mainTaskComplete = false;
        executeExtract(optionsHelper, exportDir);
        mainTaskComplete = true;
        obfDiag(optionsHelper, exportDir);
        // compress to zip package
        if (shouldCompress) {
            stage = StageEnum.COMPRESS;
            File tempZipFile = new File(RandomUtil.randomUUIDStr() + ".zip");
            ZipFileUtils.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);
            String sha256Sum = DatatypeConverter.printHexBinary((HashFunction.SHA256.checksum(tempZipFile)));

            String packageFilename = StringUtils.join(new String[]{ToolUtil.getHostName(),
                    getKylinConfig().getServerPort(), packageName, sha256Sum.substring(0, 6)}, '_');
            File zipFile = new File(exportDir, packageFilename + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }
        stage = StageEnum.DONE;
        if (isDiagFromWeb(optionsHelper)) {
            reportDiagProgressImmediately(optionsHelper.getOptionValue(OPTION_DIAGID));
            ExecutorServiceUtil.forceShutdown(timerExecutorService);
        }
    }

    String getObfMappingPath() {
        return KylinConfig.getKylinHome() + "/logs/obfuscation-mapping.json";
    }

    protected void obfDiag(OptionsHelper optionsHelper, File rootDir) throws IOException {
        logger.info("Start obf diag file.");
        ObfLevel obfLevel = ObfLevel.valueOf(kylinConfig.getDiagObfLevel());
        if (optionsHelper.hasOption(OPTION_OBF_LEVEL)) {
            obfLevel = ObfLevel.valueOf(optionsHelper.getOptionValue(OPTION_OBF_LEVEL));
        }
        logger.info("Obf level is {}.", obfLevel);
        try (MappingRecorder recorder = new MappingRecorder(null)) {
            ResultRecorder resultRecorder = new ResultRecorder();
            KylinConfObfuscator kylinConfObfuscator = new KylinConfObfuscator(obfLevel, recorder, resultRecorder);
            kylinConfObfuscator.obfuscate(new File(rootDir, SensitiveConfigKeysConstant.CONF_DIR),
                    file -> (file.isFile() && file.getName().startsWith(SensitiveConfigKeysConstant.KYLIN_PROPERTIES)));
        }
    }

    private boolean isDiag() {
        return this instanceof DiagClientTool || this instanceof JobDiagInfoTool || this instanceof StreamingJobDiagInfoTool || this instanceof QueryDiagInfoTool;
    }

    private boolean isDiagFromWeb(OptionsHelper optionsHelper) {
        return isDiag() && optionsHelper.hasOption(OPTION_DIAGID);
    }

    private void scheduleDiagProgress(String diagId) {
        long interval = 3;
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
        timerExecutorService.scheduleWithFixedDelay(
                () -> restClient.updateDiagProgress(diagId, getStage(), getProgress(), System.currentTimeMillis()), 0, interval, TimeUnit.SECONDS);
    }

    private void reportDiagProgressImmediately(String diagId) {
        int retry = 3;
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
        boolean updateSuccess = false;
        while (retry-- > 0 && !updateSuccess) {
            updateSuccess = restClient.updateDiagProgress(diagId, getStage(), getProgress(), System.currentTimeMillis());
        }
    }

    protected void exportSparkLog(File exportDir, long startTime, long endTime, File recordTime, String queryId) {
        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_LOGS);
            if (StringUtils.isEmpty(queryId)) {
                KylinLogTool.extractFullDiagSparderLog(exportDir, startTime, endTime);
            } else {
                KylinLogTool.extractQueryDiagSparderLog(exportDir, startTime, endTime);
            }
            recordTaskExecutorTimeToFile(SPARK_LOGS, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, SPARK_LOGS);

        // sparder history rolling eventlog
        Future sparderHistoryTask = executorService.submit(() -> {
            recordTaskStartTime(SPARDER_HISTORY);
            ILogExtractor extractTool = ExtractFactory.create();
            tryRollUpEventLog();
            KylinLogTool.extractSparderEventLog(exportDir, startTime, endTime, getKapConfig().getSparkConf(),
                    extractTool);
            recordTaskExecutorTimeToFile(SPARDER_HISTORY, recordTime);
        });

        scheduleTimeoutTask(sparderHistoryTask, SPARDER_HISTORY);
    }

    public void tryRollUpEventLog() {
        int retry = 3;
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
        boolean eventLogSuccess = false;
        while (retry-- > 0 && !eventLogSuccess) {
            eventLogSuccess = restClient.rollUpEventLog();
        }
    }

    public void extractCommitFile(File exportDir) {
        try {
            for (String commitSHA1File : COMMIT_SHA1_FILES) {
                File commitFile = new File(kylinHome, commitSHA1File);
                if (commitFile.exists()) {
                    Files.copy(commitFile.toPath(), new File(exportDir, commitFile.getName()).toPath());
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to copy commit_SHA1 file.", e);
        }
    }

    public void dumpSystemEnv() throws IOException {
        StringBuilder sb = new StringBuilder("System env:").append("\n");
        Map<String, String> systemEnv = System.getenv();
        for (Map.Entry<String, String> entry : systemEnv.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        sb.append("System properties:").append("\n");

        Properties systemProperties = System.getProperties();
        for (String key : systemProperties.stringPropertyNames()) {
            sb.append(key).append("=").append(systemProperties.getProperty(key)).append("\n");
        }
        FileUtils.writeStringToFile(new File(exportDir, "system_env"), sb.toString());
    }

    public void dumpLicenseInfo(File exportDir) throws IOException {
        StringBuilder basicSb = new StringBuilder();

        File[] licenseFiles = new File(kylinHome)
                .listFiles((dir, name) -> name.endsWith(".license") || name.equals("LICENSE"));
        File licFile = null;
        if (licenseFiles != null && licenseFiles.length > 0) {
            for (File licenseFile : licenseFiles) {
                licFile = licenseFile;
            }
        }

        StringBuilder licSb = new StringBuilder();
        if (null != licFile) {
            int splitPos = 0;
            List<String> lines = FileUtils.readLines(licFile);
            licSb.append("Statement: ").append(lines.get(0)).append("\n");
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.startsWith("Parallel Scale:")) {
                    licSb.append(line).append("\n");
                } else if (line.startsWith("Service End:")) {
                    licSb.append(line).append("\n");
                } else if (line.equals("====")) {
                    splitPos = i;
                }
            }
            if (splitPos > 0 && splitPos + 2 < lines.size()) {
                licSb.append(lines.get(splitPos + 1)).append("\n");
                licSb.append(lines.get(splitPos + 2)).append("\n");
            }
        }

        basicSb.append("MetaStoreID: ").append(ToolUtil.getMetaStoreId()).append("\n");
        basicSb.append(licSb.toString());
        basicSb.append("PackageType: ").append(packageType.toUpperCase(Locale.ROOT)).append("\n");
        String hostname = ToolUtil.getHostName();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z",
                Locale.getDefault(Locale.Category.FORMAT));
        basicSb.append("PackageTimestamp: ").append(format.format(new Date())).append("\n");
        basicSb.append("Host: ").append(hostname).append("\n");
        FileUtils.writeStringToFile(new File(exportDir, "info"), basicSb.toString());
    }

    private void dumpBasicDiagInfo() throws IOException {
        // dump commit file
        extractCommitFile(exportDir);

        // dump kylin env
        File kylinEnv = new File(exportDir, "kylin_env");
        FileUtils.writeStringToFile(kylinEnv, ServerInfoUtil.getKylinClientInformation());

        // dump license info
        dumpLicenseInfo(exportDir);

        // dump system env and properties
        if (includeSystemEnv) {
            dumpSystemEnv();
        }
    }

    protected void addFile(File srcFile, File destDir) {
        logger.info("copy file: {}", srcFile.getName());

        try {
            FileUtils.forceMkdir(destDir);
        } catch (IOException e) {
            logger.error("Can not create" + destDir, e);
        }

        File destFile = new File(destDir, srcFile.getName());
        String copyCmd = String.format(Locale.ROOT, "cp -r %s %s", srcFile.getAbsolutePath(),
                destFile.getAbsolutePath());
        logger.info("The command is: {}", copyCmd);

        try {
            cmdExecutor.execute(copyCmd, null);
        } catch (Exception e) {
            logger.debug("Failed to execute copyCmd", e);
        }
    }

    protected void addShellOutput(String cmd, File destDir, String filename) {
        addShellOutput(cmd, destDir, filename, false);
    }

    protected void addShellOutput(String cmd, File destDir, String filename, boolean append) {
        addShellOutput(cmd, destDir, filename, append, false);
    }

    protected void addShellOutput(String cmd, File destDir, String filename, boolean append, boolean errorDebug) {
        if (null == cmdExecutor) {
            logger.error("Failed to run cmd because cmdExecutor is null: {}", cmd);
            return;
        }

        try {
            if (null == destDir) {
                destDir = exportDir;
            }

            FileUtils.forceMkdir(destDir);
            String output = cmdExecutor.execute(cmd, null).getCmd();

            FileUtils.writeStringToFile(new File(destDir, filename), output, append);
        } catch (Exception e) {
            if (errorDebug) {
                logger.debug("Failed to run command: {}", cmd, e);
            } else {
                logger.error("Failed to run command: {}", cmd, e);
            }
        }
    }

    public String getStringOption(OptionsHelper optionsHelper, Option option, String defaultVal) {
        return optionsHelper.hasOption(option) ? optionsHelper.getOptionValue(option) : defaultVal;
    }

    public boolean getBooleanOption(OptionsHelper optionsHelper, Option option, boolean defaultVal) {
        return optionsHelper.hasOption(option) ? Boolean.parseBoolean(optionsHelper.getOptionValue(option))
                : defaultVal;
    }

    public int getIntOption(OptionsHelper optionsHelper, Option option, int defaultVal) {
        return optionsHelper.hasOption(option) ? Integer.parseInt(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public long getLongOption(OptionsHelper optionsHelper, Option option, long defaultVal) {
        return optionsHelper.hasOption(option) ? Long.parseLong(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public String getStage() {
        return stage.toString();
    }

    public float getProgress() {
        if (executorService == null || getStage().equals("PREPARE")) {
            return 0.0f;
        }
        if (getStage().equals("DONE")) {
            return 1.0f;
        }
        long totalTaskCount = ((ThreadPoolExecutor) executorService).getTaskCount() + 1;
        long completedTaskCount = ((ThreadPoolExecutor) executorService).getCompletedTaskCount()
                + (mainTaskComplete ? 1 : 0);
        return (float) completedTaskCount / totalTaskCount * 0.9f;
    }

    protected void awaitDiagPackageTermination(long timeout) throws InterruptedException {
        try {
            if (executorService != null && !executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
                ExecutorServiceUtil.forceShutdown(executorService);
                throw new KylinTimeoutException("The query exceeds the set time limit of "
                        + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds()
                        + "s. Current step: Diagnosis packaging. ");
            }
        } catch (InterruptedException e) {
            ExecutorServiceUtil.forceShutdown(executorService);
            logger.debug("diagnosis main wait for all sub task exit...");
            long start = System.currentTimeMillis();
            boolean allSubTaskExit = executorService.awaitTermination(600, TimeUnit.SECONDS);
            logger.warn("diagnosis main task quit by interrupt , all sub task exit ? {} , waiting for {} ms ",
                    allSubTaskExit, System.currentTimeMillis() - start);
            throw e;
        }
    }

    protected void dumpMetadata(String[] metaToolArgs, File recordTime) {
        Future metadataTask = executorService.submit(() -> {
            recordTaskStartTime(METADATA);
            try {
                File metaDir = new File(exportDir, "metadata");
                FileUtils.forceMkdir(metaDir);
                new MetadataTool().execute(metaToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract metadata.", e);
            }
            recordTaskExecutorTimeToFile(METADATA, recordTime);
        });

        scheduleTimeoutTask(metadataTask, METADATA);
    }

    protected void dumpStreamingSparkLog(String[] sparkToolArgs, File recordTime) {
        Future metadataTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_STREAMING_LOGS);
            try {
                new StreamingSparkLogTool().execute(sparkToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract streaming spark log.", e);
            }
            recordTaskExecutorTimeToFile(SPARK_STREAMING_LOGS, recordTime);
        });

        scheduleTimeoutTask(metadataTask, SPARK_STREAMING_LOGS);
    }

    protected void exportRecCandidate(String project, String modelId, File exportDir, boolean full, File recordTime) {
        val recTask = executorService.submit(() -> {
            recordTaskStartTime(REC_CANDIDATE);
            try {
                File recDir = new File(exportDir, "rec_candidate");
                FileUtils.forceMkdir(recDir);
                if (full) {
                    new RecCandidateTool().extractFull(recDir);
                } else {
                    new RecCandidateTool().extractModel(project, modelId, recDir);
                }
            } catch (Exception e) {
                logger.error("Failed to extract rec candidate.", e);
            }
            recordTaskExecutorTimeToFile(REC_CANDIDATE, recordTime);
        });

        scheduleTimeoutTask(recTask, REC_CANDIDATE);
    }

    protected void exportTieredStorage(String project, File exportDir, long startTime, long endTime, File recordTime) {
        Future kgLogTask = executorService.submit(() -> {
            recordTaskStartTime(TIERED_STORAGE_LOGS);
            new ClickhouseDiagTool(project).dumpClickHouseServerLog(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(TIERED_STORAGE_LOGS, recordTime);
        });

        scheduleTimeoutTask(kgLogTask, TIERED_STORAGE_LOGS);
    }

    protected void exportKgLogs(File exportDir, long startTime, long endTime, File recordTime) {
        Future kgLogTask = executorService.submit(() -> {
            recordTaskStartTime(KG_LOGS);
            KylinLogTool.extractKGLogs(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(KG_LOGS, recordTime);
        });

        scheduleTimeoutTask(kgLogTask, KG_LOGS);
    }

    protected void exportAuditLog(String[] auditLogToolArgs, File recordTime) {
        Future auditTask = executorService.submit(() -> {
            recordTaskStartTime(AUDIT_LOG);
            try {
                new AuditLogTool(KylinConfig.getInstanceFromEnv()).execute(auditLogToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract audit log.", e);
            }
            recordTaskExecutorTimeToFile(AUDIT_LOG, recordTime);
        });

        scheduleTimeoutTask(auditTask, AUDIT_LOG);
    }

    protected void exportInfluxDBMetrics(File exportDir, File recordTime) {
        // influxdb metrics
        Future metricsTask = executorService.submit(() -> {
            recordTaskStartTime(SYSTEM_METRICS);
            InfluxDBTool.dumpInfluxDBMetrics(exportDir);
            recordTaskExecutorTimeToFile(SYSTEM_METRICS, recordTime);
        });

        scheduleTimeoutTask(metricsTask, SYSTEM_METRICS);

        // influxdb sla monitor metrics
        Future monitorTask = executorService.submit(() -> {
            recordTaskStartTime(MONITOR_METRICS);
            InfluxDBTool.dumpInfluxDBMonitorMetrics(exportDir);
            recordTaskExecutorTimeToFile(MONITOR_METRICS, recordTime);
        });

        scheduleTimeoutTask(monitorTask, MONITOR_METRICS);
    }

    protected void exportClient(File recordTime) {
        Future clientTask = executorService.submit(() -> {
            recordTaskStartTime(CLIENT);
            CommonInfoTool.exportClientInfo(exportDir);
            recordTaskExecutorTimeToFile(CLIENT, recordTime);
        });

        scheduleTimeoutTask(clientTask, CLIENT);

    }

    protected void exportJstack(File recordTime) {
        Future jstackTask = executorService.submit(() -> {
            recordTaskStartTime(JSTACK);
            JStackTool.extractJstack(exportDir);
            recordTaskExecutorTimeToFile(JSTACK, recordTime);
        });

        scheduleTimeoutTask(jstackTask, JSTACK);

    }

    protected void exportUseInfo(File recordTime, long startTime, long endTime) {
        Future confTask = executorService.submit(() -> {
            recordTaskStartTime(USE_INFO);
            UseInfoTool.extractUseInfo(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(USE_INFO, recordTime);
        });

        scheduleTimeoutTask(confTask, USE_INFO);
    }

    protected void exportConf(File exportDir, final File recordTime, final boolean includeConf, final boolean includeBin) {
        // export conf
        if (includeConf) {
            Future confTask = executorService.submit(() -> {
                recordTaskStartTime(CONF);
                ConfTool.extractConf(exportDir);
                recordTaskExecutorTimeToFile(CONF, recordTime);
            });

            scheduleTimeoutTask(confTask, CONF);
        }

        // hadoop conf
        Future hadoopConfTask = executorService.submit(() -> {
            recordTaskStartTime(HADOOP_CONF);
            ConfTool.extractHadoopConf(exportDir);
            recordTaskExecutorTimeToFile(HADOOP_CONF, recordTime);
        });

        scheduleTimeoutTask(hadoopConfTask, HADOOP_CONF);

        // export bin
        if (includeBin) {
            Future binTask = executorService.submit(() -> {
                recordTaskStartTime(BIN);
                ConfTool.extractBin(exportDir);
                recordTaskExecutorTimeToFile(BIN, recordTime);
            });

            scheduleTimeoutTask(binTask, BIN);
        }

        // export hadoop env
        Future hadoopEnvTask = executorService.submit(() -> {
            recordTaskStartTime(HADOOP_ENV);
            CommonInfoTool.exportHadoopEnv(exportDir);
            recordTaskExecutorTimeToFile(HADOOP_ENV, recordTime);
        });

        scheduleTimeoutTask(hadoopEnvTask, HADOOP_ENV);

        // export KYLIN_HOME dir
        Future catcalogTask = executorService.submit(() -> {
            recordTaskStartTime(CATALOG_INFO);
            CommonInfoTool.exportKylinHomeDir(exportDir);
            recordTaskExecutorTimeToFile(CATALOG_INFO, recordTime);
        });

        scheduleTimeoutTask(catcalogTask, CATALOG_INFO);
    }

    protected void scheduleTimeoutTask(Future task, DiagSubTaskEnum taskEnum) {
        if (!KylinConfig.getInstanceFromEnv().getDiagTaskTimeoutBlackList().contains(taskEnum.name())) {
            taskQueue.add(new DiagSubTaskInfo(task, taskEnum));
            logger.info("Add {} to task queue.", taskEnum);
        }
    }

    protected void executeTimeoutTask(LinkedBlockingQueue<DiagSubTaskInfo> tasks) {
        timerExecutorService.submit(() -> {
            while (!tasks.isEmpty()) {
                val info = tasks.poll();
                Future task = info.getTask();
                DiagSubTaskEnum taskEnum = info.getTaskEnum();
                logger.info("Timeout task {} start at {}.", taskEnum, System.currentTimeMillis());
                Long startTime = taskStartTime.get(taskEnum);
                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                    logger.info("Task {} start time is not set now, choose current time {} as task start time.",
                            taskEnum, startTime);
                }
                long endTime = startTime + KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000;
                long waitTime = endTime - System.currentTimeMillis();
                logger.info("Timeout task {} wait time is {}ms.", taskEnum, waitTime);
                if (waitTime > 0) {
                    try {
                        task.get(waitTime, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        logger.warn(String.format(Locale.ROOT, "Task %s call get function.", task), e);
                    }
                }
                if (task.cancel(true)) {
                    logger.error("Cancel '{}' task.", taskEnum);
                }
                logger.info("Timeout task {} exit at {}.", taskEnum, System.currentTimeMillis());
            }
        });
    }

    protected void recordTaskStartTime(DiagSubTaskEnum subTask) {
        logger.info("Start to dump {}.", subTask);
        taskStartTime.put(subTask, System.currentTimeMillis());
    }

    protected void recordTaskExecutorTimeToFile(DiagSubTaskEnum subTask, File file) {
        long startTime = taskStartTime.get(subTask);
        DiagnosticFilesChecker.writeMsgToFile(subTask.name(), System.currentTimeMillis() - startTime, file);
    }
}

@Getter
@AllArgsConstructor
class DiagSubTaskInfo {
    private final Future task;
    private final DiagSubTaskEnum taskEnum;
}
