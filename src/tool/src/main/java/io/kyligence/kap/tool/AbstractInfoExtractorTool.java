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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.tool.util.HashFunction;
import io.kyligence.kap.tool.util.ServerInfoUtil;
import io.kyligence.kap.tool.util.ToolUtil;
import io.kyligence.kap.tool.util.ZipFileUtil;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractInfoExtractorTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true)
            .withDescription("specify the dest dir to save the related information").create("destDir");

    @SuppressWarnings("static-access")
    static final Option OPTION_START_TIME = OptionBuilder.withArgName("startTime").hasArg().isRequired(false)
            .withDescription("specify the start of time range to extract logs. ").create("startTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_END_TIME = OptionBuilder.withArgName("endTime").hasArg().isRequired(false)
            .withDescription("specify the end of time range to extract logs. ").create("endTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_CURRENT_TIME = OptionBuilder.withArgName("currentTime").hasArg().isRequired(false)
            .withDescription(
                    "specify the current of time from client to fix diff between client and server and timezone problem. ")
            .create("currentTime");

    @SuppressWarnings("static-access")
    static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false)
            .withDescription("specify whether to compress the output with zip. Default true.").create("compress");
    @SuppressWarnings("static-access")
    static final Option OPTION_SUBMODULE = OptionBuilder.withArgName("submodule").hasArg().isRequired(false)
            .withDescription("specify whether this is a submodule of other CLI tool").create("submodule");
    @SuppressWarnings("static-access")
    static final Option OPTION_SYSTEM_ENV = OptionBuilder.withArgName("systemProp").hasArg().isRequired(false)
            .withDescription("specify whether to include system env and properties to extract. Default false.")
            .create("systemProp");

    private static final String DEFAULT_PACKAGE_TYPE = "base";
    private static final String[] COMMIT_SHA1_FILES = { "commit_SHA1", "commit.sha1" };

    public static final String SLASH = "/";
    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String OPT_COMPRESS = "-compress";

    public static final String OPT_PROJECT = "-project";
    public static final String OPT_DIR = "-dir";

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

    private boolean includeSystemEnv;

    private enum StageEnum {
        PREPARE, EXTRACT, COMPRESS, DONE
    }

    protected StageEnum stage = StageEnum.PREPARE;
    protected ExecutorService executorService;
    protected boolean mainTaskComplete;

    public AbstractInfoExtractorTool() {
        options = new Options();
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_SUBMODULE);
        options.addOption(OPTION_SYSTEM_ENV);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);

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

        Preconditions.checkArgument(!StringUtils.isEmpty(exportDest),
                "destDir is not set, exit directly without extracting");

        if (!exportDest.endsWith(SLASH)) {
            exportDest = exportDest + SLASH;
        }

        // create new folder to contain the output
        String packageName = packageType.toLowerCase() + "_"
                + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date());
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
        // compress to zip package
        if (shouldCompress) {
            stage = StageEnum.COMPRESS;
            File tempZipFile = new File(UUID.randomUUID().toString() + ".zip");
            ZipFileUtil.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);
            String sha256Sum = DatatypeConverter.printHexBinary((HashFunction.SHA256.checksum(tempZipFile)));

            String packageFilename = StringUtils.join(new String[] { ToolUtil.getHostName(),
                    getKylinConfig().getServerPort(), packageName, sha256Sum.substring(0, 6) }, '_');
            File zipFile = new File(exportDir, packageFilename + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }
        stage = StageEnum.DONE;
    }

    public void extractCommitFile(File exportDir) {
        try {
            for (String commitSHA1File : COMMIT_SHA1_FILES) {
                File commitFile = new File(kylinHome, commitSHA1File);
                if (commitFile.exists()) {
                    FileUtils.copyFileToDirectory(commitFile, exportDir);
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
        basicSb.append("PackageType: ").append(packageType.toUpperCase()).append("\n");
        String hostname = ToolUtil.getHostName();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
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
        String copyCmd = String.format("cp -r %s %s", srcFile.getAbsolutePath(), destFile.getAbsolutePath());
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

            Pair<Integer, String> result = cmdExecutor.execute(cmd, null);
            String output = result.getSecond();

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
        return optionsHelper.hasOption(option) ? Boolean.valueOf(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public int getIntOption(OptionsHelper optionsHelper, Option option, int defaultVal) {
        return optionsHelper.hasOption(option) ? Integer.valueOf(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public long getLongOption(OptionsHelper optionsHelper, Option option, long defaultVal) {
        return optionsHelper.hasOption(option) ? Long.valueOf(optionsHelper.getOptionValue(option)) : defaultVal;
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
}
