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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.util.OptionsHelper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.tool.util.DiagnosticFilesChecker;

public class DiagClientTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false)
            .withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false)
            .withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");
    @SuppressWarnings("static-access")
    private static final Option OPTION_META = OptionBuilder.withArgName("includeMeta").hasArg().isRequired(false)
            .withDescription("Specify whether to include metadata to extract. Default true.").create("includeMeta");
    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG = OptionBuilder.withArgName("includeLog").hasArg().isRequired(false)
            .withDescription("Specify whether to include logs to extract. Default true.").create("includeLog");
    @SuppressWarnings("static-access")
    private static final Option OPTION_SPARK = OptionBuilder.withArgName("includeSpark").hasArg().isRequired(false)
            .withDescription("Specify whether to include spark conf to extract. Default false.").create("includeSpark");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false)
            .withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.withArgName("threads").hasArg().isRequired(false)
            .withDescription("Specify number of threads for parallel extraction.").create("threads");

    // Problem category
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_BASE = OptionBuilder.withArgName("base").hasArg().isRequired(false)
            .withDescription("package components include base").create("base");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_QUERY = OptionBuilder.withArgName("query").hasArg().isRequired(false)
            .withDescription("package components include slow and failed query").create("query");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_META = OptionBuilder.withArgName("meta").hasArg().isRequired(false)
            .withDescription("package components include wrong metadata operation").create("meta");

    private static final int DEFAULT_PARALLEL_SIZE = 4;

    public DiagClientTool() {
        super();
        setPackageType("full");

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_CONF);
        options.addOption(OPTION_CLIENT);
        options.addOption(OPTION_SPARK);
        options.addOption(OPTION_CURRENT_TIME);
        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_CATE_BASE);
        options.addOption(OPTION_CATE_QUERY);
        options.addOption(OPTION_CATE_META);
        options.addOption(OPTION_META);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_CONF,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_META,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_CLIENT,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeLog = getBooleanOption(optionsHelper, OPTION_LOG,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));

        final int threadsNum = getIntOption(optionsHelper, OPTION_THREADS, DEFAULT_PARALLEL_SIZE);

        final long startTime = getLongOption(optionsHelper, OPTION_START_TIME, getDefaultStartTime());
        final long endTime = getLongOption(optionsHelper, OPTION_END_TIME, getDefaultEndTime());
        logger.info("Time range: start={}, end={}", startTime, endTime);

        logger.info("Start diagnosis info extraction in {} threads.", threadsNum);
        executorService = Executors.newFixedThreadPool(threadsNum);

        // calculate time used
        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        // export cube metadata
        if (includeMeta) {
            executorService.execute(() -> {
                try {
                    File metaDir = new File(exportDir, "metadata");
                    FileUtils.forceMkdir(metaDir);

                    String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_COMPRESS, FALSE };
                    new MetadataTool().execute(metaToolArgs);
                } catch (Exception e) {
                    logger.warn("Failed to extract job metadata.", e);
                }

                DiagnosticFilesChecker.writeMsgToFile("METADATA", System.currentTimeMillis() - start, recordTime);
            });
        }

        // dump audit log
        executorService.execute(() -> {
            logger.info("Start to dump audit log.");
            try {
                File auditLogDir = new File(exportDir, "audit_log");
                FileUtils.forceMkdir(auditLogDir);

                String[] auditLogToolArgs = { "-startTime", String.valueOf(startTime), "-endTime",
                        String.valueOf(endTime), OPT_DIR, auditLogDir.getAbsolutePath() };
                new AuditLogTool().execute(auditLogToolArgs);
            } catch (Exception e) {
                logger.warn("Failed to extract audit log.", e);
            }
            DiagnosticFilesChecker.writeMsgToFile("AUDIT_LOG", System.currentTimeMillis() - start, recordTime);
        });

        // export conf
        if (includeConf) {
            executorService.execute(() -> {
                logger.info("Start to extract kylin conf files.");
                ConfTool.extractConf(exportDir);
                DiagnosticFilesChecker.writeMsgToFile("CONF", System.currentTimeMillis() - start, recordTime);
            });
        }

        // hadoop conf
        executorService.execute(() -> {
            logger.info("Start to extract hadoop conf files.");
            ConfTool.extractHadoopConf(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("HADOOP_CONF", System.currentTimeMillis() - start, recordTime);
        });

        // export bin
        executorService.execute(() -> {
            logger.info("Start to extract kylin bin files.");
            ConfTool.extractBin(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("BIN", System.currentTimeMillis() - start, recordTime);
        });

        // export client
        if (includeClient) {
            executorService.execute(() -> {
                logger.info("Start to extract client info.");
                CommonInfoTool.exportClientInfo(exportDir);
                DiagnosticFilesChecker.writeMsgToFile("CLIENT", System.currentTimeMillis() - start, recordTime);
            });
        }

        // export hadoop env
        executorService.execute(() -> {
            logger.info("Start to extract hadoop env.");
            CommonInfoTool.exportHadoopEnv(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("HADOOP_ENV", System.currentTimeMillis() - start, recordTime);
        });

        // export KYLIN_HOME dir
        executorService.execute(() -> {
            logger.info("Start to extract KYLIN_HOME dir.");
            CommonInfoTool.exportKylinHomeDir(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("CATALOG_INFO", System.currentTimeMillis() - start, recordTime);
        });

        // dump jstack
        executorService.execute(() -> {
            logger.info("Start to extract jstack info.");
            JStackTool.extractJstack(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("JSTACK", System.currentTimeMillis() - start, recordTime);
        });

        // job spark log
        executorService.execute(() -> {
            logger.info("Start to extract spark logs.");
            KylinLogTool.extractSparderLog(exportDir, startTime, endTime);
            DiagnosticFilesChecker.writeMsgToFile("SPARK_LOGS", System.currentTimeMillis() - start, recordTime);
        });

        // influxdb metrics
        executorService.execute(() -> {
            logger.info("Start to dump influxdb metrics.");
            InfluxDBTool.dumpInfluxDBMetrics(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("SYSTEM_METRICS", System.currentTimeMillis() - start, recordTime);
        });

        // influxdb sla monitor metrics
        executorService.execute(() -> {
            logger.info("Start to dump influxdb sla monitor metrics.");
            InfluxDBTool.dumpInfluxDBMonitorMetrics(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("MONITOR_METRICS", System.currentTimeMillis() - start, recordTime);
        });

        executorService.shutdown();
        if (!executorService.awaitTermination(getKapConfig().getDiagPackageTimeout(), TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            logger.info("diag diagnosis packaging timeout.");
            throw new KylinTimeoutException("diag diagnosis packaging timeout.");
        }

        // export logs
        if (includeLog) {
            long logStartTime = System.currentTimeMillis();
            logger.info("Start to extract log files.");
            KylinLogTool.extractKylinLog(exportDir, startTime, endTime);
            KylinLogTool.extractOtherLogs(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("LOG", System.currentTimeMillis() - logStartTime, recordTime);
        }

        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    public long getDefaultStartTime() {
        return DateTime.now().minusDays(getKapConfig().getExtractionStartTimeDays() - 1).withTimeAtStartOfDay()
                .getMillis();
    }

    public long getDefaultEndTime() {
        return DateTime.now().plusDays(1).minus(1).withTimeAtStartOfDay().getMillis();
    }
}
