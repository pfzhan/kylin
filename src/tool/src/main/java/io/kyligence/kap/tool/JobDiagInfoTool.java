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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JobDiagInfoTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.withArgName("job").hasArg().isRequired(true)
            .withDescription("specify the Job ID to extract information. ").create("job");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.withArgName("includeYarnLogs").hasArg()
            .isRequired(false)
            .withDescription("set this to true if want to extract related yarn logs too. Default true")
            .create("includeYarnLogs");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.withArgName("threads").hasArg().isRequired(false)
            .withDescription("Specify number of threads for parallel extraction.").create("threads");

    private static final String OPT_JOB = "-job";

    private static final int DEFAULT_PARALLEL_SIZE = 4;

    JobDiagInfoTool() {
        super();
        setPackageType("job");

        options.addOption(OPTION_JOB_ID);

        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);
        options.addOption(OPTION_INCLUDE_CONF);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);

        final boolean includeYarnLogs = getBooleanOption(optionsHelper, OPTION_INCLUDE_YARN_LOGS, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_INCLUDE_CLIENT, true);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_INCLUDE_CONF, true);

        final int threadsNum = getIntOption(optionsHelper, OPTION_THREADS, DEFAULT_PARALLEL_SIZE);

        logger.info("Start diagnosis job info extraction in {} threads.", threadsNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);

        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        String project = getProjectByJobId(jobId);
        if (null == project) {
            logger.error("Can not found the jobId: {}", jobId);
            return;
        }

        // dump job metadata
        executorService.execute(() -> {
            logger.info("Start to dump metadata.");

            try {
                File metaDir = new File(exportDir, "metadata");
                FileUtils.forceMkdir(metaDir);

                String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_PROJECT, project };
                new MetadataTool().execute(metaToolArgs);
            } catch (Exception e) {
                logger.warn("Failed to extract job metadata.", e);
            }

            DiagnosticFilesChecker.writeMsgToFile("METADATA", System.currentTimeMillis() - start, recordTime);
        });

        // dump audit log
        executorService.execute(() -> {
            logger.info("Start to dump audit log.");
            try {
                File auditLogDir = new File(exportDir, "audit_log");
                FileUtils.forceMkdir(auditLogDir);

                String[] auditLogToolArgs = { OPT_JOB, jobId, OPT_PROJECT, project, OPT_DIR,
                        auditLogDir.getAbsolutePath() };
                new AuditLogTool().execute(auditLogToolArgs);
            } catch (Exception e) {
                logger.warn("Failed to extract audit log.", e);
            }

            DiagnosticFilesChecker.writeMsgToFile("AUDIT_LOG", System.currentTimeMillis() - start, recordTime);
        });

        // extract yarn log
        if (includeYarnLogs) {
            executorService.execute(() -> {
                logger.info("Start to dump yarn logs...");
                new YarnApplicationTool().extractYarnLogs(exportDir, project, jobId);
                DiagnosticFilesChecker.writeMsgToFile("YARN", System.currentTimeMillis() - start, recordTime);
            });
        }

        // export client info
        if (includeClient) {
            executorService.execute(() -> {
                logger.info("Start to extract client info.");
                CommonInfoTool.exportClientInfo(exportDir);
                DiagnosticFilesChecker.writeMsgToFile("CLIENT", System.currentTimeMillis() - start, recordTime);
            });
        }

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
            KylinLogTool.extractSparkLog(exportDir, project, jobId);
            DiagnosticFilesChecker.writeMsgToFile("SPARK_LOGS", System.currentTimeMillis() - start, recordTime);
        });

        // job tmp
        executorService.execute(() -> {
            logger.info("Start to extract job_tmp.");
            KylinLogTool.extractJobTmp(exportDir, project, jobId);
            DiagnosticFilesChecker.writeMsgToFile("JOB_TMP", System.currentTimeMillis() - start, recordTime);
        });

        // influxdb metrics
        executorService.execute(() -> {
            logger.info("Start to dump influxdb metrics.");
            InfluxDBTool.dumpInfluxDBMetrics(exportDir);
            DiagnosticFilesChecker.writeMsgToFile("SYSTEM_METRICS", System.currentTimeMillis() - start, recordTime);
        });

        executorService.shutdown();
        if (!executorService.awaitTermination(getKapConfig().getDiagPackageTimeout(),
                TimeUnit.SECONDS)) {
            logger.info("diag diagnosis packaging timeout.");
            throw new KylinTimeoutException("diag diagnosis packaging timeout.");
        }

        // export logs
        logger.info("Start to extract log files.");
        long logStartTime = System.currentTimeMillis();
        KylinLogTool.extractKylinLog(exportDir, jobId);
        KylinLogTool.extractOtherLogs(exportDir);
        DiagnosticFilesChecker.writeMsgToFile("LOG", System.currentTimeMillis() - logStartTime, recordTime);

        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    /**
     * get project by jobId and check the jobId exists.
     * @param jobId
     * @return
     */
    @VisibleForTesting
    public String getProjectByJobId(String jobId) {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(getKylinConfig());
        Set<String> projects = resourceStore.listResources(SLASH);
        if (null == projects) {
            return null;
        }

        for (String project : projects) {
            if ("/UUID".equals(project) || "/_global".equals(project)) {
                continue;
            }

            String resourceRootPath = project + ResourceStore.EXECUTE_RESOURCE_ROOT;
            Set<String> jobs = resourceStore.listResources(resourceRootPath);
            if (null == jobs) {
                continue;
            }

            for (String job : jobs) {
                if (job.equals(resourceRootPath + "/" + jobId)) {
                    return project.substring(1);
                }
            }
        }

        return null;
    }
}
