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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.util.DiagnosticFilesChecker;
import lombok.val;

public class JobDiagInfoTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.getInstance().withArgName("job").hasArg().isRequired(true)
            .withDescription("specify the Job ID to extract information. ").create("job");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.getInstance().withArgName("includeYarnLogs")
            .hasArg().isRequired(false)
            .withDescription("set this to true if want to extract related yarn logs too. Default true")
            .create("includeYarnLogs");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.getInstance().withArgName("includeClient")
            .hasArg().isRequired(false)
            .withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.getInstance().withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.getInstance().withArgName("threads").hasArg()
            .isRequired(false).withDescription("Specify number of threads for parallel extraction.").create("threads");

    @SuppressWarnings("static-access")
    private static final Option OPTION_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");

    private static final String OPT_JOB = "-job";

    private static final int DEFAULT_PARALLEL_SIZE = 4;

    public JobDiagInfoTool() {
        super();
        setPackageType("job");

        options.addOption(OPTION_JOB_ID);

        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);
        options.addOption(OPTION_INCLUDE_CONF);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_META);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        final boolean includeYarnLogs = getBooleanOption(optionsHelper, OPTION_INCLUDE_YARN_LOGS, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_INCLUDE_CLIENT, true);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_INCLUDE_CONF, true);
        final int threadsNum = getIntOption(optionsHelper, OPTION_THREADS, DEFAULT_PARALLEL_SIZE);
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_META, true);
        final boolean isCloud = getKapConfig().isCloud();

        logger.info("Start diagnosis job info extraction in {} threads.", threadsNum);
        executorService = Executors.newScheduledThreadPool(threadsNum);
        canceledTasks = new ConcurrentSkipListSet<>();
        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        val job = getJobByJobId(jobId);
        if (null == job) {
            logger.error("Can not find the jobId: {}", jobId);
            throw new RuntimeException(String.format("Can not find the jobId: %s", jobId));
        }
        String project = job.getProject();
        long startTime = job.getCreateTime();
        long endTime = job.getEndTime() != 0 ? job.getEndTime() : System.currentTimeMillis();
        logger.info("job project : {} , startTime : {} , endTime : {}", project, startTime, endTime);

        if (includeMeta) {
            // dump job metadata
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_PROJECT, project,
                    "-excludeTableExd" };
            String dumpMetadataCmd = String.format(
                    "%s/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir %s -project %s -excludeTableExd",
                    KylinConfig.getKylinHome(), metaDir.getAbsolutePath(), project);
            dumpMetadata(optionsHelper, metaToolArgs, dumpMetadataCmd, recordTime);
        }

        File auditLogDir = new File(exportDir, "audit_log");
        FileUtils.forceMkdir(auditLogDir);
        String[] auditLogToolArgs = { OPT_JOB, jobId, OPT_PROJECT, project, OPT_DIR, auditLogDir.getAbsolutePath() };
        exportAuditLog(auditLogToolArgs, recordTime);

        // extract yarn log
        if (includeYarnLogs && !isCloud) {
            Future future = executorService.submit(() -> {
                logger.info("Start to dump yarn logs...");
                new YarnApplicationTool().extractYarnLogs(exportDir, project, jobId);
                DiagnosticFilesChecker.writeMsgToFile("YARN", System.currentTimeMillis() - start, recordTime);
            });

            executorService.schedule(() -> {
                if (future.cancel(true)) {
                    logger.error("Failed to extract yarn job logs, yarn service may not be running");
                }
            }, 3, TimeUnit.MINUTES);

        }

        if (includeClient) {
            exportClient(recordTime);
        }

        exportJstack(recordTime);

        exportConf(exportDir, recordTime, includeConf);

        exportSparkLog(exportDir, recordTime, project, jobId, job);

        exportKgLogs(exportDir, startTime, endTime, recordTime);
        exportInfluxDBMetrics(exportDir, recordTime);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        logger.info("Start to extract log files.");
        long logStartTime = System.currentTimeMillis();
        KylinLogTool.extractKylinLog(exportDir, jobId);
        KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
        DiagnosticFilesChecker.writeMsgToFile("LOG", System.currentTimeMillis() - logStartTime, recordTime);
        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    private void exportSparkLog(File exportDir, final File recordTime, String project, String jobId,
            AbstractExecutable job) {
        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            logger.info("Start to extract spark logs.");
            long start = System.currentTimeMillis();
            KylinLogTool.extractSparkLog(exportDir, project, jobId);
            DiagnosticFilesChecker.writeMsgToFile("SPARK_LOGS", System.currentTimeMillis() - start, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, "SPARK_LOGS");

        // extract job step eventLogs
        Future eventLogTask = executorService.submit(() -> {
            logger.info("Start to extract job eventLogs.");
            long start = System.currentTimeMillis();
            if (job instanceof DefaultChainedExecutable) {
                List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
                Map<String, String> sparkConf = getKylinConfig().getSparkConfigOverride();
                KylinLogTool.extractJobEventLogs(exportDir, tasks, sparkConf);
                DiagnosticFilesChecker.writeMsgToFile("JOB_EVENTLOGS", System.currentTimeMillis() - start, recordTime);
            }
        });

        scheduleTimeoutTask(eventLogTask, "JOB_EVENTLOGS");

        // job tmp
        Future jobTmpTask = executorService.submit(() -> {
            logger.info("Start to extract job_tmp.");
            long start = System.currentTimeMillis();
            KylinLogTool.extractJobTmp(exportDir, project, jobId);
            DiagnosticFilesChecker.writeMsgToFile("JOB_TMP", System.currentTimeMillis() - start, recordTime);
        });

        scheduleTimeoutTask(jobTmpTask, "JOB_TMP");
    }

    @VisibleForTesting
    public AbstractExecutable getJobByJobId(String jobId) {
        val projects = NProjectManager.getInstance(getKylinConfig()).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        for (String project : projects) {
            AbstractExecutable job = NExecutableManager.getInstance(getKylinConfig(), project).getJob(jobId);
            if (job != null) {
                return job;
            }
        }
        return null;
    }
}
