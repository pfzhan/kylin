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

import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.CANDIDATE_LOG;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.JOB_EVENTLOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.JOB_TMP;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.LOG;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARK_LOGS;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.YARN;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
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
    private static final Option OPTION_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");

    @SuppressWarnings("static-access")
    private static final Option OPTION_AUDIT_LOG = OptionBuilder.getInstance().withArgName("includeAuditLog").hasArg()
            .isRequired(false).withDescription("Specify whether to include auditLog to extract. Default true.")
            .create("includeAuditLog");

    private static final String OPT_JOB = "-job";

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
        options.addOption(OPTION_AUDIT_LOG);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        final boolean includeYarnLogs = getBooleanOption(optionsHelper, OPTION_INCLUDE_YARN_LOGS, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_INCLUDE_CLIENT, true);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_INCLUDE_CONF, true);
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_META, true);
        final boolean isCloud = getKapConfig().isCloud();
        final boolean includeAuditLog = getBooleanOption(optionsHelper, OPTION_AUDIT_LOG, true);
        final boolean includeBin = true;

        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        val job = getJobByJobId(jobId);
        if (null == job) {
            logger.error("Can not find the jobId: {}", jobId);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the jobId: %s", jobId));
        }
        String project = job.getProject();
        long startTime = job.getCreateTime();
        long endTime = job.getEndTime() != 0 ? job.getEndTime() : System.currentTimeMillis();
        logger.info("job project : {} , startTime : {} , endTime : {}", project, startTime, endTime);

        if (includeMeta) {
            // dump job metadata
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = {"-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_PROJECT, project,
                    "-excludeTableExd"};
            dumpMetadata(metaToolArgs, recordTime);
        }

        if (includeAuditLog) {
            File auditLogDir = new File(exportDir, "audit_log");
            FileUtils.forceMkdir(auditLogDir);
            String[] auditLogToolArgs = {OPT_JOB, jobId, OPT_PROJECT, project, OPT_DIR, auditLogDir.getAbsolutePath()};
            exportAuditLog(auditLogToolArgs, recordTime);
        }

        String modelId = job.getTargetModelId();
        if (StringUtils.isNotEmpty(modelId)) {
            exportRecCandidate(project, modelId, exportDir, false, recordTime);
        }
        // extract yarn log
        if (includeYarnLogs && !isCloud) {
            Future future = executorService.submit(() -> {
                recordTaskStartTime(YARN);
                new YarnApplicationTool().extractYarnLogs(exportDir, project, jobId);
                recordTaskExecutorTimeToFile(YARN, recordTime);
            });

            scheduleTimeoutTask(future, YARN);
        }

        if (includeClient) {
            exportClient(recordTime);
        }

        exportJstack(recordTime);

        exportConf(exportDir, recordTime, includeConf, includeBin);

        exportSparkLog(exportDir, recordTime, project, jobId, job);
        exportCandidateLog(exportDir, recordTime, project, startTime, endTime);
        exportKgLogs(exportDir, startTime, endTime, recordTime);

        exportTieredStorage(project, exportDir, startTime, endTime, recordTime);

        exportInfluxDBMetrics(exportDir, recordTime);

        executeTimeoutTask(taskQueue);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        recordTaskStartTime(LOG);
        KylinLogTool.extractKylinLog(exportDir, jobId);
        KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
        recordTaskExecutorTimeToFile(LOG, recordTime);
        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    private void exportCandidateLog(File exportDir, File recordTime, String project, long startTime, long endTime) {
        // candidate log
        val candidateLogTask = executorService.submit(() -> {
            recordTaskStartTime(CANDIDATE_LOG);
            KylinLogTool.extractJobTmpCandidateLog(exportDir, project, startTime, endTime);
            recordTaskExecutorTimeToFile(CANDIDATE_LOG, recordTime);
        });
        scheduleTimeoutTask(candidateLogTask, CANDIDATE_LOG);
    }

    private void exportSparkLog(File exportDir, final File recordTime, String project, String jobId,
                                AbstractExecutable job) {
        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_LOGS);
            KylinLogTool.extractSparkLog(exportDir, project, jobId);
            recordTaskExecutorTimeToFile(SPARK_LOGS, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, SPARK_LOGS);

        // extract job step eventLogs
        Future eventLogTask = executorService.submit(() -> {
            if (job instanceof DefaultChainedExecutable) {
                recordTaskStartTime(JOB_EVENTLOGS);
                val appIds = ExecutableManager.getInstance(getKylinConfig(), project).getYarnApplicationJobs(jobId);
                Map<String, String> sparkConf = getKylinConfig().getSparkConfigOverride();
                KylinLogTool.extractJobEventLogs(exportDir, appIds, sparkConf);
                recordTaskExecutorTimeToFile(JOB_EVENTLOGS, recordTime);
            }
        });

        scheduleTimeoutTask(eventLogTask, JOB_EVENTLOGS);

        // job tmp
        Future jobTmpTask = executorService.submit(() -> {
            recordTaskStartTime(JOB_TMP);
            KylinLogTool.extractJobTmp(exportDir, project, jobId);
            recordTaskExecutorTimeToFile(JOB_TMP, recordTime);
        });

        scheduleTimeoutTask(jobTmpTask, JOB_TMP);
    }

    @VisibleForTesting
    public AbstractExecutable getJobByJobId(String jobId) {
        val projects = NProjectManager.getInstance(getKylinConfig()).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        for (String project : projects) {
            AbstractExecutable job = ExecutableManager.getInstance(getKylinConfig(), project).getJob(jobId);
            if (job != null) {
                return job;
            }
        }
        return null;
    }
}
