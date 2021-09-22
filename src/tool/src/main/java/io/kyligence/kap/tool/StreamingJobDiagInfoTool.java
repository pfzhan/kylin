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

import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.LOG;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.tool.util.DiagnosticFilesChecker;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobDiagInfoTool extends AbstractInfoExtractorTool {

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_JOB_ID = OptionBuilder.getInstance().withArgName("streamingJob")
            .hasArg().isRequired(true).withDescription("specify the Streaming Job ID to extract information. ")
            .create("streamingJob");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMINIG_PROJECT = OptionBuilder.getInstance().withArgName("project").hasArg()
            .isRequired(false).withDescription("specify the Project ").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_INCLUDE_YARN_LOGS = OptionBuilder.getInstance()
            .withArgName("includeYarnLogs").hasArg().isRequired(false)
            .withDescription("set this to true if want to extract related streaming yarn logs too. Default true")
            .create("includeYarnLogs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_INCLUDE_CLIENT = OptionBuilder.getInstance()
            .withArgName("includeClient").hasArg().isRequired(false)
            .withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_INCLUDE_CONF = OptionBuilder.getInstance().withArgName("includeConf")
            .hasArg().isRequired(false)
            .withDescription("Specify whether to include ke conf files to extract. Default true.")
            .create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include ke metadata to extract. Default true.")
            .create("includeMeta");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING_AUDIT_LOG = OptionBuilder.getInstance().withArgName("includeAuditLog")
            .hasArg().isRequired(false)
            .withDescription("Specify whether to include ke auditLog to extract. Default true.")
            .create("includeAuditLog");

    private static final String OPT_STREAMING_JOB = "-job";

    public StreamingJobDiagInfoTool() {
        super();
        setPackageType("streaming-job");

        options.addOption(OPTION_STREAMINIG_PROJECT);
        options.addOption(OPTION_STREAMING_JOB_ID);
        options.addOption(OPTION_STREAMING_INCLUDE_CLIENT);
        options.addOption(OPTION_STREAMING_INCLUDE_YARN_LOGS);
        options.addOption(OPTION_STREAMING_INCLUDE_CONF);
        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_STREAMING_META);
        options.addOption(OPTION_STREAMING_AUDIT_LOG);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {

        final String jobId = optionsHelper.getOptionValue(OPTION_STREAMING_JOB_ID);
        final boolean includeYarnLogs = getBooleanOption(optionsHelper, OPTION_STREAMING_INCLUDE_YARN_LOGS, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_STREAMING_INCLUDE_CLIENT, true);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_STREAMING_INCLUDE_CONF, true);
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_STREAMING_META, true);
        final boolean isCloud = getKapConfig().isCloud();
        final boolean includeAuditLog = getBooleanOption(optionsHelper, OPTION_STREAMING_AUDIT_LOG, true);
        final boolean includeBin = true;

        final long diagStartTime = System.currentTimeMillis();

        final File recordTimeFile = new File(exportDir, "time_used_info");

        StreamingJobMeta job = getJobById(jobId);
        if (Objects.isNull(job)) {
            log.error("Can not find the streaming jobId: {}", jobId);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the jobId: %s", jobId));
        }

        String project = job.getProject();
        long createTime = job.getCreateTime();
        long endTime = job.getLastModified() != 0 ? job.getLastModified() : System.currentTimeMillis();
        log.info("job project: {}, job Id: {}, createTime: {}, endTime: {}", project, jobId, createTime, endTime);

        if (includeMeta) {
            File metadataDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metadataDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metadataDir.getAbsolutePath(), OPT_PROJECT, project,
                    "-excludeTableExd" };
            dumpMetadata(metaToolArgs, recordTimeFile);
        }

        if (includeAuditLog) {
            File auditLogDir = new File(exportDir, "audit_log");
            FileUtils.forceMkdir(auditLogDir);
            String[] auditLogToolArgs = { OPT_STREAMING_JOB, jobId, OPT_PROJECT, project, OPT_DIR,
                    auditLogDir.getAbsolutePath() };
            exportAuditLog(auditLogToolArgs, recordTimeFile);
        }

        String modelId = job.getModelId();
        if (StringUtils.isNotEmpty(modelId)) {
            exportRecCandidate(project, modelId, exportDir, false, recordTimeFile);
        }

        if (includeYarnLogs && !isCloud) {
            String[] sparkLogArgs = { OPT_DIR, exportDir.getAbsolutePath(), OPT_STREAMING_JOB, jobId, OPT_PROJECT,
                    project };
            dumpStreamingSparkLog(sparkLogArgs, recordTimeFile);
        }

        if (includeClient) {
            exportClient(recordTimeFile);
        }

        exportJstack(recordTimeFile);

        exportConf(exportDir, recordTimeFile, includeConf, includeBin);

        exportKgLogs(exportDir, createTime, endTime, recordTimeFile);

        exportTieredStorage(project, exportDir, createTime, endTime, recordTimeFile);

        exportInfluxDBMetrics(exportDir, recordTimeFile);

        executeTimeoutTask(taskQueue);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        recordTaskStartTime(LOG);
        KylinLogTool.extractKylinLog(exportDir, jobId);
        KylinLogTool.extractOtherLogs(exportDir, createTime, endTime);
        recordTaskExecutorTimeToFile(LOG, recordTimeFile);
        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - diagStartTime,
                recordTimeFile);
    }

    public StreamingJobMeta getJobById(String jobId) {
        List<String> projectList = NProjectManager.getInstance(getKylinConfig()).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        for (String project : projectList) {
            StreamingJobMeta streamJob = StreamingJobManager.getInstance(getKylinConfig(), project)
                    .getStreamingJobByUuid(jobId);
            if (Objects.nonNull(streamJob)) {
                return streamJob;
            }
        }
        return null;
    }
}
