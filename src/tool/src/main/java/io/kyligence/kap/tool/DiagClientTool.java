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
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARDER_HISTORY;
import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.SPARK_LOGS;
import static org.apache.kylin.common.exception.ToolErrorCode.INVALID_SHELL_PARAMETER;

import java.io.File;
import java.util.concurrent.Future;

import io.kyligence.kap.common.util.OptionBuilder;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.OptionsHelper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.query.util.ExtractFactory;
import io.kyligence.kap.query.util.ILogExtractor;
import io.kyligence.kap.tool.util.DiagnosticFilesChecker;
import io.kyligence.kap.tool.util.ToolUtil;

public class DiagClientTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().withArgName("project").hasArg()
            .isRequired(false).withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CONF = OptionBuilder.getInstance().withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");
    @SuppressWarnings("static-access")
    private static final Option OPTION_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");
    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG = OptionBuilder.getInstance().withArgName("includeLog").hasArg()
            .isRequired(false).withDescription("Specify whether to include logs to extract. Default true.")
            .create("includeLog");
    @SuppressWarnings("static-access")
    private static final Option OPTION_SPARK = OptionBuilder.getInstance().withArgName("includeSpark").hasArg()
            .isRequired(false).withDescription("Specify whether to include spark conf to extract. Default false.")
            .create("includeSpark");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CLIENT = OptionBuilder.getInstance().withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");

    // Problem category
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_BASE = OptionBuilder.getInstance().withArgName("base").hasArg()
            .isRequired(false).withDescription("package components include base").create("base");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_QUERY = OptionBuilder.getInstance().withArgName("query").hasArg()
            .isRequired(false).withDescription("package components include slow and failed query").create("query");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_META = OptionBuilder.getInstance().withArgName("meta").hasArg()
            .isRequired(false).withDescription("package components include wrong metadata operation").create("meta");

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

        final long startTime = getLongOption(optionsHelper, OPTION_START_TIME, getDefaultStartTime());
        final long endTime = getLongOption(optionsHelper, OPTION_END_TIME, getDefaultEndTime());
        if (startTime >= endTime) {
            throw new KylinException(INVALID_SHELL_PARAMETER, MsgPicker.getMsg().getINVALID_DIAG_TIME_PARAMETER());
        }
        logger.info("Time range: start={}, end={}", startTime, endTime);

        // calculate time used
        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        // export cube metadata
        if (includeMeta) {
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_COMPRESS, FALSE,
                    "-excludeTableExd" };
            dumpMetadata(metaToolArgs, recordTime);
        }

        File auditLogDir = new File(exportDir, "audit_log");
        FileUtils.forceMkdir(auditLogDir);
        String[] auditLogToolArgs = { "-startTime", String.valueOf(startTime), "-endTime", String.valueOf(endTime),
                OPT_DIR, auditLogDir.getAbsolutePath() };
        exportAuditLog(auditLogToolArgs, recordTime);

        if (includeClient) {
            exportClient(recordTime);
        }

        exportJstack(recordTime);

        exportConf(exportDir, recordTime, includeConf);
        exportInfluxDBMetrics(exportDir, recordTime);

        exportSparkLog(exportDir, startTime, endTime, recordTime);

        exportKgLogs(exportDir, startTime, endTime, recordTime);

        executeTimeoutTask(taskQueue);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        if (includeLog) {
            recordTaskStartTime(LOG);
            KylinLogTool.extractKylinLog(exportDir, startTime, endTime);
            KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(LOG, recordTime);
        }

        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    private void exportSparkLog(File exportDir, long startTime, long endTime, File recordTime) {
        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_LOGS);
            KylinLogTool.extractSparderLog(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(SPARK_LOGS, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, SPARK_LOGS);

        // sparder history rolling eventlog
        Future sparderHistoryTask = executorService.submit(() -> {
            recordTaskStartTime(SPARDER_HISTORY);
            ILogExtractor extractTool = ExtractFactory.create();
            ToolUtil.waitForSparderRollUp();
            KylinLogTool.extractSparderEventLog(exportDir, startTime, endTime, getKapConfig().getSparkConf(),
                    extractTool);
            recordTaskExecutorTimeToFile(SPARDER_HISTORY, recordTime);
        });

        scheduleTimeoutTask(sparderHistoryTask, SPARDER_HISTORY);
    }

    public long getDefaultStartTime() {
        return DateTime.now().minusDays(getKapConfig().getExtractionStartTimeDays() - 1).withTimeAtStartOfDay()
                .getMillis();
    }

    public long getDefaultEndTime() {
        return DateTime.now().plusDays(1).minus(1).withTimeAtStartOfDay().getMillis();
    }
}
