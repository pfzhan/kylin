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

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.tool.util.DiagnosticFilesChecker;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;

import static io.kyligence.kap.tool.constant.DiagSubTaskEnum.LOG;

public class QueryDiagInfoTool extends AbstractInfoExtractorTool{
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_QUERY_ID = OptionBuilder.getInstance().withArgName("query").hasArg().isRequired(true)
            .withDescription("specify the Query ID to extract information. ").create("query");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().withArgName("project").hasArg().isRequired(true)
            .withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_QUERY_CONF = OptionBuilder.getInstance().withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");
    @SuppressWarnings("static-access")
    private static final Option OPTION_QUERY_CLIENT = OptionBuilder.getInstance().withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");
    @SuppressWarnings("static-access")
    private static final Option OPTION_QUERY_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");

    public QueryDiagInfoTool() {
        super();
        setPackageType("query");

        options.addOption(OPTION_QUERY_ID);
        options.addOption(OPTION_PROJECT);

        options.addOption(OPTION_QUERY_CLIENT);
        options.addOption(OPTION_QUERY_CONF);
        options.addOption(OPTION_QUERY_META);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String queryId = optionsHelper.getOptionValue(OPTION_QUERY_ID);
        final String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_QUERY_CONF, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_QUERY_CLIENT, true);
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_QUERY_META, true);

        final boolean includeBin = false;
        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        QueryHistory query = getQueryByQueryId(queryId);
        if (null == query) {
            logger.error("Can not find the queryId: {}", queryId);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the queryId: %s", queryId));
        }
        if(project == null || !project.equals(query.getProjectName())) {
            logger.error("Can not find the project: {}", project);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the project: %s", project));
        }
        long startTime = query.getQueryTime();
        long endTime = query.getDuration() + startTime;
        logger.info("query project : {} , startTime : {} , endTime : {}", project, startTime, endTime);

        // export project metadata
        if (includeMeta) {
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_PROJECT, project,
                    "-excludeTableExd" };
            dumpMetadata(metaToolArgs, recordTime);
        }

        if (includeClient) {
            exportClient(recordTime);
        }

        exportConf(exportDir, recordTime, includeConf, includeBin);

        exportSparkLog(exportDir, startTime, endTime, recordTime, queryId);

        exportKgLogs(exportDir, startTime, endTime, recordTime);

        executeTimeoutTask(taskQueue);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        recordTaskStartTime(LOG);
        KylinLogTool.extractKylinLog(exportDir, startTime, endTime, queryId);
        KylinLogTool.extractKylinQueryLog(exportDir, queryId);
        KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
        recordTaskExecutorTimeToFile(LOG, recordTime);

        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    public QueryHistory getQueryByQueryId(String queryId) {
        RDBMSQueryHistoryDAO rdbmsQueryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        return rdbmsQueryHistoryDAO.getByQueryId(queryId);
    }
}
