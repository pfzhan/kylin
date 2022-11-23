/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.rest.cluster.NacosClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

public class DiagK8sTool extends AbstractInfoExtractorTool{
    private static final Logger logger = LoggerFactory.getLogger("diag");
    HttpHeaders headers;

    // Query parameters
    private static final Option OPTION_QUERY_ID = OptionBuilder.getInstance().withArgName("query").hasArg()
            .isRequired(false).withDescription("specify the Query ID to extract information. ").create("query");
    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().withArgName("project").hasArg()
            .isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    // Job Parameters
    private static final Option OPTION_JOB_ID = OptionBuilder.getInstance().withArgName("job").hasArg().isRequired(false)
            .withDescription("specify the Job ID to extract information. ").create("job");

    public DiagK8sTool(HttpHeaders headers) {
        super();
        this.headers = headers;
        options.addOption(OPTION_QUERY_ID);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_JOB_ID);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {

        final long startTime = getLongOption(optionsHelper, OPTION_START_TIME, getDefaultStartTime());
        final long endTime = getLongOption(optionsHelper, OPTION_END_TIME, getDefaultEndTime());
        final String queryId = optionsHelper.getOptionValue(OPTION_QUERY_ID);
        final String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        final String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);

        final File recordTime = new File(exportDir, "time_used_info");

        if(queryId != null) {
            extractQueryDiag(exportDir, recordTime, queryId, project);
        } else if(jobId != null) {
            extractJobDiag(exportDir, recordTime, jobId);
        } else {
            extractSysDiag(exportDir, recordTime, startTime, endTime);
        }

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());
    }

    private void dumpMetadata(File exportDir, File recordTime) throws IOException {
        File metaDir = new File(exportDir, "metadata");
        FileUtils.forceMkdir(metaDir);
        String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_COMPRESS, FALSE,
                "-excludeTableExd" };
        dumpMetadata(metaToolArgs, recordTime);
        //TODO dump other metadata which has isolate table
    }

    private void exportAuditLog(File exportDir, File recordTime, long startTime, long endTime) throws IOException {
        File auditLogDir = new File(exportDir, "audit_log");
        FileUtils.forceMkdir(auditLogDir);
        String[] auditLogToolArgs = { "-startTime", String.valueOf(startTime), "-endTime", String.valueOf(endTime),
                OPT_DIR, auditLogDir.getAbsolutePath() };
        exportAuditLog(auditLogToolArgs, recordTime);
    }

    private void extractSysDiag(File exportDir, File recordTime, long startTime, long endTime) throws IOException {
        dumpMetadata(exportDir, recordTime);
        exportK8sConf(headers, exportDir, recordTime, null);
        KylinLogTool.extractK8sKylinLog(exportDir, startTime, endTime, null);
        exportAuditLog(exportDir, recordTime, startTime, endTime);
        // TODO Spark logs
    }

    private void extractQueryDiag(File exportDir, File recordTime, String queryId, String project) throws IOException {
        QueryHistory query = new QueryDiagInfoTool().getQueryByQueryId(queryId);
        if (project == null || !project.equals(query.getProjectName())) {
            logger.error("Can not find the project: {}", project);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the project: %s", project));
        }
        long startTime = query.getQueryTime();
        long endTime = query.getDuration() + startTime;
        logger.info("query project : {} , startTime : {} , endTime : {}", project, startTime, endTime);
        dumpMetadata(exportDir, recordTime);
        exportK8sConf(headers, exportDir, recordTime, NacosClusterManager.QUERY);
        // TODO this will extract logs from all query pods.
        KylinLogTool.extractK8sKylinLog(exportDir, startTime, endTime, NacosClusterManager.QUERY);
        exportAuditLog(exportDir, recordTime, startTime, endTime);
        //TODO extract SparkLogs

    }

    private void extractJobDiag(File exportDir, File recordTime, String jobId) throws IOException {
        ExecutablePO job = new JobDiagInfoTool().getJobByJobId(jobId);
        if (null == job) {
            logger.error("Can not find the jobId: {}", jobId);
            throw new RuntimeException(String.format(Locale.ROOT, "Can not find the jobId: %s", jobId));
        }
        String project = job.getProject();
        long startTime = job.getCreateTime();
        long endTime = job.getOutput().getEndTime() != 0 ? job.getOutput().getEndTime() : System.currentTimeMillis();
        logger.info("job project : {} , startTime : {} , endTime : {}", project, startTime, endTime);
        dumpMetadata(exportDir, recordTime);
        exportAuditLog(exportDir, recordTime, startTime, endTime);
        exportK8sConf(headers, exportDir, recordTime, NacosClusterManager.DATA_LOADING);
        // TODO this will extract logs from all data_loading pods.
        KylinLogTool.extractK8sKylinLog(exportDir, startTime, endTime, NacosClusterManager.DATA_LOADING);
        //TODO extract SparkLogs

    }
}
