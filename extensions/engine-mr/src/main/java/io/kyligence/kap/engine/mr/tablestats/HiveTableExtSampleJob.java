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

package io.kyligence.kap.engine.mr.tablestats;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityJob;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityUpdateJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableExtSampleJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtSampleJob.class);
    private static final String DEPLOY_ENV_NAME = "envName";
    private static final String PROJECT_INSTANCE_NAME = "projectName";
    private static final String CARDINALITY = "cardinality";
    private static final String SAMPLES = "samples";

    public static List<String> createSampleJob(String project, String submitter, String... tables) throws IOException {
        List<String> jobIDs = new ArrayList<>();
        for (String table : tables) {
            String jobID = initSampleJob(project, submitter, table);
            jobIDs.add(jobID);
        }
        return jobIDs;
    }

    public static List<String> createSampleJob(String project, String submitter) throws IOException {
        String[] tables = getTableFromProject(project);
        List<String> jobIDs = new ArrayList<>();
        for (String table : tables) {
            String jobID = initSampleJob(project, submitter, table);
            jobIDs.add(jobID);
        }
        return jobIDs;
    }

    public static String initSampleJob(String project, String submitter, String table) throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        HiveTableExtSampleJob result = new HiveTableExtSampleJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(config.getDeployEnv());
        result.setProjectName(project);
        result.setName("Build " + table + " samples " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);

        StringBuffer jobID = new StringBuffer("null");

        if (isJobRuning(config, jobID, table))
            return jobID.toString();

        calculateSamples(result, table, config);

        ExecutableManager.getInstance(config).addJob(result);
        logger.info("Start HiveTableExt job: " + result.getId());
        return result.getId();
    }

    public static void calculateSamples(HiveTableExtSampleJob result, String tableName, KylinConfig config) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableDesc table = metaMgr.getTableDesc(tableName);
        TableExtDesc table_ext = metaMgr.getTableExt(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Cannot find table descirptor " + tableName);
        }

        String samplesOutPath = getOutputPath(config, result.getId(), HiveTableExtSampleJob.SAMPLES) + table.getIdentity();
        String samplesParam = "-table " + tableName + " -output " + samplesOutPath;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setName("Extract Samples from " + tableName);
        step1.setMapReduceJobClass(HiveTableExtJob.class);
        step1.setMapReduceParams(samplesParam);

        result.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setName("Update " + tableName + " Samples to MetaData");
        step2.setJobClass(HiveTableExtUpdate.class);
        step2.setJobParams(samplesParam);
        result.addTask(step2);

        String cardinalityOutPath = getOutputPath(config, result.getId(), HiveTableExtSampleJob.CARDINALITY) + table.getIdentity();
        String cardinalityParam = "-table " + tableName + " -output " + cardinalityOutPath;

        MapReduceExecutable step3 = new MapReduceExecutable();

        step3.setName("Extract Cardinality from " + tableName);
        step3.setMapReduceJobClass(HiveColumnCardinalityJob.class);
        step3.setMapReduceParams(cardinalityParam);

        result.addTask(step3);

        HadoopShellExecutable step4 = new HadoopShellExecutable();

        step4.setName("Update " + tableName + "' Cardinality to MetaData");
        step4.setJobClass(HiveColumnCardinalityUpdateJob.class);
        step4.setJobParams(cardinalityParam);
        result.addTask(step4);

        table_ext.setJodID(result.getId());
        metaMgr.saveTableExt(table_ext);
    }

    private static boolean isJobRuning(KylinConfig config, StringBuffer runningJobID, String table) throws IOException {

        MetadataManager metaMgr = MetadataManager.getInstance(config);

        TableExtDesc tableExtDesc = metaMgr.getTableExt(table);

        String jobID = tableExtDesc.getJodID();

        if (null == jobID || jobID.isEmpty()) {
            return false;
        }

        ExecutableManager exeMgt = ExecutableManager.getInstance(config);
        if (ExecutableState.RUNNING == exeMgt.getOutput(jobID).getState()) {
            runningJobID.append(jobID);
            return true;
        }

        return false;
    }

    private static String[] getTableFromProject(String project) throws IOException {

        List<TableDesc> tables = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listDefinedTables(project);
        if (null == tables)
            return null;
        String[] tableNames = new String[tables.size()];
        int index = 0;
        for (TableDesc desc : tables) {
            tableNames[index] = desc.getName();
            index++;
        }

        return tableNames;
    }

    private static String getOutputPath(KylinConfig config, String jobID, String tag) {
        return config.getHdfsWorkingDirectory() + jobID + "/" + tag + "/";
    }

    private void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    private void setProjectName(String name) {
        setParam(PROJECT_INSTANCE_NAME, name);
    }
}