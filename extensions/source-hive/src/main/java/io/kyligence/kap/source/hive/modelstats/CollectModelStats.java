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

package io.kyligence.kap.source.hive.modelstats;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;

public class CollectModelStats extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(CollectModelStats.class);

    public static String createCollectJob(String project, String submitter, String modelName) throws IOException {
        return initCollectJob(project, submitter, modelName);
    }

    private static String initCollectJob(String project, String submitter, String modelName) throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String runningJobID = findRunningJob(modelName, config);
        if (runningJobID != null)
            return runningJobID;

        CollectModelStats result = createCollectJob(project, modelName, submitter, config);

        ExecutableManager.getInstance(config).addJob(result);
        logger.info("Start ModelStats job: " + result.getId());
        return result.getId();
    }

    private static CollectModelStats createCollectJob(String project, String modelName, String submitter, KylinConfig config) throws IOException {
        CollectModelStats result = new CollectModelStats();

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(config.getDeployEnv());
        result.setProjectName(project);
        result.setName("Collect " + modelName + " statistics " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(modelName);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(modelName);
        IJoinedFlatTableDesc flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);
        JobEngineConfig jobConf = new JobEngineConfig(config);

        //Step1 Create Flat Table
        result.addTask(createStatsFlatTableStep(jobConf, flatTableDesc, result.getId()));

        String samplesOutPath = getOutputPath(config, result.getId()) + modelName;
        String samplesParam = "-model " + modelName + " -output " + samplesOutPath;
        MapReduceExecutable step1 = new MapReduceExecutable();
        step1.setName("Extract stats from model " + modelName);
        step1.setMapReduceJobClass(ModelStatsJob.class);
        step1.setMapReduceParams(samplesParam);

        //Step2 Collect Stats
        result.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        //String updateParam = "-table " + flatTableDesc.getTableName() + " -output " + samplesOutPath + " -columnsize " + flatTableDesc.getAllColumns().size();
        step2.setName("Move " + modelName + " stats to MetaData");
        step2.setJobClass(ModelStatsUpdate.class);
        step2.setJobParams(samplesParam);
        result.addTask(step2);

        //Step3 Delete Flat Table
        result.addTask(deleteFlatTable(flatTableDesc.getTableName(), config));

        modelStats.setJodID(result.getId());
        modelStatsManager.saveModelStats(modelStats);

        return result;
    }

    public static String findRunningJob(String model, KylinConfig config) throws IOException {

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(model);
        String jobID = modelStats.getJodID();

        if (null == jobID || jobID.isEmpty()) {
            return null;
        }

        ExecutableManager exeMgt = ExecutableManager.getInstance(config);
        ExecutableState state = exeMgt.getOutput(jobID).getState();
        if (ExecutableState.RUNNING == state || ExecutableState.READY == state) {
            return jobID;
        }

        return null;
    }

    public static AbstractExecutable createStatsFlatTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId) {
        StringBuilder hiveInitBuf = new StringBuilder();
        hiveInitBuf.append(JoinedFlatTable.generateHiveSetStatements(conf));
        final KylinConfig kylinConfig = conf.getConfig();
        appendHiveOverrideProperties(kylinConfig, hiveInitBuf);
        final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";\n";
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, conf);

        ModelStatsFlatTableStep step = new ModelStatsFlatTableStep();
        step.setInitStatement(hiveInitBuf.toString());
        step.setCreateTableStatement(useDatabaseHql + dropTableHql + createTableHql + insertDataHqls);
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    private static ShellExecutable deleteFlatTable(String flatTableName, KylinConfig config) throws IOException {

        ShellExecutable step = new ShellExecutable();
        step.setName("Drop Intermediate Flat Table " + flatTableName);
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();

        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("USE " + config.getHiveDatabaseForIntermediateTable() + ";\n");
        createIntermediateTableHql.append("DROP TABLE IF EXISTS " + flatTableName + ";\n");
        hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());
        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    private static void appendHiveOverrideProperties(final KylinConfig kylinConfig, StringBuilder hiveCmd) {
        final Map<String, String> hiveConfOverride = kylinConfig.getHiveConfigOverride();
        if (hiveConfOverride.isEmpty() == false) {
            for (String key : hiveConfOverride.keySet()) {
                hiveCmd.append("SET ").append(key).append("=").append(hiveConfOverride.get(key)).append(";\n");
            }
        }
    }

    private static String getOutputPath(KylinConfig config, String jobID) {
        return config.getHdfsWorkingDirectory() + "modelstats/" + jobID + "/";
    }

}