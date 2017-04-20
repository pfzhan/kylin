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
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
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
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

public class CollectModelStatsJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(CollectModelStatsJob.class);

    public static String initCollectJob(String project, String modelName, String submitter) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String runningJobID = findRunningJob(modelName, config);
        if (runningJobID != null)
            return runningJobID;

        CollectModelStatsJob result = createCollectJob(project, modelName, submitter, config);

        ExecutableManager.getInstance(config).addJob(result);
        logger.info("Start ModelStats job: " + result.getId());
        return result.getId();
    }

    private static CollectModelStatsJob createCollectJob(String project, String modelName, String submitter, KylinConfig config) throws IOException {
        CollectModelStatsJob modelStatsJob = new CollectModelStatsJob();

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        modelStatsJob.setDeployEnvName(config.getDeployEnv());
        modelStatsJob.setProjectName(project);
        modelStatsJob.setName("Collect " + modelName + " statistics " + format.format(new Date(System.currentTimeMillis())));
        modelStatsJob.setSubmitter(submitter);
        modelStatsJob.setParam(CubingExecutableUtil.CUBE_NAME, modelName);

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(modelName);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(modelName);
        IJoinedFlatTableDesc flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc);
        JobEngineConfig jobConf = new JobEngineConfig(config);

        String factTableName = dataModelDesc.getRootFactTable().getTableIdentity();
        TableExtDesc factTableExtDesc = MetadataManager.getInstance(config).getTableExt(factTableName);

        // Do table stats firstly
        if (factTableExtDesc.getColumnStats().size() == 0) {
            HiveTableExtSampleJob.addStatsSteps(factTableName, config, 0, modelStatsJob);
        }

        //Step1: check duplicate key
        checkDuplicateKeyStep(modelStatsJob, modelName);
        //Step2: check data skew
        checkDataSkewStep(modelStatsJob, modelName);
        //step3: create flat table
        modelStatsJob.addTask(createStatsFlatTableStep(jobConf, flatTableDesc, modelStatsJob.getId()));
        //step4: extract model stats
        extractModelStatsStep(modelStatsJob, config, modelName);
        //step5: update model stats metadata
        updateMetaStep(modelStatsJob, config, modelName);
        //step6: clean up intermediate table
        modelStatsJob.addTask(deleteFlatTable(flatTableDesc.getTableName(), config));

        modelStats.setJodID(modelStatsJob.getId());
        modelStatsManager.saveModelStats(modelStats);

        return modelStatsJob;
    }

    private static void checkDuplicateKeyStep(CollectModelStatsJob modelStatsJob, String modelName) {
        CheckLookupStep checkLookupStep = new CheckLookupStep();
        checkLookupStep.setName("Check Duplicate Key");
        checkLookupStep.setParam(CheckLookupStep.MODEL_NAME, modelName);
        modelStatsJob.addTask(checkLookupStep);
    }

    private static void checkDataSkewStep(CollectModelStatsJob modelStatsJob, String modelName) {
        CheckDataSkewStep checkDataSkewStep = new CheckDataSkewStep();
        checkDataSkewStep.setName("Check Data Skew");
        checkDataSkewStep.setParam(CheckLookupStep.MODEL_NAME, modelName);
        modelStatsJob.addTask(checkDataSkewStep);

    }

    private static void extractModelStatsStep(CollectModelStatsJob modelStatsJob, KylinConfig config, String modelName) {
        String outPath = getOutputPath(config, modelStatsJob.getId()) + modelName;
        String param = "-model " + modelName + " -output " + outPath;
        MapReduceExecutable extractStatsStep = new MapReduceExecutable();
        extractStatsStep.setName("Extract Stats from Model: " + modelName);
        extractStatsStep.setMapReduceJobClass(ModelStatsJob.class);
        extractStatsStep.setMapReduceParams(param);
        modelStatsJob.addTask(extractStatsStep);
    }

    private static void updateMetaStep(CollectModelStatsJob modelStatsJob, KylinConfig config, String modelName) {
        String outPath = getOutputPath(config, modelStatsJob.getId()) + modelName;
        HadoopShellExecutable updateMetaStep = new HadoopShellExecutable();
        String param = "-model " + modelName + " -output " + outPath;
        updateMetaStep.setName("Save Model' Stats");
        updateMetaStep.setJobClass(ModelStatsUpdate.class);
        updateMetaStep.setJobParams(param);
        modelStatsJob.addTask(updateMetaStep);
    }

    public static String findRunningJob(String model, KylinConfig config) throws IOException {

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(model);
        String jobID = modelStats.getJodID();

        if (null == jobID || jobID.isEmpty()) {
            return null;
        }

        ExecutableManager exeMgt = ExecutableManager.getInstance(config);
        AbstractExecutable job = exeMgt.getJob(jobID);
        if (null == job) {
            return null;
        }
        ExecutableState state = exeMgt.getOutput(jobID).getState();
        if (ExecutableState.RUNNING == state || ExecutableState.READY == state || ExecutableState.STOPPED == state) {
            return jobID;
        }

        return null;
    }

    public static AbstractExecutable createStatsFlatTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId) {
        String initStatements = JoinedFlatTable.generateHiveInitStatements(conf.getConfig().getHiveDatabaseForIntermediateTable(), conf.getHiveConfFilePath(), conf.getConfig().getHiveConfigOverride());
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc);

        ModelStatsFlatTableStep step = new ModelStatsFlatTableStep();
        step.setInitStatement(initStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
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