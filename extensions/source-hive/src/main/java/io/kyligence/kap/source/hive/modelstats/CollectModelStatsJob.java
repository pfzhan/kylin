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
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.DataModelStatsFlatTableDesc;

public class CollectModelStatsJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(CollectModelStatsJob.class);
    private static final KylinConfig config = KylinConfig.getInstanceFromEnv();

    private SegmentRange segRange;
    private String project;
    private String modelName;
    private String submitter;
    private int frequency;

    // for reflection only
    public CollectModelStatsJob() {
    }

    public CollectModelStatsJob(String project, String modelName, String submitter, SegmentRange segRange,
            int frequency) {
        this.project = project;
        this.modelName = modelName;
        this.submitter = submitter;
        this.segRange = segRange;
        this.frequency = frequency;
    }

    public CollectModelStatsJob(String project, String modelName) {
        this.project = project;
        this.modelName = modelName;
    }

    public String start() throws IOException {

        String runningJobID = findRunningJob();
        if (runningJobID != null)
            return runningJobID;

        logger.info("Start ModelStats job: " + getId());
        ExecutableManager.getInstance(config).addJob(build());
        return getId();
    }

    public CubingJob build() throws IOException {

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        setDeployEnvName(config.getDeployEnv());
        setProjectName(project);
        setName("CHECK MODEL - " + modelName + " - " + format.format(new Date(System.currentTimeMillis())));
        setSubmitter(submitter);
        setParam(CubingExecutableUtil.CUBE_NAME, modelName);

        MetadataManager manager = MetadataManager.getInstance(config);
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(modelName);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(modelName);
        IJoinedFlatTableDesc flatTableDesc = new DataModelStatsFlatTableDesc(dataModelDesc, segRange, getId());
        JobEngineConfig jobConf = new JobEngineConfig(config);

        String factTableName = dataModelDesc.getRootFactTable().getTableIdentity();
        TableExtDesc factTableExtDesc = manager.getTableExt(factTableName, dataModelDesc.getProject());

        boolean isAvail = (factTableExtDesc != null) && factTableExtDesc.getColumnStats().size() > 0;

        //Step1: check duplicate key
        checkDuplicateKeyStep();
        //Step2: check data skew
        checkDataSkewStep(isAvail);
        //step3: create flat table
        addTask(createStatsFlatTableStep(jobConf, flatTableDesc, this.getId()));
        //step4: extract model stats
        extractModelStatsStep();
        //step5: update model stats metadata
        updateMetaStep();
        //step6: clean up intermediate table
        addTask(deleteFlatTable(flatTableDesc.getTableName()));

        modelStats.setStartTime(segRange == null ? 0L : (Long) segRange.start.v);
        modelStats.setEndTime(segRange == null ? 0L : (Long) segRange.end.v);
        modelStats.setJodID(getId());
        modelStatsManager.saveModelStats(modelStats);
        return this;
    }

    private void checkDuplicateKeyStep() {
        CheckLookupStep checkLookupStep = new CheckLookupStep();
        checkLookupStep.setName("Check Duplicate Key");
        checkLookupStep.setParam(CheckLookupStep.MODEL_NAME, modelName);
        addTask(checkLookupStep);
    }

    private void checkDataSkewStep(boolean isAvail) {
        if (false == isAvail)
            return;
        CheckDataSkewStep checkDataSkewStep = new CheckDataSkewStep();
        checkDataSkewStep.setName("Check Data Skew");
        checkDataSkewStep.setParam(CheckLookupStep.MODEL_NAME, modelName);
        addTask(checkDataSkewStep);

    }

    private void extractModelStatsStep() {
        String outPath = getOutputPath(getId()) + modelName;
        String param = "-model " + modelName + " -output " + outPath + " -frequency " + frequency + " -jobId "
                + getId();
        MapReduceExecutable extractStatsStep = new MapReduceExecutable();
        extractStatsStep.setName("Extract Stats from Model: " + modelName);
        extractStatsStep.setMapReduceJobClass(ModelStatsJob.class);
        extractStatsStep.setMapReduceParams(param);
        addTask(extractStatsStep);
    }

    private void updateMetaStep() {
        String outPath = getOutputPath(getId()) + modelName;
        HadoopShellExecutable updateMetaStep = new HadoopShellExecutable();
        String param = "-model " + modelName + " -output " + outPath;
        updateMetaStep.setName("Save Model's Stats");
        updateMetaStep.setJobClass(ModelStatsUpdate.class);
        updateMetaStep.setJobParams(param);
        addTask(updateMetaStep);
    }

    public String findRunningJob() throws IOException {
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(modelName);
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
        if (ExecutableState.RUNNING == state || ExecutableState.READY == state || ExecutableState.STOPPED == state
                || ExecutableState.ERROR == state) {
            return jobID;
        }

        return null;
    }

    public AbstractExecutable createStatsFlatTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc,
            String jobId) {
        String initStatements = JoinedFlatTable
                .generateHiveInitStatements(conf.getConfig().getHiveDatabaseForIntermediateTable());
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc,
                JobBuilderSupport.getJobWorkingDir(conf, jobId));

        String insertDataHqls = JoinedFlatTable.generateInsertPartialDataStatement(flatTableDesc);

        ModelStatsFlatTableStep step = new ModelStatsFlatTableStep();
        step.setInitStatement(initStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    private ShellExecutable deleteFlatTable(String flatTableName) throws IOException {

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

    private String getOutputPath(String jobID) {
        return config.getHdfsWorkingDirectory() + "model_stats/" + jobID + "/";
    }
}