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

package io.kyligence.kap.source.hive.tablestats;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableExtSampleJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtSampleJob.class);

    private String project;
    private String submitter;
    private String tableName;
    private int frequency;
    KylinConfig config;

    // for reflection only
    public HiveTableExtSampleJob() {
    }

    public HiveTableExtSampleJob(String project, String tableName) {
        this(project, null, tableName, 0);
    }

    public HiveTableExtSampleJob(String project, String tableName, int frequency) {
        this(project, null, tableName, frequency);
    }

    public HiveTableExtSampleJob(String project, String submitter, String tableName, int frequency) {
        this.project = project;
        this.submitter = submitter;
        this.tableName = tableName;
        this.frequency = frequency;
        this.config = KylinConfig.getInstanceFromEnv();
    }

    public String start() throws IOException {

        String runningJobID = findRunningJob();
        if (runningJobID != null)
            return runningJobID;

        ExecutableManager.getInstance(config).addJob(build());
        return getId();
    }

    public String start(CubingJob parent) throws IOException {
        String runningJobID = findRunningJob();
        if (runningJobID != null)
            return runningJobID;

        addSteps(parent);
        return parent.getId();
    }

    public CubingJob build() throws IOException {
        logger.info("Start HiveTableExt job: " + getId());
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        setDeployEnvName(config.getDeployEnv());
        setProjectName(project);
        setName("SAMPLING TABLE - " + tableName + " - " + format.format(new Date(System.currentTimeMillis())));
        setSubmitter(submitter);
        setParam(CubingExecutableUtil.CUBE_NAME, tableName);
        addSteps(this);
        return this;
    }

    private void addSteps(CubingJob parent) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableDesc desc = metaMgr.getTableDesc(tableName, project);
        TableExtDesc table_ext = metaMgr.getTableExt(tableName, project);
        if (desc == null) {
            throw new IllegalArgumentException("Cannot find table descriptor " + tableName);
        }

        String samplesOutPath = getOutputPath(parent.getId()) + desc.getIdentity();

        if (desc.isView()) {
            addMaterializeViewSteps(parent, desc);
        }

        addExtractStatsStep(parent, desc, samplesOutPath);

        addUpdateStatsMetaStep(parent, samplesOutPath);

        /**
         *
         * The materialized Hive view might be used in model check, so it's better to be reserved.
        if (desc.isView())
            parent.addTask(deleteMaterializedView(desc));
         */

        table_ext.setJodID(parent.getId());
        metaMgr.saveTableExt(table_ext, project);
    }

    private void addMaterializeViewSteps(CubingJob parent, TableDesc desc) throws IOException {
        JobEngineConfig jobConf = new JobEngineConfig(config);
        String checkParam = "-output " + getViewPath(jobConf, desc);
        HadoopShellExecutable checkHdfsPathStep = new HadoopShellExecutable();
        checkHdfsPathStep.setName("Check Dfs Path");
        checkHdfsPathStep.setJobClass(CheckHdfsPath.class);
        checkHdfsPathStep.setJobParams(checkParam);
        parent.addTask(checkHdfsPathStep);
        parent.addTask(materializedView(desc, jobConf));
    }

    private void addExtractStatsStep(CubingJob parent, TableDesc table, String samplesOutPath) {
        String statsStepParam = "-table " + table.getIdentity() + " -output " + samplesOutPath + " -frequency "
                + frequency + " -project " + project;
        MapReduceExecutable collectStatsStep = new MapReduceExecutable();
        collectStatsStep.setName("Extract Stats from Table: " + table.getIdentity());
        collectStatsStep.setMapReduceJobClass(HiveTableExtJob.class);
        collectStatsStep.setMapReduceParams(statsStepParam);
        parent.addTask(collectStatsStep);
    }

    private void addUpdateStatsMetaStep(CubingJob parent, String samplesOutPath) {
        HadoopShellExecutable updateStatsStep = new HadoopShellExecutable();

        String updateStatsParam = "-table " + tableName + " -output " + samplesOutPath + " -project " + project;
        updateStatsStep.setName("Save Table's Stats");
        updateStatsStep.setJobClass(HiveTableExtUpdate.class);
        updateStatsStep.setJobParams(updateStatsParam);
        parent.addTask(updateStatsStep);
    }

    public String findRunningJob() {

        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableExtDesc tableExtDesc = metaMgr.getTableExt(tableName, project);
        if (tableExtDesc == null)
            return null;

        String jobID = tableExtDesc.getJodID();
        if (null == jobID || jobID.isEmpty()) {
            return null;
        }

        AbstractExecutable job = null;
        ExecutableManager exeMgt = ExecutableManager.getInstance(config);
        try {
            job = exeMgt.getJob(jobID);
        } catch (RuntimeException e) {
            /**
             * By design, HiveTableExtSampleJob is moved from kap-engine-mr to kap-source-hive in kap2.3,
             * therefore, kap2.3 or higher version can not parse kap2.2 stats job info.
             */
            logger.warn("Could not parse old version table stats job. job_id:{}, table_name:{}" + jobID + tableName);
        }

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

    private ShellExecutable materializedView(TableDesc desc, JobEngineConfig conf) throws IOException {

        ShellExecutable step = new ShellExecutable();
        step.setName("Materialized View " + desc.getName());
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();

        String condition = "";
        KapConfig kapConfig = KapConfig.wrap(config);
        long viewCounter = kapConfig.getViewMaterializeRowLimit();
        if (viewCounter != -1) {
            condition = "limit " + viewCounter;
        }

        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("USE " + config.getHiveDatabaseForIntermediateTable() + ";").append("\n");
        createIntermediateTableHql.append("DROP TABLE IF EXISTS " + desc.getMaterializedName() + ";\n");
        createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " + desc.getMaterializedName() + "\n");
        createIntermediateTableHql.append("LOCATION '" + getViewPath(conf, desc) + "'\n");
        createIntermediateTableHql.append("AS SELECT * FROM " + desc.getIdentity() + " " + condition + ";\n");
        hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());

        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    private String getViewPath(JobEngineConfig conf, TableDesc desc) {
        return JobBuilderSupport.getJobWorkingDir(conf, getId()) + "/" + desc.getMaterializedName();
    }

    private ShellExecutable deleteMaterializedView(TableDesc desc) throws IOException {

        ShellExecutable step = new ShellExecutable();
        step.setName("Drop Intermediate Table " + desc.getMaterializedName());
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();

        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("USE " + desc.getDatabase() + ";\n");
        createIntermediateTableHql.append("DROP TABLE IF EXISTS " + desc.getMaterializedName() + ";\n");
        hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());
        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    private String getOutputPath(String jobId) {
        return config.getHdfsWorkingDirectory() + "table_stats/" + jobId + "/";
    }

}