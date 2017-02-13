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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
    private static final String SAMPLES = "samples";

    public static List<String> createSampleJob(String project, String submitter, String... tables) throws IOException {
        List<String> jobIDs = new ArrayList<>();
        for (String table : tables) {
            String jobID = initSampleJob(project, submitter, table);
            jobIDs.add(jobID);
        }
        return jobIDs;
    }

    private static String initSampleJob(String project, String submitter, String table) throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String runningJobID = findRunningJob(table, config);
        if (runningJobID != null)
            return runningJobID;

        HiveTableExtSampleJob sampleJob = createSamplesJob(project, table, submitter, config);

        ExecutableManager.getInstance(config).addJob(sampleJob);
        logger.info("Start HiveTableExt job: " + sampleJob.getId());
        return sampleJob.getId();
    }

    private static HiveTableExtSampleJob createSamplesJob(String project, String tableName, String submitter, KylinConfig config) throws IOException {
        HiveTableExtSampleJob sampleJob = new HiveTableExtSampleJob();

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        sampleJob.setDeployEnvName(config.getDeployEnv());
        sampleJob.setProjectName(project);
        sampleJob.setName("Collect " + tableName + " statistics " + format.format(new Date(System.currentTimeMillis())));
        sampleJob.setSubmitter(submitter);
        sampleJob.setParam(CubingExecutableUtil.CUBE_NAME, tableName);

        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableDesc table = metaMgr.getTableDesc(tableName);
        TableExtDesc table_ext = metaMgr.getTableExt(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Cannot find table descriptor " + tableName);
        }

        boolean isScanWhole = KapConfig.wrap(config).IsTableStatsScanWholeTable();
        table.setWholeScan(isScanWhole);
        metaMgr.saveSourceTable(table);

        if (table.isView() || !table.getWholeScan()) {
            String limitRowSize = KapConfig.wrap(config).getTableStatusLimitRowSize();
            JobEngineConfig jobConf = new JobEngineConfig(config);
            String checkParam = "-output " + getViewPath(jobConf, sampleJob.getId(), table);
            HadoopShellExecutable prestep = new HadoopShellExecutable();
            prestep.setName("Check Hdfs Path");
            prestep.setJobClass(CheckHdfsPath.class);
            prestep.setJobParams(checkParam);
            sampleJob.addTask(prestep);
            sampleJob.addTask(materializedView(table, sampleJob.getId(), jobConf, "limit " + limitRowSize));
            logger.info("The View: " + tableName + " will be materialized in maximum" + limitRowSize + "lines!");
        }

        String samplesOutPath = getOutputPath(config, sampleJob.getId(), HiveTableExtSampleJob.SAMPLES) + table.getIdentity();
        String samplesParam = "-table " + tableName + " -output " + samplesOutPath;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setName("Extract Samples from " + tableName);
        step1.setMapReduceJobClass(HiveTableExtJob.class);
        step1.setMapReduceParams(samplesParam);

        sampleJob.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setName("Move " + tableName + " Samples to MetaData");
        step2.setJobClass(HiveTableExtUpdate.class);
        step2.setJobParams(samplesParam);
        sampleJob.addTask(step2);

        if (table.isView())
            sampleJob.addTask(deleteMaterializedView(table, config));

        table_ext.setJodID(sampleJob.getId());
        metaMgr.saveTableExt(table_ext);

        return sampleJob;
    }

    public static String findRunningJob(String table, KylinConfig config) throws IOException {

        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableExtDesc tableExtDesc = metaMgr.getTableExt(table);
        String jobID = tableExtDesc.getJodID();

        if (null == jobID || jobID.isEmpty()) {
            return null;
        }

        ExecutableManager exeMgt = ExecutableManager.getInstance(config);
        AbstractExecutable job = exeMgt.getJob(jobID);
        if (null == job) {
            return null;
        }
        ExecutableState state = exeMgt.getOutput(jobID).getState();
        if (ExecutableState.RUNNING == state || ExecutableState.READY == state) {
            return jobID;
        }
        return null;
    }

    private static ShellExecutable materializedView(TableDesc desc, String jobId, JobEngineConfig conf, String condition) throws IOException {

        ShellExecutable step = new ShellExecutable();
        step.setName("Materialized View " + desc.getName());
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();

        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("USE " + desc.getDatabase() + ";").append("\n");
        createIntermediateTableHql.append("DROP TABLE IF EXISTS " + desc.getMaterializedName() + ";\n");
        createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " + desc.getMaterializedName() + "\n");
        createIntermediateTableHql.append("LOCATION '" + getViewPath(conf, jobId, desc) + "'\n");
        createIntermediateTableHql.append("AS SELECT * FROM " + desc.getIdentity() + " " + condition + ";\n");
        hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());

        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    private static String getViewPath(JobEngineConfig conf, String jobId, TableDesc desc) {
        return JobBuilderSupport.getJobWorkingDir(conf, jobId) + "/" + desc.getMaterializedName();
    }

    private static ShellExecutable deleteMaterializedView(TableDesc desc, KylinConfig config) throws IOException {

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

    private static String getOutputPath(KylinConfig config, String jobID, String tag) {
        return config.getHdfsWorkingDirectory() + "tablestats/" + jobID + "/" + tag + "/";
    }

}