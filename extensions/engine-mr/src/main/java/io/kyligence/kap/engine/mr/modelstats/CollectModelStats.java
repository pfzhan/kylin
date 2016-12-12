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

package io.kyligence.kap.engine.mr.modelstats;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.tablestats.HiveTableExtJob;
import io.kyligence.kap.engine.mr.tablestats.HiveTableExtUpdate;

public class CollectModelStats extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(CollectModelStats.class);

    public static String createCollectJob(String project, String submitter, String modelFlatTable) throws IOException {
        return initCollectJob(project, submitter, modelFlatTable);
    }

    private static String initCollectJob(String project, String submitter, String modelFlatTable) throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String runningJobID = findRunningJob(modelFlatTable, config);
        if (runningJobID != null)
            return runningJobID;

        CollectModelStats result = createCollectJob(project, modelFlatTable, submitter, config);

        ExecutableManager.getInstance(config).addJob(result);
        logger.info("Start ModelStats job: " + result.getId());
        return result.getId();
    }

    private static CollectModelStats createCollectJob(String project, String tableName, String submitter, KylinConfig config) throws IOException {
        CollectModelStats result = new CollectModelStats();

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(config.getDeployEnv());
        result.setProjectName(project);
        result.setName("Collect " + tableName + " statistics " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(tableName);

        String samplesOutPath = getOutputPath(config, result.getId()) + tableName;

        String samplesParam = "-table " + tableName + " -output " + samplesOutPath;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setName("Extract Samples from " + tableName);
        step1.setMapReduceJobClass(HiveTableExtJob.class);
        step1.setMapReduceParams(samplesParam);

        result.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setName("Move " + tableName + " Samples to MetaData");
        step2.setJobClass(HiveTableExtUpdate.class);
        step2.setJobParams(samplesParam);
        result.addTask(step2);

        modelStats.setJodID(result.getId());
        modelStatsManager.saveModelStats(modelStats);

        return result;
    }

    private static String findRunningJob(String table, KylinConfig config) throws IOException {

        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(config);
        ModelStats modelStats = modelStatsManager.getModelStats(table);
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

    private static String getOutputPath(KylinConfig config, String jobID) {
        return config.getHdfsWorkingDirectory() + "modelstats/" + jobID + "/";
    }

}