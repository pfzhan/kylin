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

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityJob;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityUpdateJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceStatisticJob extends CubingJob {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceStatisticJob.class);
    private static final String DEPLOY_ENV_NAME = "envName";
    private static final String PROJECT_INSTANCE_NAME = "projectName";
    private static final String STATISTIC_JOB_ID = "jobId";

    public static void createStatisticJob(String project, String submitter, String table) throws IOException {
        initStatJob(project, submitter, table);
    }

    public static void initStatJob(String project, String submitter, String table) throws IOException {
        List<String> tables = null;
        String JobName;
        if (null == table) {
            tables = getTableFromProject(project);
            JobName = "Build Multi-Table Statistics-";
        } else {
            tables = new ArrayList<>();
            tables.add(table);
            JobName = "Build Single-Table Statistics-";
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        if (isAlreadyRuning(config, project, tables))
            return;

        DataSourceStatisticJob result = new DataSourceStatisticJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(config.getDeployEnv());
        result.setProjectName(project);
        result.setName(JobName + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);

        MetadataManager metaMgr = MetadataManager.getInstance(config);
        for (String tableName : tables) {
            Map<String, String> exdMap = metaMgr.getTableDescExd(tableName);
            if (!exdMap.containsKey(DataSourceStatisticJob.STATISTIC_JOB_ID)) {
                exdMap.put(DataSourceStatisticJob.STATISTIC_JOB_ID, result.getId());
                String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
                tableName = dbTableName[0] + "." + dbTableName[1];
                metaMgr.saveTableExd(tableName.toUpperCase(), exdMap);
            }
            calculateCardinality(result, tableName, submitter, config);
        }
        ExecutableManager.getInstance(config).addJob(result);
    }

    public static void calculateCardinality(DataSourceStatisticJob result, String tableName, String submitter, KylinConfig config) {
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        tableName = dbTableName[0] + "." + dbTableName[1];
        TableDesc table = metaMgr.getTableDesc(tableName);
        final Map<String, String> tableExd = metaMgr.getTableDescExd(tableName);
        if (tableExd == null || table == null) {
            IllegalArgumentException e = new IllegalArgumentException("Cannot find table descirptor " + tableName);
            logger.error("Cannot find table descirptor " + tableName, e);
            throw e;
        }

        String outPath = HiveColumnCardinalityJob.OUTPUT_PATH + "/" + tableName;
        String param = "-table " + tableName + " -output " + outPath;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setName("Extract Cardinality from Table-" + tableName);
        step1.setMapReduceJobClass(HiveColumnCardinalityJob.class);
        step1.setMapReduceParams(param);

        result.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setName("Update Table-" + tableName + "' Cardinality to MetaData");
        step2.setJobClass(HiveColumnCardinalityUpdateJob.class);
        step2.setJobParams(param);
        result.addTask(step2);
    }

    private static boolean isAlreadyRuning(KylinConfig config, String project, List<String> tables) {
        boolean isRunning = false;
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        for (String table : tables) {
            Map<String, String> exdMap = metaMgr.getTableDescExd(table);
            if (!exdMap.containsKey(DataSourceStatisticJob.STATISTIC_JOB_ID)) {
                continue;
            } else {
                String jobId = exdMap.get(DataSourceStatisticJob.STATISTIC_JOB_ID);
                ExecutableManager exeMgt = ExecutableManager.getInstance(config);
                if (ExecutableState.RUNNING == exeMgt.getOutput(jobId).getState()) {
                    isRunning = true;
                    break;
                }
            }
        }
        return isRunning;
    }

    public static List<String> getTableFromProject(String project) {
        List<TableDesc> tables = null;
        List<String> tableNames = new ArrayList<>();
        try {
            tables = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listDefinedTables(project);
            for (TableDesc desc : tables) {
                tableNames.add(desc.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableNames;
    }

    void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    void setProjectName(String name) {
        setParam(PROJECT_INSTANCE_NAME, name);
    }
}
