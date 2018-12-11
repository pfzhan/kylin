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
package io.kyligence.kap.spark;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.Driver;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

@SuppressWarnings("serial")
public class KapSparkSession extends SparkSession {

    private static final Logger logger = LoggerFactory.getLogger(KapSparkSession.class);
    private static Boolean isRegister = false;

    private Properties prop;
    private String project;

    public KapSparkSession(SparkContext sc) {
        super(sc);
        SparderEnv.setSparkSession(this);
    }

    public void prepareKylinConfig() {
        String metadataUrl = System.getProperty(KylinConfig.KYLIN_CONF);
        prepareKylinConfig(metadataUrl);
    }

    private void prepareKylinConfig(String metadataUrl) {
        Preconditions.checkNotNull(metadataUrl);
        KylinConfig.destroyInstance();

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, metadataUrl);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl(metadataUrl);

        // make sure a local working directory
        File workingDir = new File(metadataUrl, "working-dir");
        if (workingDir.mkdirs()) {
            String path = workingDir.getAbsolutePath();
            if (!path.startsWith("/"))
                path = "/" + path;
            if (!path.endsWith("/"))
                path = path + "/";
            path = path.replace("\\", "/");
            config.setProperty("kylin.env.hdfs-working-dir", "file:" + path);
        }
    }

    public void use(String project) throws Exception {
        Preconditions.checkArgument(project != null);
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(project, KylinConfig.getInstanceFromEnv());
        prop = new Properties();
        prop.put("model", olapTmp.getAbsolutePath());
        if (KapConfig.getInstanceFromEnv().isSparderEnabled()) {
            prop.put("timeZone", "UTC");
        }
        this.project = project;
        if (!isRegister) {
            DriverManager.registerDriver(new Driver());
            isRegister = true;
        }
        logger.info("Switch project to: {}", project);
    }

    @Override
    public Dataset<Row> sql(String sqlText) {
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            logger.info("Try to query from cube....");
            long startTs = System.currentTimeMillis();
            Dataset<Row> dataset = queryCube(sqlText);
            logger.info("Cool! This sql hits cube...");
            logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return dataset;
        } catch (Throwable e) {
            logger.error("There is no cube can be used for query [{}]", sqlText);
            logger.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    public Dataset<Row> queryFromCube(String sqlText) {
        sqlText = QueryUtil.massageSql(sqlText, project, 0, 0, "DEFAULT");
        return sql(sqlText);
    }

    public Dataset<Row> querySparkSql(String sqlText) {
        logger.info("Fallback this sql to original engine...");
        long startTs = System.currentTimeMillis();
        Dataset<Row> r = super.sql(sqlText);
        logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
        return r;
    }

    /*
    public void collectQueries(String sqlText) {
        // collect queries
        try {
            logger.info("Collect a query: {}", sqlText);
            collectQueryHistory(sqlText);
        } catch (Throwable e) {
            logger.error("Collect query error", e);
        }
    }
    
    private void collectQueryHistory(String sql) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final QueryHistoryManager manager = QueryHistoryManager.getInstance(config);
        QueryHistory queryEntry = new QueryHistory(sql);
        queryEntry.setProject(project);
        queryEntry.setAnsweredBy("RDBMS");
        manager.save(queryEntry);
    }
    */

    public Dataset<Row> queryCube(String sql) throws Exception {
        SparderEnv.skipCompute();
        Connection connection = QueryConnection.getConnection(project);
        connection.createStatement().execute(sql);
        SparderEnv.cleanCompute();
        return SparderEnv.getDF();
    }

    public Dataset<Row> csv(String path) {
        return super.read().csv(path);
    }

    /*
    public void speedUp() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(config), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    
        logger.info("Magic starts from here. Let's wait several minutes......");
        NSmartController.optimizeFromPushdown(config, project);
        logger.info("Auto modeling done. Starts to build......");
        buildAllCubes(config, project);
    
        NDefaultScheduler.destroyInstance();
        use(project);
        logger.info("Job finished. Come on! Query me!");
    }
    */

    public void buildAllCubes(KylinConfig kylinConfig, String proj) throws IOException, InterruptedException {
        kylinConfig.clearManagers();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        NExecutableManager execMgr = NExecutableManager.getInstance(kylinConfig, proj);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);

        for (IRealization realization : projectManager.listAllRealizations(proj)) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
            NDataSegment oneSeg;
            List<NCuboidLayout> layouts;
            boolean isAppend = false;
            if (readySegments.isEmpty()) {
                oneSeg = dataflowManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
                layouts = df.getCubePlan().getAllCuboidLayouts();
                isAppend = true;
            } else {
                oneSeg = readySegments.getFirstSegment();
                layouts = df.getCubePlan().getAllCuboidLayouts().stream()
                        .filter(c -> !oneSeg.getCuboidsMap().containsKey(c.getId())).collect(Collectors.toList());
            }

            // create cubing job
            if (!layouts.isEmpty()) {
                NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts),
                        "ADMIN");
                execMgr.addJob(job);
                while (true) {
                    Thread.sleep(500);
                    ExecutableState status = job.getStatus();
                    if (!status.isReadyOrRunning()) {
                        if (status == ExecutableState.ERROR) {
                            throw new IllegalStateException("Failed to execute job. " + job);
                        } else
                            break;
                    }
                }
                val analysisStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkAnalysisStep());
                val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
                AfterBuildResourceMerger merger = new AfterBuildResourceMerger(kylinConfig, project);
                val layoutIds = layouts.stream().map(NCuboidLayout::getId).collect(Collectors.toSet());
                if (isAppend) {
                    merger.mergeAfterIncrement(df.getName(), oneSeg.getId(), layoutIds, buildStore);
                } else {
                    merger.mergeAfterCatchup(df.getName(), Sets.newHashSet(oneSeg.getId()), layoutIds, buildStore);
                }
                merger.mergeAnalysis(df.getName(), analysisStore);
            }
        }
    }
}
