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

package io.kyligence.kap.rest;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kylingence.kap.event.handle.AddCuboidHandler;
import io.kylingence.kap.event.handle.AddSegmentHandler;
import io.kylingence.kap.event.handle.CubePlanRuleUpdateHandler;
import io.kylingence.kap.event.handle.LoadingRangeRefreshHandler;
import io.kylingence.kap.event.handle.LoadingRangeUpdateHandler;
import io.kylingence.kap.event.handle.MergeSegmentHandler;
import io.kylingence.kap.event.handle.ModelUpdateHandler;
import io.kylingence.kap.event.handle.PostCubePlanRuleUpdateHandler;
import io.kylingence.kap.event.handle.ProjectHandler;
import io.kylingence.kap.event.handle.RefreshSegmentHandler;
import io.kylingence.kap.event.handle.RemoveCuboidByIdHandler;
import io.kylingence.kap.event.handle.RemoveCuboidBySqlHandler;
import io.kylingence.kap.event.handle.RemoveSegmentHandler;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.source.jdbc.H2Database;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

@ImportResource(locations = {"applicationContext.xml", "kylinSecurity.xml"})
@SpringBootApplication
public class BootstrapServer {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapServer.class);

    public static void main(String[] args) {
        setEnvs(args);
        SpringApplication.run(BootstrapServer.class, args);
    }

    private static void setEnvs(String[] args) {
        String runEnv = "";
        if (args.length >= 1) {
            runEnv = args[0];
        }
        if ("SANDBOX".equals(runEnv)) {
            setSandboxEnvs();
        } else if ("PROD".equals(runEnv)){
            setProdEnvs();
        } else {
            setLocalEnvs();
        }
        initBackend();
        initSparkSession();
    }

    private static void setProdEnvs() {
        //TODO prepare prod env if need
    }

    private static void setSandboxEnvs() {
        File dir1 = new File("../examples/test_case_data/sandbox");
        ClassUtil.addClasspath(dir1.getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());

        System.setProperty("kylin.hadoop.conf.dir",
                "../examples/test_case_data/sandbox");

    }

    private static void initSparkSession() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        // use SparkEnv to init SparkSession later
        final SparkConf sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString());
        if (config.getDeployEnv().equals("UT")) {
            sparkConf.setMaster("local[4]");
        } else {
            sparkConf.setMaster("yarn-client");
            sparkConf.set("spark.yarn.dist.jars", config.getKylinJobJarPath());
            Map<String, String> sparkConfs = config.getSparkConfigOverride();
            if (sparkConfs != null && sparkConfs.size() > 0) {
                for (Map.Entry<String, String> conf : sparkConfs.entrySet()) {
                    sparkConf.set(conf.getKey(), conf.getValue());
                }
            }
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");

        SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    }

    private static void initBackend() {
        new ProjectHandler();
        new AddSegmentHandler();
        new MergeSegmentHandler();
        new RemoveSegmentHandler();
        new RemoveCuboidBySqlHandler();
        new RemoveCuboidByIdHandler();
        new AddCuboidHandler();
        new ModelUpdateHandler();
        new LoadingRangeUpdateHandler();
        new LoadingRangeRefreshHandler();
        new RefreshSegmentHandler();
        new CubePlanRuleUpdateHandler();
        new PostCubePlanRuleUpdateHandler();

        NDefaultScheduler scheduler = NDefaultScheduler.getInstance("default");
        try {
            scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    private static void setLocalEnvs() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        File localMetadata = new File(tempMetadataDir);

        // pass checkHadoopHome
        System.setProperty("hadoop.home.dir", localMetadata.getAbsolutePath() + "/working-dir");

        // set influx config
        System.setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");

        // enable push down
        System.setProperty("kylin.query.pushdown.runner-class-name",
                "org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl");
        System.setProperty("kylin.query.pushdown.converter-class-names",
                "org.apache.kylin.source.adhocquery.HivePushDownConverter");

        // set h2 configuration
        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");

        // Load H2 Tables (inner join)
        try {
            Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
            H2Database h2DB = new H2Database(h2Connection, KylinConfig.getInstanceFromEnv(), "default");
            h2DB.loadAllTables();
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

}
