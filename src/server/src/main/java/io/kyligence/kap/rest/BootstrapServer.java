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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.catalina.Context;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.jdbc.H2Database;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.event.handle.AddCuboidHandler;
import io.kyligence.kap.event.handle.AddSegmentHandler;
import io.kyligence.kap.event.handle.CubePlanRuleUpdateHandler;
import io.kyligence.kap.event.handle.LoadingRangeRefreshHandler;
import io.kyligence.kap.event.handle.LoadingRangeUpdateHandler;
import io.kyligence.kap.event.handle.MergeSegmentHandler;
import io.kyligence.kap.event.handle.ModelSemanticUpdateHandler;
import io.kyligence.kap.event.handle.AccelerateEventHandler;
import io.kyligence.kap.event.handle.PostCubePlanRuleUpdateHandler;
import io.kyligence.kap.event.handle.PostModelSemanticUpdateHandler;
import io.kyligence.kap.event.handle.ProjectHandler;
import io.kyligence.kap.event.handle.RefreshSegmentHandler;
import io.kyligence.kap.event.handle.RemoveCuboidByIdHandler;
import io.kyligence.kap.event.handle.RemoveSegmentHandler;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.springframework.scheduling.annotation.EnableScheduling;

@ImportResource(locations = {"applicationContext.xml", "kylinSecurity.xml"})
@SpringBootApplication
@EnableScheduling
public class BootstrapServer implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapServer.class);

    public static void main(String[] args) {
        setEnvs(args);
        SpringApplication.run(BootstrapServer.class, args);
        initSparkSession();
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
        // set influx config
        System.setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
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
        SparderEnv.init();
    }

    private static void initBackend() {
        new ProjectHandler();
        new AddSegmentHandler();
        new MergeSegmentHandler();
        new RemoveSegmentHandler();
        new RemoveCuboidByIdHandler();
        new AddCuboidHandler();
        new AccelerateEventHandler();
        new LoadingRangeUpdateHandler();
        new LoadingRangeRefreshHandler();
        new RefreshSegmentHandler();
        new CubePlanRuleUpdateHandler();
        new PostCubePlanRuleUpdateHandler();
        new ModelSemanticUpdateHandler();
        new PostModelSemanticUpdateHandler();

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            NDefaultScheduler scheduler = NDefaultScheduler.getInstance(projectInstance.getName());
            try {
                scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
            } catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
            if (!scheduler.hasStarted()) {
                throw new RuntimeException("Scheduler for " +projectInstance.getName() + " has not been started");
            }
        }
    }

    private static void setLocalEnvs() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        File localMetadata = new File(tempMetadataDir);

        // pass checkHadoopHome
        System.setProperty("hadoop.home.dir", localMetadata.getAbsolutePath() + "/working-dir");

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

    @Bean
    public EmbeddedServletContainerFactory servletContainer() {
        TomcatEmbeddedServletContainerFactory tomcatFactory = new TomcatEmbeddedServletContainerFactory(){
            @Override
            protected void postProcessContext(Context context) {
                context.addWelcomeFile("index.html");
            }
        };

        return tomcatFactory;
    }
    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        initBackend();
        logger.info("init backend end...");
    }
}
