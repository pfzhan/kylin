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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.source.jdbc.H2Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.tool.kerberos.KerberosLoginTask;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinPrepareEnvListener implements EnvironmentPostProcessor, Ordered {
    private static final Logger logger = LoggerFactory.getLogger(KylinPrepareEnvListener.class);

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {

        if (env.getPropertySources().contains("bootstrap")) {
            return;
        }

        if (env.getActiveProfiles().length == 0) {
            env.addActiveProfile("dev");
        }

        if (env.acceptsProfiles("sandbox")) {
            setSandboxEnvs();
        } else if (env.acceptsProfiles("dev")) {
            setLocalEnvs();
        }
        // enable CC check
        Unsafe.setProperty("needCheckCC", "true");
        val config = KylinConfig.getInstanceFromEnv();
        if (config.isCalciteInClauseEnabled()) {
            Unsafe.setProperty("calcite.keep-in-clause", "true");
        } else {
            Unsafe.setProperty("calcite.keep-in-clause", "false");
        }
        if (config.isCalciteConvertMultipleColumnsIntoOrEnabled()) {
            Unsafe.setProperty("calcite.convert-multiple-columns-in-to-or", "true");
        } else {
            Unsafe.setProperty("calcite.convert-multiple-columns-in-to-or", "false");
        }
        TimeZoneUtils.setDefaultTimeZone(config);
        KerberosLoginTask kerberosLoginTask = new KerberosLoginTask();
        kerberosLoginTask.execute();
        env.addActiveProfile(config.getSecurityProfile());

        // add extra hive class paths.
        val extraClassPath = config.getHiveMetastoreExtraClassPath();
        if (StringUtils.isNotEmpty(extraClassPath)) {
            ClassUtil.addToClasspath(extraClassPath, Thread.currentThread().getContextClassLoader());
        }
    }

    private static void setSandboxEnvs() {
        File dir1 = new File("../examples/test_case_data/sandbox");
        ClassUtil.addClasspath(dir1.getAbsolutePath());
        Unsafe.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());

        Unsafe.setProperty("kylin.hadoop.conf.dir", "../examples/test_case_data/sandbox");
        Unsafe.setProperty("hdp.version", "current");

    }

    private static void setLocalEnvs() {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        File localMetadata = new File(tempMetadataDir);

        // pass checkHadoopHome
        Unsafe.setProperty("hadoop.home.dir", localMetadata.getAbsolutePath() + "/working-dir");
        Unsafe.setProperty("spark.local", "true");

        // enable push down
        Unsafe.setProperty("kylin.query.pushdown-enabled", "true");
        Unsafe.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");

        // set h2 configuration
        Unsafe.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.password", "");

        // Load H2 Tables (inner join)
        try {
            Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
            H2Database h2DB = new H2Database(h2Connection, KylinConfig.getInstanceFromEnv(), "default");
            // h2DB.loadAllTables();
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        }
    }
}
