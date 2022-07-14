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

package io.kyligence.kap.job.execution;

import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.var;

public class NSparkExecutableTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NDataModelManager modelManager;

    @Before
    public void setup() {
        createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void attachMetadataAndKylinProps() throws Exception {
        KylinConfig config = getTestConfig();
        val junitFolder = temporaryFolder.getRoot();
        val path = junitFolder.getAbsolutePath();
        MockSparkTestExecutable executable = new MockSparkTestExecutable();
        executable.setMetaUrl(path);
        executable.setProject("default");
        Assert.assertEquals(8, executable.getMetadataDumpList(config).size());
        NDataModel model = modelManager.getDataModelDesc("82fa7671-a935-45f5-8779-85703601f49a");
        for (int i = 0; i < 10; i++) {
            new Thread(new AddModelRunner(model)).start();
        }
        executable.attachMetadataAndKylinProps(config);
        Assert.assertEquals(2, Objects.requireNonNull(junitFolder.listFiles()).length);
    }

    class AddModelRunner implements Runnable {

        private final NDataModel model;

        AddModelRunner(NDataModel model) {
            this.model = model;
        }

        @Override
        public void run() {
            UnitOfWork.doInTransactionWithRetry(() -> {
                addModel(model);
                Thread.sleep(new Random().nextInt(50));
                return null;
            }, "default");
        }
    }

    private void addModel(NDataModel model) {
        for (int i = 0; i < 3; i++) {
            model = modelManager.copyForWrite(model);
            model.setUuid(RandomUtil.randomUUIDStr());
            model.setAlias(RandomUtil.randomUUIDStr());
            model.setMvcc(-1);
            modelManager.createDataModelDesc(model, "owner");
        }
    }

    @Test
    public void testGenerateSparkCmd() {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("KYLIN_HOME", "/kylin");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");
        String hadoopConf = System.getProperty("KYLIN_HOME") + "/hadoop";
        String kylinJobJar = System.getProperty("KYLIN_HOME") + "/lib/job.jar";
        String appArgs = "/tmp/output";

        overwriteSystemProp("kylin.engine.spark.job-jar", kylinJobJar);
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("spark-submit"));
            Assert.assertTrue(
                    cmd.contains("log4j.configurationFile=file:" + kylinConfig.getLogSparkDriverPropertiesFile()));
            Assert.assertTrue(cmd.contains("spark.executor.extraClassPath=job.jar"));
            Assert.assertTrue(cmd.contains("spark.driver.log4j.appender.hdfs.File="));
            Assert.assertTrue(cmd.contains("kylin.hdfs.working.dir="));
        }

        overwriteSystemProp("kylin.engine.extra-jars-path", "/this_new_path.jar");
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("/this_new_path.jar"));
        }

        overwriteSystemProp("kylin.engine.spark-conf.spark.driver.extraJavaOptions",
                "'`touch /tmp/foo.bar` $(touch /tmp/foo.bar)'");
        {
            try {
                val desc = sparkExecutable.getSparkAppDesc();
                desc.setHadoopConfDir(hadoopConf);
                desc.setKylinJobJar(kylinJobJar);
                desc.setAppArgs(appArgs);
                String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);
            } catch (IllegalArgumentException iae) {
                Assert.assertTrue(iae.getMessage().contains("Not allowed to specify injected command"));
            }
        }
    }

    @Test
    public void testPlatformZKEnable() {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("KYLIN_HOME", "/kylin");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");

        var driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));

        kylinConfig.setProperty("kylin.kerberos.enabled", "true");
        driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertTrue(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));

        kylinConfig.setProperty("kylin.env.zk-kerberos-enabled", "false");
        driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));
    }
}