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
package io.kyligence.kap.streaming.jobs.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import lombok.val;

public class StreamingJobLauncherTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBuildJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        Assert.assertTrue(!launcher.isInitialized());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        Assert.assertTrue(launcher.isInitialized());
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(4, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_DURATION_DEFAULT, appArgs[2]);
        Assert.assertEquals("", appArgs[3]);
    }

    @Test
    public void testMergeJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_MERGE_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(4, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT, appArgs[2]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT, appArgs[3]);
    }

    @Test
    public void testStop() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);

        launcher.stop();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val uuid = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        val meta = mgr.getStreamingJobByUuid(uuid);
        Assert.assertEquals(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN, meta.getAction());
    }

    @Test
    public void testStartBuildJob() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        launcher.startYarnJob();
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.keytab"));
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.principal"));
    }

    @Test
    public void testStartMergeJob() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        launcher.startYarnJob();
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.keytab"));
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.principal"));
    }

    @Test
    public void testStartYarnBuildJob() throws Exception {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.tool.mount-spark-log-dir", ".");
        val kapConfig = KapConfig.getInstanceFromEnv();

        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.kafka-jaas.enabled", "true");
        config.setProperty("kylin.streaming.spark-conf.spark.driver.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        config.setProperty("kylin.streaming.spark-conf.spark.executor.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        config.setProperty("kylin.streaming.spark-conf.spark.am.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        launcher.startYarnJob();
        Assert.assertNotNull(mockup.sparkConf.get("spark.driver.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.executor.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.am.extraJavaOptions"));
    }

    @Test
    public void testStartYarnMergeJob() throws Exception {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        try {
            config.setProperty("kylin.kerberos.enabled", "true");
            config.setProperty("kylin.tool.mount-spark-log-dir", ".");
            val kapConfig = KapConfig.getInstanceFromEnv();
            config.setProperty("kylin.kafka-jaas.enabled", "true");
            FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                    "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required}");

            val mockup = new MockupSparkLauncher();
            ReflectionUtils.setField(launcher, "launcher", mockup);
            launcher.startYarnJob();
            Assert.assertNotNull(mockup.sparkConf.get("spark.kerberos.keytab"));
            Assert.assertNotNull(mockup.sparkConf.get("spark.kerberos.principal"));
            Assert.assertTrue(mockup.files.contains(kapConfig.getKafkaJaasConfPath()));
        } finally {
            FileUtils.deleteQuietly(new File(KapConfig.getInstanceFromEnv().getKafkaJaasConfPath()));
        }
    }

    @Test
    public void testLaunchMergeJobException() {
        try {
            overwriteSystemProp("streaming.local", "true");
            val config = getTestConfig();

            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
            config.setProperty("kylin.env", "local");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("KE-010035005", ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testLaunchBuildJobException() {
        try {
            overwriteSystemProp("streaming.local", "true");
            val config = getTestConfig();
            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
            config.setProperty("kylin.env", "local");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("KE-010035005", ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    static class MockupSparkLauncher extends SparkLauncher {
        private Map<String, String> sparkConf;
        private List<String> files;

        public SparkAppHandle startApplication(SparkAppHandle.Listener... listeners) throws IOException {
            val builder = ReflectionUtils.getField(this, "builder");
            sparkConf = (Map) ReflectionUtils.getField(builder, "conf");
            files = (List<String>) ReflectionUtils.getField(builder, "files");
            return null;
        }
    }
}
