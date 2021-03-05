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
package io.kyligence.kap.tool.obf;

import java.io.File;
import java.nio.file.Files;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.FileUtils;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import lombok.val;

public class KylinConfObfuscatorTest extends NLocalFileMetadataTestCase {

    public static void prepare() throws Exception {
        val confFile = new File(
                KylinConfObfuscatorTest.class.getClassLoader().getResource("obf/kylin.properties").getFile());
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        confPath.mkdirs();
        File kylinConf = new File(confPath, "kylin.properties");
        Files.copy(confFile.toPath(), kylinConf.toPath());
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testObfKylinConf() throws Exception {
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        confPath.mkdirs();
        File kylinConf = new File(confPath, "kylin.properties");
        File outputFile = new File(path, "output.json");
        ResultRecorder resultRecorder = new ResultRecorder();
        try (MappingRecorder mappingRecorder = new MappingRecorder(outputFile)) {
            FileObfuscator fileObfuscator = new KylinConfObfuscator(ObfLevel.OBF, mappingRecorder, resultRecorder);
            fileObfuscator.obfuscateFile(kylinConf);
        }
        val properties = FileUtils.readFromPropertiesFile(kylinConf);
        Assert.assertEquals(
                "zjz@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://10.1.3.16:3306/kylin,username=<hidden>,password=<hidden>,maxActive=20,maxIdle=20,test=",
                properties.get("kylin.metadata.url"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.env.zookeeper-connect-string"));
        Assert.assertEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.influxdb.password"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.influxdb.address"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.metrics.influx-rpc-service-bind-address"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("server.port"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.metadata.random-admin-password.enabled"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.security.user-password-encoder"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.job.notification-admin-emails"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.job.tracking-url-pattern"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.storage.columnar.spark-conf.spark.executor.cores"));

    }

    @Test
    public void testRawKylinConf() throws Exception {
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        File kylinConf = new File(confPath, "kylin.properties");
        File outputFile = new File(path, "output.json");
        ResultRecorder resultRecorder = new ResultRecorder();
        try (MappingRecorder mappingRecorder = new MappingRecorder(outputFile)) {
            FileObfuscator fileObfuscator = new KylinConfObfuscator(ObfLevel.RAW, mappingRecorder, resultRecorder);
            fileObfuscator.obfuscateFile(kylinConf);
        }
        val properties = FileUtils.readFromPropertiesFile(kylinConf);
        for (val kv : properties.entrySet()) {
            Assert.assertFalse(kv.getValue().contains(SensitiveConfigKeysConstant.HIDDEN));
        }
    }
}
