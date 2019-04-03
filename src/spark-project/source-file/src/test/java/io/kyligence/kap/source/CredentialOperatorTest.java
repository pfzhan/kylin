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
package io.kyligence.kap.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.source.file.CredentialOperator;
import io.kyligence.kap.source.file.LocalFileCredentialOperator;
import io.kyligence.kap.source.file.S3KeyCredential;
import io.kyligence.kap.source.file.S3KeyCredentialOperator;
import org.apache.hadoop.util.Shell;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CredentialOperatorTest {

    protected static SparkConf sparkConf;
    protected static SparkSession ss;
    private LocalFileCredentialOperator localFileCredentialOperator;
    private S3KeyCredential s3KeyCredential;
    private S3KeyCredentialOperator s3KeyCredentialOperator;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();

        SparderEnv.setSparkSession(ss);

    }

    @AfterClass
    public static void afterClass() {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        ss.close();
    }

    @Before
    public void setup() {
        localFileCredentialOperator = new LocalFileCredentialOperator();
        s3KeyCredential = new S3KeyCredential();
        s3KeyCredential.setAccessKey("mockAccessKey");
        s3KeyCredential.setSecretKey("mockSecretKey");
        s3KeyCredentialOperator = new S3KeyCredentialOperator();
        s3KeyCredentialOperator.setCredential(s3KeyCredential);
    }

    @Test
    public void testCsvSamples() {
        String[] firstRowArray = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        String url = "../../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_COUNTRY.csv";
        Map<String, String> map = Maps.newHashMap();
        map.put("delimiter", ",");
        String[][] csvSamples = localFileCredentialOperator.csvSamples(url, map);
        Assert.assertEquals(10, csvSamples.length);
        Assert.assertArrayEquals(firstRowArray, csvSamples[0]);
    }

    @Test
    public void testCsvSchema() {
        List<String> expectedSchema = Lists.newArrayList("string", "double", "double", "string");
        String url = "../../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_COUNTRY.csv";
        Map<String, String> map = Maps.newHashMap();
        map.put("delimiter", ",");
        List<String> csvSchema = localFileCredentialOperator.csvSchema(url, map);
        Assert.assertEquals(expectedSchema, csvSchema);
    }

    @Test
    public void testChooseCredentialClassName() {
        Assert.assertEquals("io.kyligence.kap.source.file.S3KeyCredentialOperator",
                CredentialOperator.chooseCredentialClassName("AWS_S3_KEY"));
        Assert.assertEquals("io.kyligence.kap.source.file.LocalFileCredentialOperator",
                CredentialOperator.chooseCredentialClassName("LOCAL"));
        Assert.assertNull(CredentialOperator.chooseCredentialClassName("OTHER"));

    }

    @Test
    public void testChooseCredential() {
        Assert.assertTrue(CredentialOperator.chooseCredential("AWS_S3_KEY") instanceof S3KeyCredentialOperator);
        Assert.assertTrue(CredentialOperator.chooseCredential("LOCAL") instanceof LocalFileCredentialOperator);
        Assert.assertNull(CredentialOperator.chooseCredential("OTHER"));

    }

    @Test
    public void testS3Decode() {
        String mockS3Credential = "{\"type\":\"AWS_S3_KEY\",\"accessKey\":\"mockAccessKey\",\"secretKey\":\"mockSecretKey\"}";
        S3KeyCredentialOperator s3KeyCredentialOperator = new S3KeyCredentialOperator();
        CredentialOperator credentialOperator = s3KeyCredentialOperator.decode(mockS3Credential);
        S3KeyCredential credential = (S3KeyCredential) credentialOperator.getCredential();
        Assert.assertEquals("mockAccessKey", credential.getAccessKey());
        Assert.assertEquals("mockSecretKey", credential.getSecretKey());
        Assert.assertEquals("AWS_S3_KEY", credential.getType());

        mockS3Credential = "{\"type\":\"AWS_S3_KEY\",\"accessKey\":\"mockAccessKey\",\"secretKey\":\"mockSecretKey\"";
        thrown.expect(IllegalStateException.class);
        s3KeyCredentialOperator.decode(mockS3Credential);
    }

    @Test
    public void testS3Encode() {
        Assert.assertEquals("{\"accessKey\":\"mockAccessKey\",\"secretKey\":\"mockSecretKey\",\"type\":\"AWS_S3_KEY\"}",
                s3KeyCredentialOperator.encode());
    }

    @Test
    public void testS3Build() {
        s3KeyCredentialOperator.build(ss);
        Assert.assertEquals("mockAccessKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.access.key"));
        Assert.assertEquals("mockSecretKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.secret.key"));
    }

    @Test
    public void testS3BuildConf() {
        s3KeyCredentialOperator.buildConf(sparkConf);
        Assert.assertEquals("mockAccessKey", sparkConf.get("spark.hadoop.fs.s3a.access.key"));
        Assert.assertEquals("mockSecretKey", sparkConf.get("spark.hadoop.fs.s3a.secret.key"));
    }

    @Test
    public void testS3MassageUrl() {
        Assert.assertEquals("s3a://kyligence/sales", s3KeyCredentialOperator.massageUrl("s3://kyligence/sales"));
        Assert.assertEquals("file://dir/file", localFileCredentialOperator.massageUrl("file://dir/file"));
    }

    @Test
    public void testPrepare() {
        String credentialString = "{\"type\":\"AWS_S3_KEY\",\"accessKey\":\"mockAccessKey\",\"secretKey\":\"mockSecretKey\"}";
        s3KeyCredentialOperator.prepare(credentialString, ss);
        Assert.assertEquals("mockAccessKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.access.key"));
        Assert.assertEquals("mockSecretKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.secret.key"));

    }
}
