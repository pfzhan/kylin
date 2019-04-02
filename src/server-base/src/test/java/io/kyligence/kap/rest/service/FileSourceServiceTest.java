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
package io.kyligence.kap.rest.service;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.CSVRequest;
import io.kyligence.kap.source.file.S3KeyCredential;
import io.kyligence.kap.source.file.S3KeyCredentialOperator;
import io.kyligence.kap.spark.common.CredentialUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class FileSourceServiceTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private FileSourceService fileSourceService = Mockito.spy(new FileSourceService());

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
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
        staticCreateTestMetadata();
        ReflectionTestUtils.setField(fileSourceService, "projectService", projectService);
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    public CSVRequest mockCsvRequest() {
        CSVRequest csvRequest = new CSVRequest();
        csvRequest.setUrl("../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_COUNTRY.csv");
        csvRequest.setType("LOCAL");
        return csvRequest;
    }

    @Test
    public void testCsvSamples() {
        CSVRequest csvRequest = mockCsvRequest();
        csvRequest.setSeparatorChar(",");
        String[][] csvSamples = fileSourceService.csvSamples(csvRequest);
        Assert.assertEquals(10, csvSamples.length);
        String[] firstRowArray = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        Assert.assertArrayEquals(firstRowArray, csvSamples[0]);

        firstRowArray = new String[] { "AD,42.546245,1.601554,Andorra" };
        csvRequest.setSeparatorChar("\t");
        csvSamples = fileSourceService.csvSamples(csvRequest);
        Assert.assertArrayEquals(firstRowArray, csvSamples[0]);

    }

    @Test
    public void testCsvSchema() {
        CSVRequest csvRequest = mockCsvRequest();
        csvRequest.setSeparatorChar(",");
        List<String> expectedSchema = Lists.newArrayList("string", "double", "double", "string");
        Assert.assertEquals(expectedSchema, fileSourceService.csvSchema(csvRequest));

        csvRequest.setSeparatorChar("\t");
        expectedSchema = Lists.newArrayList("string");
        Assert.assertEquals(expectedSchema, fileSourceService.csvSchema(csvRequest));

    }

    @Test
    public void testValidateSql() throws Exception {
        Assert.assertTrue(fileSourceService.validateSql("create table t(a int,b string)"));
        thrown.expect(Exception.class);
        Assert.assertFalse(fileSourceService.validateSql("create tables t(a int,b string)"));
        thrown.expect(Exception.class);
        Assert.assertFalse(fileSourceService.validateSql("create table t(a int,b varchar)"));
    }

    @Test
    public void testVerifyCredential() {
        CSVRequest csvRequest = mockCsvRequest();
        boolean success = fileSourceService.verifyCredential(csvRequest);
        Assert.assertTrue(success);
    }

    @Test
    public void testEscape() {
        String s = "\"s\"";
        Assert.assertEquals("\\\"s\\\"", fileSourceService.escape(s));
    }

    @Test
    public void testGenerateCSVSql() throws IOException {
        String tableData = "{\"name\":\"COUNTRY\",\"database\":\"SSB\",\"source_type\":13,"
                + "\"columns\":[{\"id\":1,\"name\":\"COUNTRY\",\"datatype\":\"string\",\"comment\":\"\"},{\"id\":2,\"name\":\"LATITUDE\",\"datatype\":\"double\",\"comment\":\"\"},"
                + "{\"id\":3,\"name\":\"LONGITUDE\",\"datatype\":\"double\",\"comment\":\"\"},{\"id\":4,\"name\":\"NAME\",\"datatype\":\"string\",\"comment\":\"\"}]}";
        TableDesc tableDesc = JsonUtil.readValue(tableData, TableDesc.class);
        String ddl = fileSourceService.generateCSVSql(tableDesc, mockCsvRequest());
        Assert.assertEquals("create external table SSB.COUNTRY(COUNTRY string,LATITUDE double,"
                + "LONGITUDE double,NAME string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
                + "with serdeproperties(\"separatorChar\" = \",\", \"quoteChar\" = \"\\\"\") location "
                + "'../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_COUNTRY.csv'", ddl);
    }

    @Test
    public void testFindTableName() {
        Assert.assertEquals("T1", fileSourceService.findTableName("create external table t1(a string,b int);"));
        Assert.assertNull(fileSourceService.findTableName("use default;"));
    }

    @Test
    public void testWrapCredential() {
        String project = "default";
        prepareProjectWithCredential(project);
        CredentialUtils.wrap(ss, project);
        Assert.assertEquals("mockAccessKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.access.key"));
        Assert.assertEquals("mockSecretKey", ss.sparkContext().hadoopConfiguration().get("fs.s3a.secret.key"));

        CredentialUtils.wrap(sparkConf, project);
        Assert.assertEquals("mockAccessKey", sparkConf.get("spark.hadoop.fs.s3a.access.key"));
        Assert.assertEquals("mockSecretKey", sparkConf.get("spark.hadoop.fs.s3a.secret.key"));
    }

    private void prepareProjectWithCredential(String project) {
        S3KeyCredentialOperator operator = new S3KeyCredentialOperator();
        S3KeyCredential s3KeyCredential = new S3KeyCredential();
        s3KeyCredential.setAccessKey("mockAccessKey");
        s3KeyCredential.setSecretKey("mockSecretKey");
        operator.setCredential(s3KeyCredential);
        projectService.updateFileSourceCredential(project, operator);
    }

}
