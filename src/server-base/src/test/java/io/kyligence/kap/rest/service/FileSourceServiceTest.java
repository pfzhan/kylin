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
import io.kyligence.kap.source.file.ICredential;
import org.apache.hadoop.util.Shell;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;

public class FileSourceServiceTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private FileSourceService fileSourceService = Mockito.spy(new FileSourceService());

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

    public CSVRequest mockCsvRequest() {
        CSVRequest csvRequest = new CSVRequest();
        csvRequest.setUrl("../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_COUNTRY.csv");
        csvRequest.setType(ICredential.LOCAL);
        return csvRequest;
    }

    @Test
    public void testCsvSamples() throws Exception {
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
    public void testCsvSchema() throws Exception {
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
    public void testVerifyCredential() throws Exception {
        CSVRequest csvRequest = mockCsvRequest();
        boolean success = fileSourceService.verifyCredential(csvRequest);
        Assert.assertTrue(success);
    }

}
