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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DDLDesc;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.DDLRequest;
import io.kyligence.kap.rest.response.DDLResponse;

public class SparkSourceServiceTest extends NLocalFileMetadataTestCase {

    protected static SparkSession ss;
    private NProjectManager projectManager;
    @InjectMocks
    private final SparkSourceService sparkSourceService = Mockito.spy(new SparkSourceService());
    private TestingServer zkTestServer;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").enableHiveSupport().getOrCreate();
        ss.sparkContext().hadoopConfiguration().set("javax.jdo.option.ConnectionURL",
                "jdbc:derby:memory:db;create=true");
        SparderEnv.setSparkSession(ss);
        createTestMetadata();
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("use default;create external table COUNTRY(COUNTRY string,LATITUDE double,"
                + "LONGITUDE double,NAME string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
                + "with serdeproperties(\"separatorChar\" = \",\", \"quoteChar\" = \"\\\"\") location "
                + "'../examples/test_case_data/localmeta/data'");
        sparkSourceService.executeSQL(ddlRequest);
        zkTestServer = new TestingServer(true);
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kap.env.zookeeper-max-retries", "1");
        overwriteSystemProp("kap.env.zookeeper-base-sleep-time", "1000");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        ss.stop();
    }

    @Test
    public void testExecuteSQL() {
        String sql = "show databases";
        DDLDesc ddlDesc = new DDLDesc("show databases", null, null, DDLDesc.DDLType.NONE);
        Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
    }

    @Test
    public void testExecuteSQL2() {
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("show databases;");
        DDLResponse ddlResponse = new DDLResponse();
        Map<String, DDLDesc> succeed = Maps.newHashMap();
        Map<String, String> failed = Maps.newHashMap();
        DDLDesc ddlDesc = new DDLDesc("show databases", null, null, DDLDesc.DDLType.NONE);
        succeed.put("show databases", ddlDesc);
        ddlResponse.setSucceed(succeed);
        ddlResponse.setFailed(failed);
        Assert.assertEquals(ddlResponse, sparkSourceService.executeSQL(ddlRequest));
    }

    @Test
    public void testGetTableDesc() {
        try {
            sparkSourceService.getTableDesc("default", "COUNTRY");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testListDatabase() {
        List<String> result = new ArrayList<>();
        result.add("DEFAULT");
        Assert.assertEquals(result, sparkSourceService.listDatabase());

    }

    @Test
    public void testListTables() throws Exception {
        Assert.assertEquals(11, sparkSourceService.listTables("DEFAULT", "default").size());
    }

    @Test
    public void testDatabaseExists() {
        Assert.assertEquals(true, sparkSourceService.databaseExists("default"));
    }

    @Test
    public void testDropTable() throws AnalysisException {
        sparkSourceService.dropTable("default", "COUNTRY");
        Assert.assertEquals(false, sparkSourceService.tableExists("default", "COUNTRY"));
    }

    @Test
    public void testListColumns() {
        Assert.assertEquals(4, sparkSourceService.listColumns("default", "COUNTRY").size());

    }

    @Test
    public void testExportTables() {
        String expectedTableStructure = "CREATE EXTERNAL TABLE `default`.`hive_bigints`(   `id` BIGINT) "
            + "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
            + "WITH SERDEPROPERTIES (   'serialization.format' = '1') STORED AS   "
            + "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'   "
            + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
            + "LOCATION 'file:/tmp/parquet_data' ";
        sparkSourceService.executeSQL(
                "CREATE EXTERNAL TABLE hive_bigints(id bigint)  STORED AS PARQUET LOCATION '/tmp/parquet_data'");

        String actureTableStructure = sparkSourceService.exportTables("default", new String[] { "hive_bigints" })
                .getTables().get("hive_bigints");

        Assert.assertEquals(actureTableStructure.substring(0, actureTableStructure.lastIndexOf("TBLPROPERTIES")),
                expectedTableStructure);
    }

    @Test
    public void testLoadSamples() throws Exception {
        Assert.assertEquals(9, sparkSourceService.loadSamples(ss, SaveMode.Overwrite).size());
        // KC-6666, table not exists but table location exists
        // re-create spark context and re-load samples
        {
            ss.stop();
            ss = SparkSession.builder().appName("local").master("local[1]").enableHiveSupport().getOrCreate();
            ss.sparkContext().hadoopConfiguration().set("javax.jdo.option.ConnectionURL",
                    "jdbc:derby:memory:db;create=true");
        }
        Assert.assertEquals(9, sparkSourceService.loadSamples(ss, SaveMode.Overwrite).size());
        FileUtils.deleteDirectory(new File("spark-warehouse"));
    }

    @Test
    public void testLoadSamples2() throws Exception {
        Assert.assertEquals(9, sparkSourceService.loadSamples().size());
        FileUtils.deleteDirectory(new File("spark-warehouse"));
    }

    @Test
    public void testTableExists() throws IOException {
        Assert.assertTrue(sparkSourceService.tableExists("default", "COUNTRY"));
    }
}
