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
package io.kyligence.kap.engine.spark.source;

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkMetadataExplorerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testListDatabases() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.size() > 0);
    }

    @Test
    public void testListTables() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        List<String> tables = sparkMetadataExplorer.listTables("");
        Assert.assertTrue(tables != null && tables.size() > 0);
    }

    @Test
    public void testListTablesInDatabase() throws Exception {
        String testDataBase = "SSB";
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        sparkMetadataExplorer.createSampleDatabase(testDataBase);
        Assert.assertTrue(ss.catalog().databaseExists(testDataBase));

        TableDesc tableDesc = tableMgr.getTableDesc("SSB.PART");
        sparkMetadataExplorer.createSampleTable(tableDesc);

        List<String> tables = sparkMetadataExplorer.listTables(testDataBase);

        Assert.assertTrue(tables != null && tables.size() > 0);
        Assert.assertEquals(tables.get(0), "part");
    }

    @Test
    public void testGetTableDesc() throws Exception {
        populateSSWithCSVData(getTestConfig(), "ssb", ss);

        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "p_lineorder", "ssb");
        Assert.assertTrue(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst() != null);
        Assert.assertFalse(tableDescTableExtDescPair.getFirst().isTransactional());
        Assert.assertNotNull(tableDescTableExtDescPair.getFirst().isRangePartition());
        Assert.assertFalse(tableDescTableExtDescPair.getFirst().isRangePartition());
    }

    @Test
    public void testCreateSampleDatabase() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        sparkMetadataExplorer.createSampleDatabase("test");
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.contains("test"));
    }

    @Test
    public void testCheckPartitionTable() throws Exception {
        populateSSWithCSVData(getTestConfig(), "tdh", ss);

        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "CUSTOMER", "tdh");
        TableDesc fact1 = tableDescTableExtDescPair.getKey();
        Assert.assertEquals(Boolean.FALSE, fact1.isRangePartition());

        tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "DATES_RANGE", "tdh");
        TableDesc fact2 = tableDescTableExtDescPair.getFirst();
        Assert.assertEquals(Boolean.FALSE, fact2.isRangePartition());

        tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "DATES_TRANSACTION_RANGE", "tdh");
        TableDesc fact4 = tableDescTableExtDescPair.getKey();
        Assert.assertEquals(Boolean.FALSE, fact4.isRangePartition());

        tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "LINEORDER_PARTITION", "tdh");
        TableDesc fact5 = tableDescTableExtDescPair.getKey();
        Assert.assertEquals(Boolean.FALSE, fact5.isRangePartition());

        tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "LINEORDER_TRANSACTION", "tdh");
        TableDesc fact6 = tableDescTableExtDescPair.getKey();
        Assert.assertEquals(Boolean.FALSE, fact6.isRangePartition());

        tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "LINEORDER_TRANSACTION_PARTITION", "tdh");
        TableDesc fact7 = tableDescTableExtDescPair.getFirst();
        Assert.assertEquals(Boolean.FALSE, fact7.isRangePartition());
    }

    @Test
    public void testCreateSampleTable() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        TableDesc fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        sparkMetadataExplorer.createSampleTable(fact);
        List<String> tables = sparkMetadataExplorer.listTables("default");
        Assert.assertTrue(tables != null && tables.contains("test_kylin_fact"));
    }

    @Test
    public void testLoadData() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        sparkMetadataExplorer.loadSampleData("SSB.PART", TempMetadataBuilder.TEMP_TEST_METADATA + "/data/");
        List<Row> rows = ss.sql("select * from part").collectAsList();
        Assert.assertTrue(rows != null && rows.size() > 0);
    }

    @Test
    public void testGetLoc() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        TableDesc fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        sparkMetadataExplorer.createSampleTable(fact);
        SparkSession sparkSession = Mockito.mock(SparkSession.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(sparkSession.sql("desc formatted " + "TEST_KYLIN_FACT").where("col_name == 'Location'").head()
                .getString(1)).thenReturn(
                        "hdfs://hacluster//KAP/src/spark-project/examples/test_data/27578/spark-warehouse/test_kylin_fact");
        Assert.assertTrue(
                sparkMetadataExplorer.getLoc(sparkSession, "TEST_KYLIN_FACT", null).contains("hdfs://hacluster"));
        Assert.assertTrue(sparkMetadataExplorer.getLoc(sparkSession, "TEST_KYLIN_FACT", "hdfs://writecluster")
                .contains("hdfs://writecluster"));
        Mockito.when(sparkSession.sql("desc formatted " + "TEST_KYLIN_FACT").where("col_name == 'Location'").head()
                .getString(1)).thenReturn(null);
        Assert.assertNull(sparkMetadataExplorer.getLoc(sparkSession, "TEST_KYLIN_FACT", "hdfs://writecluster"));
    }

    @Test
    public void testCheckTableAccess() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        TableDesc fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        sparkMetadataExplorer.createSampleTable(fact);
        Assert.assertTrue(sparkMetadataExplorer.checkTableAccess("DEFAULT.TEST_KYLIN_FACT"));
        SparderEnv.getSparkSession().sessionState().conf().setConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION(),
                "hdfs://writecluster");
        Assert.assertFalse(sparkMetadataExplorer.checkTableAccess("DEFAULT.TEST_KYLIN_FACT"));
        SparderEnv.getSparkSession().sessionState().conf().unsetConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION());
    }
}
