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
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;

public class NSparkMetadataExplorerTest extends NLocalSparkWithCSVDataTest {
    @Test
    public void testListDatabases() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.size() > 0);
    }

    @Test
    public void testListTables() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        List<String> tables = sparkMetadataExplorer.listTables("");
        Assert.assertTrue(tables != null && tables.size() > 0);
    }

    @Test
    public void testGetTableDesc() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("", "p_lineorder", "ssb");
        Assert.assertTrue(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst() != null);
    }

    @Test
    public void testCreateSampleDatabase() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        sparkMetadataExplorer.createSampleDatabase("test");
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.contains("test"));
    }

    @Test
    public void testCreateSampleTable() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        TableMetadataManager tableMgr = TableMetadataManager.getInstance(getTestConfig());
        TableDesc fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT", "default");
        sparkMetadataExplorer.createSampleTable(fact);
        List<String> tables = sparkMetadataExplorer.listTables("default");
        Assert.assertTrue(tables != null && tables.contains("test_kylin_fact"));
    }

    @Test
    public void testLoadData() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer(ss);
        sparkMetadataExplorer.loadSampleData("SSB.PART", "../examples/test_metadata/data/");
        List<Row> rows = ss.sql("select * from part").collectAsList();
        Assert.assertTrue(rows != null && rows.size() > 0);
    }
}
