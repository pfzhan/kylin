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
package io.kyligence.kap.source.jdbc;

import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class JdbcExplorerTest extends JdbcTestBase {
    private static JdbcExplorer explorer;
    private static JdbcSource jdbcSource;

    @BeforeClass
    public static void setUp() throws SQLException {
        JdbcTestBase.setUp();
        jdbcSource = new JdbcSource(getTestConfig());
        explorer = (JdbcExplorer) jdbcSource.getSourceMetadataExplorer();
    }

    @Test
    public void testListDatabases() throws Exception {
        List<String> databases = explorer.listDatabases();
        Assert.assertTrue(databases != null && databases.size() > 0);
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tables = explorer.listTables("SSB");
        Assert.assertTrue(tables != null && tables.size() > 0);
    }

    @Test
    public void testListTablesInDatabase() throws Exception {
        String testDataBase = "SSB";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        explorer.createSampleDatabase(testDataBase);
        Assert.assertTrue(explorer.listDatabases().contains(testDataBase));

        connector.executeUpdate("drop table if exists SSB.PART");
        Assert.assertFalse(explorer.listTables(testDataBase).contains("PART"));

        TableDesc tableDesc = tableMgr.getTableDesc("SSB.PART");
        explorer.createSampleTable(tableDesc);
        Assert.assertTrue(explorer.listTables(testDataBase).contains("PART"));
    }

    @Test
    public void testGetTableDesc() throws Exception {
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = explorer.loadTableMetadata("SSB", "LINEORDER", "ssb");
        Assert.assertTrue(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst() != null);
    }

    @Test
    public void testCreateSampleDatabase() throws Exception {
        explorer.createSampleDatabase("TEST");
        List<String> databases = explorer.listDatabases();
        Assert.assertTrue(databases != null && databases.contains("TEST"));
    }

}
