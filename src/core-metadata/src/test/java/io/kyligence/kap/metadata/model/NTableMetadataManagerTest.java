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

package io.kyligence.kap.metadata.model;

import static io.kyligence.kap.metadata.model.NTableMetadataManager.getInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

/**
 */
public class NTableMetadataManagerTest extends NLocalFileMetadataTestCase {
    private final String projectDefault = "default";
    private final String tableKylinFact = "DEFAULT.TEST_KYLIN_FACT";
    private NTableMetadataManager mgrDefault;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        mgrDefault = getInstance(getTestConfig(), projectDefault);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testListAllTables() {
        List<TableDesc> tables = mgrDefault.listAllTables();
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.size() > 0);
    }

    @Test
    public void testGetAllTablesMap() {
        Map<String, TableDesc> tm = mgrDefault.getAllTablesMap();
        Assert.assertTrue(tm.size() > 0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", tm.get("DEFAULT.TEST_KYLIN_FACT").getIdentity());
    }

    @Test
    public void testGetTableDesc() {
        TableDesc tbl = mgrDefault.getTableDesc(tableKylinFact);
        Assert.assertEquals(tableKylinFact, tbl.getIdentity());
    }

    @Test
    public void testFindTableByName() {
        TableDesc table = mgrDefault.getTableDesc("EDW.TEST_CAL_DT");
        Assert.assertNotNull(table);
        Assert.assertEquals("EDW.TEST_CAL_DT", table.getIdentity());
    }

    @Test
    public void testGetInstance() {
        Assert.assertNotNull(mgrDefault);
        Assert.assertNotNull(mgrDefault.listAllTables());
        Assert.assertTrue(mgrDefault.listAllTables().size() > 0);
    }

    @Test
    public void testTableSample() {
        TableExtDesc tableExtDesc = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc);

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
        columnStats.setColumnName("test_col");
        columnStats.setColumnSamples("Max", "Min", "dfadsfdsfdsafds", "d");
        columnStatsList.add(columnStats);
        tableExtDesc.setColumnStats(columnStatsList);
        mgrDefault.saveTableExt(tableExtDesc);

        TableExtDesc tableExtDesc1 = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc1);
        Assert.assertEquals(1, tableExtDesc1.getAllColumnStats().size());
        mgrDefault.removeTableExt(tableKylinFact);
    }

    @Test
    public void testGetTableExt() {
        TableDesc tableDesc = mgrDefault.getTableDesc(tableKylinFact);

        final TableExtDesc t1 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(t1);

        final TableExtDesc t2 = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(t2);
        Assert.assertEquals(0, t2.getAllColumnStats().size());
        Assert.assertEquals(0, t2.getTotalRows());

        final TableExtDesc t3 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(t3);

        t2.setTotalRows(100);
        mgrDefault.saveTableExt(t2);

        final TableExtDesc t4 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(t4);
        Assert.assertEquals(100, t4.getTotalRows());

        final TableExtDesc t5 = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(t5);
        Assert.assertEquals(100, t5.getTotalRows());

    }

    @Test
    public void testGetIncrementalLoadTables() {
        TableDesc tableDesc = mgrDefault.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableDesc.setIncrementLoading(true);
        List<TableDesc> tables = mgrDefault.getAllIncrementalLoadTables();
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.get(0).isIncrementLoading());
    }

}