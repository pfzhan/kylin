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

import java.io.IOException;
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
    private String projectDefault = "default";
    private String tableKylinFact = "DEFAULT.TEST_KYLIN_FACT";
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
    public void testListAllTables() throws Exception {
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
    public void testFindTableByName() throws Exception {
        TableDesc table = mgrDefault.getTableDesc("EDW.TEST_CAL_DT");
        Assert.assertNotNull(table);
        Assert.assertEquals("EDW.TEST_CAL_DT", table.getIdentity());
    }

    @Test
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(mgrDefault);
        Assert.assertNotNull(mgrDefault.listAllTables());
        Assert.assertTrue(mgrDefault.listAllTables().size() > 0);
    }

    @Test
    public void testTableSample() throws IOException {
        TableExtDesc tableExtDesc = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc);

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        TableExtDesc.ColumnStats columnStats = new NTableExtDesc.ColumnStats();
        columnStats.setColumnSamples("Max", "Min", "dfadsfdsfdsafds", "d");
        columnStatsList.add(columnStats);
        tableExtDesc.setColumnStats(columnStatsList);
        mgrDefault.saveTableExt(tableExtDesc);

        TableExtDesc tableExtDesc1 = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc1);

        List<TableExtDesc.ColumnStats> columnStatsList1 = tableExtDesc1.getColumnStats();
        Assert.assertEquals(1, columnStatsList1.size());

        mgrDefault.removeTableExt(tableKylinFact);
    }
}
