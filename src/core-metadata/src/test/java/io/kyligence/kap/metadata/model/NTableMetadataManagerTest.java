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

import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.measure.hllc.HLLCounter;
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
        TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
        columnStats.setColumnName("test_col");
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

    @Test
    public void testGetTableExt() throws IOException {
        TableDesc tableDesc = mgrDefault.getTableDesc(tableKylinFact);

        final TableExtDesc t1 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(t1);

        final TableExtDesc t2 = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(t2);
        Assert.assertEquals(0, t2.getColumnStats().size());
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

    @Test
    public void testColumnStatsStore() throws IOException {
        TableDesc tableDesc = mgrDefault.getTableDesc(tableKylinFact);
        TableExtDesc tableExtDesc = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(tableExtDesc);
        tableExtDesc = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(tableExtDesc);
        val colStatsStore = NTableMetadataManager.ColumnStatsStore.getInstance(tableExtDesc, getTestConfig());

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        val colStats = new TableExtDesc.ColumnStats();
        colStats.setColumnName("col1");
        colStats.setNullCount(10);
        colStats.setMaxLength(20);
        val hllc = new HLLCounter(14);
        hllc.add(1);
        hllc.add(2);
        hllc.add(3);
        colStats.addRangeHLLC("0_1", hllc);
        val colStatsPath = new Path(KapConfig.wrap(getTestConfig()).getReadHdfsWorkingDirectory() + colStatsStore.getColumnStatsPath());
        val colStatsFS = HadoopUtil.getWorkingFileSystem();

        columnStatsList.add(colStats);
        tableExtDesc.setColumnStats(columnStatsList);

        // round 1
        Assert.assertFalse(colStatsFS.exists(colStatsPath));
        mgrDefault.saveTableExt(tableExtDesc);
        Assert.assertTrue(colStatsFS.exists(colStatsPath));
        Assert.assertEquals(1, colStatsFS.listStatus(colStatsPath).length);

        val actualExt = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(actualExt);
        val actualColStatsPath = actualExt.getColStatsPath();
        val actualFullColStatsPath = KapConfig.wrap(getTestConfig()).getReadHdfsWorkingDirectory() + actualColStatsPath;
        Assert.assertEquals(colStatsFS.listStatus(colStatsPath)[0].getPath().toString().replace("file:", "file://"), actualFullColStatsPath);
        Assert.assertEquals(1, actualExt.getColumnStats().size());
        val actualColStats = actualExt.getColumnStats().get(0);
        Assert.assertEquals("col1", actualColStats.getColumnName());
        Assert.assertEquals(10, actualColStats.getNullCount());
        Assert.assertEquals(20, actualColStats.getMaxLength().intValue());
        Assert.assertEquals(1, actualColStats.getRangeHLLC().size());
        Assert.assertTrue(actualColStats.getRangeHLLC().containsKey("0_1"));
        Assert.assertEquals(hllc.getCountEstimate(), actualColStats.getRangeHLLC().get("0_1").getCountEstimate());

        // round 2
        val tableExtDesc2 = mgrDefault.copyForWrite(actualExt);
        val colStats2 = tableExtDesc2.getColumnStats().get(0);
        colStats2.setNullCount(11);
        val hllc2 = new HLLCounter(14);
        hllc2.add(1);
        colStats2.addRangeHLLC("1_2", hllc2);
        mgrDefault.saveTableExt(tableExtDesc2);
        Assert.assertEquals(1, colStatsFS.listStatus(colStatsPath).length);
        val actualExt2 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(actualExt2);
        val actualColStatsPath2 = actualExt2.getColStatsPath();
        val actualFullColStatsPath2 = KapConfig.wrap(getTestConfig()).getReadHdfsWorkingDirectory() + actualColStatsPath2;
        Assert.assertTrue(StringUtils.isNotBlank(actualColStatsPath2));
        Assert.assertNotEquals(actualColStatsPath, actualColStatsPath2);
        Assert.assertEquals(colStatsFS.listStatus(colStatsPath)[0].getPath().toString().replace("file:", "file://"), actualFullColStatsPath2);
        Assert.assertEquals(1, actualExt2.getColumnStats().size());
        val actualColStats2 = actualExt2.getColumnStats().get(0);
        Assert.assertEquals("col1", actualColStats2.getColumnName());
        Assert.assertEquals(11, actualColStats2.getNullCount());
        Assert.assertEquals(20, actualColStats2.getMaxLength().intValue());
        Assert.assertEquals(2, actualColStats2.getRangeHLLC().size());
        Assert.assertTrue(actualColStats2.getRangeHLLC().containsKey("0_1"));
        Assert.assertTrue(actualColStats2.getRangeHLLC().containsKey("1_2"));
        Assert.assertEquals(hllc2.getCountEstimate(), actualColStats2.getRangeHLLC().get("1_2").getCountEstimate());

        // round 3, delete
        mgrDefault.removeTableExt(tableExtDesc.getIdentity());
        Assert.assertFalse(colStatsFS.exists(new Path(colStatsPath, "0_1")));
        Assert.assertFalse(colStatsFS.exists(new Path(colStatsPath, "1_2")));
        Assert.assertFalse(colStatsFS.exists(new Path(colStatsStore.getColumnStatsPath())));

        Assert.assertNull(mgrDefault.getTableExtIfExists(tableDesc));
    }
}
