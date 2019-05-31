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

package io.kyligence.kap.engine.spark.stats.analyzer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;

public class TableAnalyzerTest extends NLocalWithSparkSessionTest {

    private NTableMetadataManager tableMgr;

    @Before
    public void setup() {
        getTestConfig().setProperty("kylin.engine.spark-conf.spark.executor.instances", "1");
        getTestConfig().setProperty("kylin.engine.spark-conf.spark.executor.cores", "1");
        tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
    }

    @Test
    public void testAnalyze() {
        TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        Dataset<Row> sourceData = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, Maps.newHashMap());

        final List<Row> rows = sourceData.collectAsList();

        final TableAnalyzer tableAnalyzer = new TableAnalyzer(tableDesc);
        for (final Row row : rows) {
            tableAnalyzer.analyze(row);
        }

        final TableAnalyzerResult result = tableAnalyzer.getResult();
        Assert.assertEquals(10000, result.getRowCount());

        // column "TRANS_ID"
        int TRANS_ID = 0;
        {
            Assert.assertEquals(0, result.getNullOrBlankCount(TRANS_ID));
            Assert.assertEquals(4, result.getMaxLength(TRANS_ID));
            Assert.assertEquals("1000", result.getMaxLengthValue(TRANS_ID));
            Assert.assertEquals(1, result.getMinLength(TRANS_ID));
            Assert.assertEquals("0", result.getMinLengthValue(TRANS_ID));
            Assert.assertEquals(9999.0d, result.getMaxNumeral(TRANS_ID), 0.0001d);
            Assert.assertEquals(0.0, result.getMinNumeral(TRANS_ID), 0.0001d);
            Assert.assertEquals(9958, result.getCardinality(TRANS_ID));
        }

        // column "CAL_DT"
        int CAL_DT = 2;
        {
            Assert.assertEquals(0, result.getNullOrBlankCount(CAL_DT));
            Assert.assertEquals(10, result.getMaxLength(CAL_DT));
            Assert.assertEquals("2012-01-01", result.getMaxLengthValue(CAL_DT));
            Assert.assertEquals(10, result.getMinLength(CAL_DT));
            Assert.assertEquals("2012-01-01", result.getMinLengthValue(CAL_DT));
            Assert.assertEquals(734, result.getCardinality(CAL_DT));
        }

        // column "LSTG_FORMAT_NAME"
        int LSTG_FORMAT_NAME = 3;
        {
            Assert.assertEquals(0, result.getNullOrBlankCount(LSTG_FORMAT_NAME));
            Assert.assertEquals(10, result.getMaxLength(LSTG_FORMAT_NAME));
            Assert.assertEquals("FP-non GTC", result.getMaxLengthValue(LSTG_FORMAT_NAME));
            Assert.assertEquals(4, result.getMinLength(LSTG_FORMAT_NAME));
            Assert.assertEquals("ABIN", result.getMinLengthValue(LSTG_FORMAT_NAME));
            Assert.assertEquals(5, result.getCardinality(LSTG_FORMAT_NAME));
        }

        // column "PRICE"
        int PRICE = 8;
        {
            Assert.assertEquals(0, result.getNullOrBlankCount(PRICE));
            Assert.assertEquals(8, result.getMaxLength(PRICE));
            Assert.assertEquals("290.4800", result.getMaxLengthValue(PRICE));
            Assert.assertEquals(6, result.getMinLength(PRICE));
            Assert.assertEquals("9.1000", result.getMinLengthValue(PRICE));
            Assert.assertEquals(999.84d, result.getMaxNumeral(PRICE), 0.0001d);
            Assert.assertEquals(-99.79, result.getMinNumeral(PRICE), 0.0001d);
            Assert.assertEquals(9443, result.getCardinality(PRICE));
        }
    }

    @Test
    public void testSampleFullTable() {
        TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        new TableAnalyzerJob().analyzeTable(tableDesc, getProject(), 20_000_000, getTestConfig(), ss);
        val tableExt = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertEquals(10, tableExt.getSampleRows().size());

        {
            var result = tableExt.getColumnStatsByName("TRANS_ID");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(9571, result.getCardinality());
            Assert.assertEquals("9999", result.getMaxValue());
            Assert.assertEquals("0", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("CAL_DT");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(722, result.getCardinality());
            Assert.assertEquals("2014-01-01", result.getMaxValue());
            Assert.assertEquals("2012-01-01", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("LSTG_FORMAT_NAME");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(5, result.getCardinality());
            Assert.assertEquals("Others", result.getMaxValue());
            Assert.assertEquals("ABIN", result.getMinValue());
        }

        {
            val result = tableExt.getColumnStatsByName("PRICE");
            Assert.assertEquals(0, result.getNullCount());
            Assert.assertEquals(8787, result.getCardinality());
            Assert.assertEquals("999.8400", result.getMaxValue());
            Assert.assertEquals("-99.7900", result.getMinValue());
        }
    }

    @Test
    public void testSampleTableForColumnOrRowAlwaysNull() {
        // case 1: this case test specified column always null, corresponding column is 'CATEG_BUSN_MGR'
        TableDesc testCategoryGroupings = tableMgr.getTableDesc("DEFAULT.TEST_CATEGORY_GROUPINGS");
        final ColumnDesc categBusnMgr = Arrays.stream(testCategoryGroupings.getColumns())
                .filter(columnDesc -> columnDesc.getName().equalsIgnoreCase("CATEG_BUSN_MGR"))
                .collect(Collectors.toList()).get(0);
        new TableAnalyzerJob().analyzeTable(testCategoryGroupings, getProject(), 10000, getTestConfig(), ss);
        val tableExt = tableMgr.getTableExtIfExists(testCategoryGroupings);
        final TableExtDesc.ColumnStats columnStats = tableExt.getColumnStatsByName("CATEG_BUSN_MGR");
        Assert.assertEquals(categBusnMgr.getName(), columnStats.getColumnName());
        Assert.assertNull(columnStats.getMaxValue());
        Assert.assertNull(columnStats.getMinValue());
        Assert.assertEquals(144, columnStats.getNullCount());
        Assert.assertEquals(0, columnStats.getCardinality());

        // case 2: this case test sample data has a line always null in each column
        TableDesc testEncodings = tableMgr.getTableDesc("DEFAULT.TEST_ENCODING");
        new TableAnalyzerJob().analyzeTable(testEncodings, getProject(), 10000, getTestConfig(), ss);
        final TableExtDesc testEncodingsExt = tableMgr.getTableExtIfExists(testEncodings);
        final List<String[]> sampleRows = testEncodingsExt.getSampleRows();
        final String[] rowValue = sampleRows.get(sampleRows.size() - 1);
        Arrays.stream(rowValue).forEach(Assert::assertNull);
    }

    @Test
    public void testSamplingTaskRunningFailedForTableNotExistAnyMore() {
        String tableIdentity = "DEFAULT.NOT_EXIST_TABLE";
        Assert.assertNull(tableMgr.getTableDesc(tableIdentity));

        // mock a table desc without corresponding data exist
        final TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_COUNTRY");
        final TableDesc notExistTableDesc = tableMgr.copyForWrite(tableDesc);
        notExistTableDesc.setName(tableIdentity);

        try {
            new TableAnalyzerJob().analyzeTable(notExistTableDesc, getProject(), 10000, getTestConfig(), ss);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().startsWith("Path does not exist:")
                    && e.getMessage().endsWith("/test_metadata/data/DEFAULT.NOT_EXIST_TABLE.csv;"));
        }
    }

    @Test
    public void testSamplePartTable() {
        TableDesc tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        new TableAnalyzerJob().analyzeTable(tableDesc, getProject(), 100, getTestConfig(), ss);
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val tableExt = tableMetadataManager.getTableExtIfExists(tableDesc);
        Assert.assertEquals(10, tableExt.getSampleRows().size());
        Assert.assertEquals(100.0 / 10000, tableExt.getTotalRows() / 10000.0, 0.1);

    }
}
