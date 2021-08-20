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

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkCubingSourceInputTest extends NLocalWithSparkSessionTest {

    private final ColumnDesc[] COLUMN_DESCS = new ColumnDesc[2];
    private final String ORIGIN_TABLE = "test1";
    private final String INTERMEDIATE_TABLE = "test1_hive_tx";
    private final String STORAGE_FORMAT = "TEXTFILE";
    private final String STORAGE_DFS_DIR = "/test";
    private final String FILED_DELIMITER = "|";

    @Before
    public void setup() {
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("id1");
            columnDesc.setDatatype("integer");
            COLUMN_DESCS[0] = columnDesc;
        }
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("str1");
            columnDesc.setDatatype("varchar");
            COLUMN_DESCS[1] = columnDesc;
        }
    }

    @Test
    public void testGetSourceData() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
        Dataset<Row> sourceData = SourceFactory.createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(fact, ss, Maps.newHashMap());
        List<Row> rows = sourceData.collectAsList();
        Assert.assertTrue(rows != null && rows.size() > 0);
    }

    @Test
    public void testHiveInitStatement() {
        String DATABASE = "DEFAULT";
        Assert.assertEquals("USE DEFAULT;\n", NSparkCubingSourceInput.generateHiveInitStatements(DATABASE));
    }

    @Test
    public void testInsertDataStatement() {
        String statement = "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n" + ",`STR1`\n"
                + "FROM `test1` \n;\n";
        Assert.assertEquals(statement,
                NSparkCubingSourceInput.generateInsertDataStatement(COLUMN_DESCS, ORIGIN_TABLE, INTERMEDIATE_TABLE));
    }

    @Test
    public void testDropTableStatement() {
        String statement = "DROP TABLE IF EXISTS `test1_hive_tx`;\n";
        Assert.assertEquals(statement, NSparkCubingSourceInput.generateDropTableStatement(INTERMEDIATE_TABLE));
    }

    @Test
    public void testCreateTableStatement() {
        String statement = "CREATE EXTERNAL TABLE IF NOT EXISTS `test1_hive_tx`\n" + "(\n" + "`ID1` int\n"
                + ",`STR1` string\n" + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n"
                + "STORED AS TEXTFILE\n" + "LOCATION '/test/test1_hive_tx';\n"
                + "ALTER TABLE `test1_hive_tx` SET TBLPROPERTIES('auto.purge'='true');\n";
        String actual = NSparkCubingSourceInput.generateCreateTableStatement(INTERMEDIATE_TABLE, STORAGE_DFS_DIR,
                COLUMN_DESCS, STORAGE_FORMAT, FILED_DELIMITER);
        Assert.assertEquals(statement, actual);
    }

    @Test
    public void testCreateTableStatements() {
        String statement = "DROP TABLE IF EXISTS `test1_hive_tx`;\n"
                + "CREATE EXTERNAL TABLE IF NOT EXISTS `test1_hive_tx`\n" + "(\n" + "`ID1` int\n" + ",`STR1` string\n"
                + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n" + "STORED AS TEXTFILE\n"
                + "LOCATION '/test/test1_hive_tx';\n"
                + "ALTER TABLE `test1_hive_tx` SET TBLPROPERTIES('auto.purge'='true');\n"
                + "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n" + ",`STR1`\n" + "FROM `test1` \n"
                + ";\n";
        String actual = NSparkCubingSourceInput.getCreateTableStatement(ORIGIN_TABLE, INTERMEDIATE_TABLE, COLUMN_DESCS,
                STORAGE_DFS_DIR, STORAGE_FORMAT, FILED_DELIMITER);
        Assert.assertEquals(statement, actual);
    }
}
