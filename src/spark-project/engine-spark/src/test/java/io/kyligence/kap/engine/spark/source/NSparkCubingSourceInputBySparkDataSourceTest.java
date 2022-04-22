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
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkCubingSourceInputBySparkDataSourceTest extends NLocalWithSparkSessionTest {

    private final ColumnDesc[] COLUMN_DESCS = new ColumnDesc[2];
    
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
    public void testGetHiveSourceData() {
        try {
            KylinBuildEnv.clean();
            KylinBuildEnv kylinBuildEnv = KylinBuildEnv.getOrCreate(getTestConfig());
            getTestConfig().setProperty("kylin.source.provider.9",
                    "io.kyligence.kap.engine.spark.source.NSparkDataSource");
            getTestConfig().setProperty("kylin.build.resource.read-transactional-table-enabled", "true");
            NTableMetadataManager tableMgr = NTableMetadataManager
                    .getInstance(getTestConfig(), "ssb");
            TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
            Dataset<Row> sourceData = SourceFactory
                    .createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(fact, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && rows.size() > 0);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof org.apache.spark.sql.AnalysisException);
        }
    }

    @Test
    public void testGetHiveSourceDataByTransaction() {
        try {
            KylinBuildEnv.clean();
            KylinBuildEnv kylinBuildEnv = KylinBuildEnv.getOrCreate(getTestConfig());
            getTestConfig().setProperty("kylin.source.provider.9",
                    "io.kyligence.kap.engine.spark.source.NSparkDataSource");
            getTestConfig().setProperty("kylin.build.resource.read-transactional-table-enabled", "true");
            NTableMetadataManager tableMgr = NTableMetadataManager
                    .getInstance(getTestConfig(), "ssb");
            TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
            fact.setTransactional(true);
            Dataset<Row> sourceData = SourceFactory
                    .createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(fact, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && rows.size() > 0);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof org.apache.spark.sql.AnalysisException);
        }
    }

    @Test
    public void testGetHiveSourceDataDisableAddBackTick() {
        try {
            KylinBuildEnv.clean();
            KylinBuildEnv.getOrCreate(getTestConfig());
            getTestConfig().setProperty("kylin.source.provider.9",
                    "io.kyligence.kap.engine.spark.source.NSparkDataSource");
            getTestConfig().setProperty("kylin.build.resource.read-transactional-table-enabled", "true");

            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
            TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
            Dataset<Row> sourceData = SourceFactory
                    .createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(fact, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && rows.size() > 0);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof org.apache.spark.sql.AnalysisException);
        }
    }

}
