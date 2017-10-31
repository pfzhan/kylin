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

import java.util.Set;

import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
public class NSparkSourceTest extends NLocalSparkWithCSVDataTest {

    @Test
    public void testGetTable() {
        TableMetadataManager tableMgr = TableMetadataManager.getInstance(getTestConfig());
        TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER", "ssb");

        NSparkCubingSource cubingSource = new NSparkDataSource().adaptToBuildEngine(NSparkCubingSource.class);
        Dataset<Row> df = cubingSource.getSourceData(fact, ss);
        df.show(10);

        StructType schema = df.schema();
        ColumnDesc[] colDescs = fact.getColumns();
        for (int i = 0; i < colDescs.length; i++) {
            StructField field = schema.fields()[i];
            Assert.assertEquals(field.name(), colDescs[i].getName());
            Assert.assertEquals(field.dataType(), DataTypes.StringType);
        }
    }

    @Test
    public void testGetFlatTable() {
        System.out.println(getTestConfig().getMetadataUrl());
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig());
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataModel model = (NDataModel) df.getModel();

        NSparkCubingSource cubingSource = new NSparkDataSource().adaptToBuildEngine(NSparkCubingSource.class);
        Dataset<Row> ds = cubingSource.getSourceData(df, new SegmentRange<>(0L, System.currentTimeMillis()), ss);
        ds.show(10);

        StructType schema = ds.schema();
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(model.getColumnNameByColumnId(Integer.valueOf(field.name()))));
            Assert.assertEquals(field.dataType(), DataTypes.StringType);
        }

        Set<Integer> dims = df.getCubePlan().getEffectiveDimCols().keySet();
        Column[] modelCols = new Column[dims.size()];
        int index = 0;
        for (int id : dims) {
            modelCols[index] = new Column(String.valueOf(id));
            index++;
        }
        ds.select(modelCols).show(10);

        System.out.println(ds.select(modelCols).queryExecution().optimizedPlan());
    }
}
