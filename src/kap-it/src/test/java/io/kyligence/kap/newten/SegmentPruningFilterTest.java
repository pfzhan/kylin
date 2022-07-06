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

package io.kyligence.kap.newten;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationPruner;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.TypeSystem;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerBuilder;
import lombok.val;

public class SegmentPruningFilterTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

    }

    private static List<NDataSegment> startRealizationPruner(NDataflowManager dataflowManager, String dataflowId,
            String sql, String project, KylinConfig kylinConfig) throws Exception {
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        AbstractQueryRunner queryRunner1 = new QueryRunnerBuilder(project, kylinConfig, new String[] { sql }).build();
        queryRunner1.execute();
        Map<String, Collection<OLAPContext>> olapContexts = queryRunner1.getOlapContexts();
        OLAPContext context = olapContexts.get(sql).iterator().next();
        TblColRef filterColumn = context.filterColumns.iterator().next();
        dataflow.getModel().getPartitionDesc().setPartitionDateColumnRef(filterColumn);
        CalciteSchema rootSchema = new QueryExec(project, kylinConfig).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                kylinConfig);
        context.firstTableScan.getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
        return RealizationPruner.pruneSegments(dataflow, context);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition_date_type");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testSegmentPruningPartitionDateColumnFilter() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e309";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= '2021-10-28' AND DATE_6 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStrColumnFilter() throws Exception {
        val dataflowId = "00a91916-d31e-ed40-b1ba-4a86765072f6";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= '2021-12-20' AND STRING_DATE_20 < '2021-12-26'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < '2021-12-26'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < CAST('2021-12-26' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStr2ColumnFilter() throws Exception {
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM SONGZHEN_TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < '20211224'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Override
    public String getProject() {
        return "multi_partition_date_type";
    }

}
