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

import java.io.File;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.spark.KapSparkSession;

public class NMeasuresTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    @Ignore
    // TODO FIXME issue #8315
    public void testMeasures() throws Exception {
        final String cubeName = "ncube_full_measure_test";
        buildCuboid(cubeName);

        //validate Cube Data by decode
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        NDataSegDetails segCuboids = NDataflowManager.getInstance(config, getProject()).getDataflow(cubeName)
                .getSegment(1).getSegDetails();
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, 1);
        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getCuboidData(dataCuboid, ss);
        for (Row row : ret.collectAsList()) {
            if (row.apply(0).toString().equals("10000000158")) {
                Assert.assertEquals("4", row.apply(1).toString());// COUNT(*)
                Assert.assertEquals("40000000632", row.apply(2).toString());// SUM(ID1)
                Assert.assertEquals(Double.valueOf("2637.703"), Double.valueOf(row.apply(3).toString()), 0.000001);// SUM(PRICE2)
                Assert.assertEquals("10000000158", row.apply(10).toString());// MIN(ID1)
                //Assert.assertEquals(10000000158.0, ((TopNCounter) row.apply(11)).getCounters()[0], 0.000001);// TOPN(ID1)
                //Assert.assertEquals("3", row.apply(15).toString());// HLL(NAME1)
                //Assert.assertEquals("4", row.apply(16).toString());
                //Assert.assertEquals(4, ((PercentileCounter) row.apply(21)).getRegisters().size());// percentile(PRICE1)
                //Assert.assertEquals("478.63", row.apply(25).toString());// HLL(NAME1, PRICCE1)
            }
            // verify the all null value aggregate
            if (row.apply(0).toString().equals("10000000162")) {
                Assert.assertEquals("3", row.apply(1).toString());// COUNT(*)
                Assert.assertEquals(Double.valueOf("0"), Double.valueOf(row.apply(3).toString()), 0.000001);// SUM(PRICE2)
                Assert.assertEquals(Double.valueOf("0"), Double.valueOf(row.apply(4).toString()), 0.000001);// SUM(PRICE3)
                Assert.assertEquals("0", row.apply(5).toString());// MAX(PRICE3)
                Assert.assertEquals("10000000162", row.apply(6).toString());// MIN(ID1)
                //Assert.assertEquals("0", row.apply(15).toString());// HLL(NAME1)
                //Assert.assertEquals("0", row.apply(16).toString());// HLL(NAME2)
                //Assert.assertEquals(0, ((PercentileCounter) row.apply(21)).getRegisters().size());// percentile(PRICE1)
                //Assert.assertEquals("0.0", row.apply(25).toString());// HLL(NAME1, PRICE1)
            }

            //build is done, start to test query
            SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
            existingCxt.stop();
            // Validate results between sparksql and cube
            KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
            kapSparkSession.use("default");
            populateSSWithCSVData(config, "default", kapSparkSession);
            List<Pair<String, String>> queries = NExecAndComp
                    .fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_measures");
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, "left");
            queries = NExecAndComp.fetchQueries(
                    KAP_SQL_BASE_DIR + File.separator + "sql_measures" + File.separator + "inaccurate_sql");
            NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.NONE, "left");
        }
    }

    private void buildCuboid(String cubeName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(cubeName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> toBuildLayouts = Lists.newArrayList(df.getCubePlan().getAllCuboidLayouts().get(0));

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(toBuildLayouts),
                "ADMIN");
        NSparkCubingStep sparkStep = (NSparkCubingStep) job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        Assert.assertEquals(ExecutableState.SUCCEED, wait(job));
    }
}
