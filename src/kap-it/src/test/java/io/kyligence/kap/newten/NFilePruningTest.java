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

import io.kyligence.kap.junit.TimeZoneTestRunner;
import java.util.ArrayList;
import java.util.List;

import java.util.UUID;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import scala.runtime.AbstractFunction1;

@RunWith(TimeZoneTestRunner.class)
public class NFilePruningTest extends NLocalWithSparkSessionTest {

    private String base = "select count(*) FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if(ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "true");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }


    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/file_pruning");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testNonExistTimeRange() throws Exception {
        val start = SegmentRange.dateToLong("2023-01-01 00:00:00");
        val end = SegmentRange.dateToLong("2025-01-01 00:00:00");
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
        assertResultsAndScanFiles(base, 1);
    }

    @Test
    public void testSegPruningWithTimeStamp() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        String and_pruning0 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' and TEST_TIME_ENC < TIMESTAMP '2013-01-01 00:00:00'";
        String and_pruning1 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' and TEST_TIME_ENC = TIMESTAMP '2016-01-01 00:00:00'";

        String or_pruning0 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' or TEST_TIME_ENC = TIMESTAMP '2016-01-01 00:00:00'";
        String or_pruning1 = base
                + "where TEST_TIME_ENC < TIMESTAMP '2009-01-01 00:00:00' or TEST_TIME_ENC > TIMESTAMP '2015-01-01 00:00:00'";

        String pruning0 = base + "where TEST_TIME_ENC < TIMESTAMP '2009-01-01 00:00:00'";
        String pruning1 = base + "where TEST_TIME_ENC <= TIMESTAMP '2009-01-01 00:00:00'";
        String pruning2 = base + "where TEST_TIME_ENC >= TIMESTAMP '2015-01-01 00:00:00'";

        String not0 = base + "where TEST_TIME_ENC <> TIMESTAMP '2012-01-01 00:00:00'";

        String in_pruning0 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2009-01-01 00:00:00',TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";
        String in_pruning1 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";

        assertResultsAndScanFiles(base, 3);

        assertResultsAndScanFiles(and_pruning0, 1);
        assertResultsAndScanFiles(and_pruning1, 0);

        assertResultsAndScanFiles(or_pruning0, 2);
        assertResultsAndScanFiles(or_pruning1, 0);

        assertResultsAndScanFiles(pruning0, 0);
        assertResultsAndScanFiles(pruning1, 1);
        assertResultsAndScanFiles(pruning2, 0);

        // pruning with "not" is not supported
        assertResultsAndScanFiles(not0, 3);

        assertResultsAndScanFiles(in_pruning0, 1);
        assertResultsAndScanFiles(in_pruning1, 0);
    }

    @Test
    public void testShardPruning() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "100");

        buildSegs("8c670664-8d05-466a-802f-83c023b56c77");

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        basicPruningScenario();
        pruningWithVariousTypesScenario();

        System.clearProperty("kap.storage.columnar.shard-rowcount");
    }

    @Test
    public void testPruningWithChineseCharacter() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "1");
        fullBuildCube("9cde9d25-9334-4b92-b229-a00f49453757", getProject());
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val chinese0 = "select count(*) from TEST_MEASURE where name1 = '中国'";
        val chinese1 = "select count(*) from TEST_MEASURE where name1 <> '中国'";

        assertResultsAndScanFiles(chinese0, 1);
        assertResultsAndScanFiles(chinese1, 2);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", chinese0));
        query.add(Pair.newPair("", chinese1));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");

        System.clearProperty("kap.storage.columnar.shard-rowcount");
    }

    private void pruningWithVariousTypesScenario() throws Exception {
        // int type is tested #basicPruningScenario

        // xx0 means can pruning, while xx1 can not.
        val bool0 = base + "where IS_EFFECTUAL = true";
        val bool1 = base + "where IS_EFFECTUAL <> true";

        val decimal0 = base + "where PRICE = 290.48";
        val decimal1 = base + "where PRICE > 290.48";

        val short0 = base + "where SLR_SEGMENT_CD = 16";
        val short1 = base + "where SLR_SEGMENT_CD > 16";

        val string0 = base + "where LSTG_FORMAT_NAME = 'Auction'";
        val string1 = base + "where LSTG_FORMAT_NAME <> 'Auction'";

        val long0 = base + "where TEST_ORDER.ORDER_ID = 2662";
        val long1 = base + "where TEST_ORDER.ORDER_ID <> 2662";

        val date0 = base + "where TEST_DATE_ENC = DATE '2011-07-10'";
        val date1 = base + "where TEST_DATE_ENC <> DATE '2011-07-10'";

        val ts0 = base + "where TEST_TIME_ENC = TIMESTAMP '2013-06-18 07:07:10'";

        val ts1 = base + "where TEST_TIME_ENC > TIMESTAMP '2013-01-01 00:00:00' "
                + "and TEST_TIME_ENC < TIMESTAMP '2015-01-01 00:00:00' "
                + "and TEST_TIME_ENC <> TIMESTAMP '2013-06-18 07:07:10'";

        assertResultsAndScanFiles(bool0, 3);
        assertResultsAndScanFiles(bool1, 11);

        assertResultsAndScanFiles(decimal0, 3);
        assertResultsAndScanFiles(decimal1, 52);

        // calcite will treat short as int. So pruning will not work.
        assertResultsAndScanFiles(short0, 25);
        assertResultsAndScanFiles(short1, 25);

        assertResultsAndScanFiles(string0, 3);
        assertResultsAndScanFiles(string1, 12);

        assertResultsAndScanFiles(long0, 3);
        assertResultsAndScanFiles(long1, 28);

        assertResultsAndScanFiles(date0, 3);
        assertResultsAndScanFiles(date1, 19);

        // segment pruning first, then shard pruning
        // so the scanned files is 1 not 3(each segment per shard)
        assertResultsAndScanFiles(ts0, 1);
        assertResultsAndScanFiles(ts1, 11);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", bool0));
        query.add(Pair.newPair("", bool1));
        query.add(Pair.newPair("", decimal0));
        query.add(Pair.newPair("", decimal1));
        query.add(Pair.newPair("", short0));
        query.add(Pair.newPair("", short1));
        query.add(Pair.newPair("", string0));
        query.add(Pair.newPair("", string1));
        query.add(Pair.newPair("", long0));
        query.add(Pair.newPair("", long1));
        query.add(Pair.newPair("", date0));
        query.add(Pair.newPair("", date1));

        // see #11598
        // query.add(Pair.newPair("", ts0));
        // query.add(Pair.newPair("", ts1));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    private void basicPruningScenario() throws Exception {
        // shard pruning supports: Equality/In/IsNull/And/Or
        // other expression(gt/lt/like/cast/substr, etc.) will select all files.

        val equality = base + "where SELLER_ID = 10000233";
        val in = base + "where SELLER_ID in (10000233,10000234,10000235)";
        val isNull = base + "where SELLER_ID is NULL";
        val and = base + "where SELLER_ID in (10000233,10000234,10000235) and SELLER_ID = 10000233 ";
        val or = base + "where SELLER_ID = 10000233 or SELLER_ID = 1 ";
        val notSupported0 = base + "where SELLER_ID <> 10000233";
        val notSupported1 = base + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(equality, 3);
        assertResultsAndScanFiles(in, 9);
        assertResultsAndScanFiles(isNull, 3);
        assertResultsAndScanFiles(and, 3);
        assertResultsAndScanFiles(or, 4);
        assertResultsAndScanFiles(notSupported0, 17);
        assertResultsAndScanFiles(notSupported1, 17);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", equality));
        query.add(Pair.newPair("", in));
        query.add(Pair.newPair("", isNull));
        query.add(Pair.newPair("", and));
        query.add(Pair.newPair("", or));
        query.add(Pair.newPair("", notSupported0));
        query.add(Pair.newPair("", notSupported1));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Override
    public String getProject() {
        return "file_pruning";
    }

    private long assertResultsAndScanFiles(String sql, long numScanFiles) throws Exception {
        val df = NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        df.collect();
        val actualNum = findFileSourceScanExec(df.queryExecution().sparkPlan()).metrics().get("numFiles").get().value();
        Assert.assertEquals(numScanFiles, actualNum);
        return actualNum;
    }

    private FileSourceScanExec findFileSourceScanExec(SparkPlan plan) {
        return (FileSourceScanExec) plan.find(new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan p) {
                return p instanceof FileSourceScanExec;
            }
        }).get();
    }

    private void buildSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getCuboidLayout(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2011-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);

        start = SegmentRange.dateToLong("2011-01-01 00:00:00");
        end = SegmentRange.dateToLong("2013-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);

        start = SegmentRange.dateToLong("2013-01-01 00:00:00");
        end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        buildCuboid(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
    }
}
