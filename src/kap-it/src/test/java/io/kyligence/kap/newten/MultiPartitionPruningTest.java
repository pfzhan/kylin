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

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.relnode.ContextUtil;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.KylinFileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(TimeZoneTestRunner.class)
public class MultiPartitionPruningTest extends NLocalWithSparkSessionTest {
    private final String sql = "select count(*) from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id ";

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
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition_pruning");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        System.setProperty("kylin.model.multi-partition-enabled", "true");
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.model.multi-partition-enabled");
    }

    @Override
    public String getProject() {
        return "multi_partition_pruning";
    }

    @Test
    public void testPartitionPruningVarchar() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c78";

        // segment1 [2009-01-01, 2011-01-01] partition value Others, ABIN, FP-non GTC
        // segment2 [2011-01-01, 2013-01-01] partition value Others, ABIN
        // segment3 [2013-01-01, 2015-01-01] partition value Others, ABIN, FP-GTC
        buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 2L));
        buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 3L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val noPartitionFilterSql = sql + "where cal_dt between '2008-01-01' and '2012-01-01' ";
        val andSql = sql + "where cal_dt between '2009-01-01' and '2012-01-01' and lstg_format_name = 'ABIN' ";
        val notInSql = sql + "where cal_dt > '2009-01-01' and cal_dt < '2012-01-01' and lstg_format_name not in ('ABIN', 'FP-non GTC', 'FP-GTC', 'Auction')";
        val emptyResultSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_format_name = 'NOT-EXIST-VALUE' ";
        val pushdownSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_format_name = 'FP-GTC' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 3L));
        assertResultsAndScanFiles(dfName, sql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(0L));
        expectedPartitions.add(Lists.newArrayList(0L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", notInSql));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningInteger() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c76";

        // segment1 [2009-01-01, 2011-01-01] partition value 0, 2, 3
        // segment2 [2011-01-01, 2013-01-01] partition value 0, 2
        // segment3 [2013-01-01, 2015-01-01] partition value 0, 2, 15
        buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 2L));
        buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 3L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val noPartitionFilterSql = sql + "where cal_dt between '2008-01-01' and '2012-01-01' ";
        val andSql = sql + "where cal_dt between '2009-01-01' and '2012-01-01' and lstg_site_id = 2 ";
        val notInSql = sql + "where cal_dt > '2009-01-01' and cal_dt < '2012-01-01' and lstg_site_id not in (2, 3, 15, 23)";
        val emptyResultSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_site_id = 10000 ";
        val pushdownSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_site_id = 15 ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 3L));
        assertResultsAndScanFiles(dfName, sql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(0L));
        expectedPartitions.add(Lists.newArrayList(0L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", notInSql));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningDate() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";

        // segment1 [2009-01-01, 2011-01-01] partition value 2010-01-01, 2011-01-01
        // segment2 [2011-01-01, 2013-01-01] partition value 2011-01-01, 2012-01-01, 2013-01-01
        // segment3 [2013-01-01, 2015-01-01] partition value 2012-01-01, 2013-01-01, 2014-01-01
        buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(1L, 2L, 3L));
        buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01 00:00:00", "2011-01-01 00:00:00");
        val segmentRange2 = Pair.newPair("2011-01-01 00:00:00", "2013-01-01 00:00:00");
        val segmentRange3 = Pair.newPair("2013-01-01 00:00:00", "2015-01-01 00:00:00");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val baseSql = "select count(*) from test_order left join test_kylin_fact on test_order.order_id = test_kylin_fact.order_id ";
        val noPartitionFilterSql = baseSql + "where test_time_enc between '2010-01-01 00:00:00' and '2012-01-01 00:00:00' ";
        val andSql = baseSql + "where test_time_enc > '2012-01-01 00:00:00' and test_time_enc < '2014-01-01 00:00:00' and test_date_enc = '2013-01-01' ";
        val inSql = baseSql + "where test_time_enc > '2012-01-01 00:00:00' and test_time_enc < '2014-01-01 00:00:00' and test_date_enc in ('2012-01-01', '2013-01-01') ";
        val notInSql = baseSql + "where test_time_enc between '2009-01-01 00:00:00' and '2011-01-01 00:00:00' and test_date_enc not in ('2010-01-01', '2012-01-01', '2013-01-01', '2014-01-01') ";
        val emptyResultSql = baseSql + "where test_time_enc between '2009-01-01 00:00:00' and '2011-01-01 00:00:00' and test_date_enc = '2020-01-01' ";
        val pushdownSql = baseSql + "where test_time_enc between '2011-01-01 00:00:00' and '2015-01-01 00:00:00' and test_date_enc = '2011-01-01' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, baseSql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(3L));
        expectedPartitions.add(Lists.newArrayList(3L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        assertResultsAndScanFiles(dfName, inSql, 4, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", inSql));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningTimestamp() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c79";

        // segment1 [2009-01-01, 2011-01-01] partition value 2010-01-01 00:56:38, 2010-01-01 04:03:59
        // segment2 [2011-01-01, 2013-01-01] partition value 2010-01-01 04:03:59, 2010-01-01 08:16:36, 2010-01-02 14:24:50
        // segment3 [2013-01-01, 2015-01-01] partition value 2010-01-01 08:16:36, 2010-01-02 14:24:50, 2010-01-03 05:15:09
        buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(1L, 2L, 3L));
        buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00", Lists.newArrayList(10001L), Lists.newArrayList(2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val baseSql = "select count(*) from test_order left join test_kylin_fact on test_order.order_id = test_kylin_fact.order_id ";
        val noPartitionFilterSql = baseSql + "where test_date_enc between '2010-01-01' and '2012-01-01' ";
        val andSql = baseSql + "where test_date_enc > '2012-01-01' and test_date_enc < '2014-01-01' and test_time_enc = '2010-01-02 14:24:50' ";
        val inSql = baseSql + "where test_date_enc > '2012-01-01' and test_date_enc < '2014-01-01' and test_time_enc in ('2010-01-01 08:16:36', '2010-01-02 14:24:50') ";
        val notInSql = baseSql + "where test_date_enc between '2009-01-01' and '2011-01-01' and test_time_enc not in ('2010-01-01 00:56:38', '2010-01-01 08:16:36', '2010-01-02 14:24:50', '2010-01-03 05:15:09') ";
        val emptyResultSql = baseSql + "where test_date_enc between '2009-01-01' and '2011-01-01' and test_time_enc = '2020-01-01 00:00:00' ";
        val pushdownSql = baseSql + "where test_date_enc between '2011-01-01' and '2015-01-01' and test_time_enc = '2010-01-01 04:03:59' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, baseSql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(3L));
        expectedPartitions.add(Lists.newArrayList(3L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        assertResultsAndScanFiles(dfName, inSql, 4, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", inSql));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningChinese() throws Exception {
        val dfName = "9cde9d25-9334-4b92-b229-a00f49453757";

        // segment1 [2012-01-01, 2013-01-01] partition value FT, 中国
        // segment2 [2013-01-01, 2014-01-01] partition value 中国
        buildMultiSegmentPartitions(dfName, "2012-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(100001L), Lists.newArrayList(0L, 1L));
        buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2014-01-01 00:00:00", Lists.newArrayList(100001L), Lists.newArrayList(0L, 1L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2013-01-01");
        val segmentRange2 = Pair.newPair("2013-01-01", "2014-01-01");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val chineseSql = "select count(*), time1 from test_measure where time1 > '2012-01-01' and time1 < '2013-01-01' and name1 = '中国' group by time1";

        expectedRanges.add(segmentRange1);
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, chineseSql, 1, false, expectedRanges, expectedPartitions);

        val queries = Lists.<Pair<String, String>>newArrayList();
        queries.add(Pair.newPair("", chineseSql));
        NExecAndComp.execAndCompare(queries, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testExactlyMatch() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c80";

        // segment1 [2009-01-01, 2011-01-01] build all partitions
        // segment2 [2011-01-01, 2013-01-01] build all partitions
        buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00", Lists.newArrayList(10001L, 11001L), Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00", Lists.newArrayList(10001L, 11001L), Lists.newArrayList(0L, 1L, 2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val expectedPartitions = Lists.<List<Long>>newArrayList();

        val sql1 = "select\n" +
                "  count(*), cal_dt\n" +
                "from\n" +
                "  test_kylin_fact\n" +
                "  left join test_order on test_kylin_fact.order_id = test_order.order_id\n" +
                "where\n" +
                "  cal_dt between '2009-01-01'\n" +
                "  and '2012-01-01'\n" +
                "group by\n" +
                "  cal_dt\n" +
                "order by\n" +
                "  cal_dt\n";
        val sql2 = "select\n" +
                "  count(*), cal_dt\n" +
                "from\n" +
                "  test_kylin_fact\n" +
                "  left join test_order on test_kylin_fact.order_id = test_order.order_id\n" +
                "where\n" +
                "  cal_dt between '2009-01-01'\n" +
                "  and '2012-01-01' and lstg_format_name in ('ABIN', 'FP-non GTC') \n" +
                "group by\n" +
                "  cal_dt, lstg_format_name\n" +
                "order by\n" +
                "  cal_dt\n";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, sql1, 10, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L, 2L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L));
        assertResultsAndScanFiles(dfName, sql2, 4, false, expectedRanges, expectedPartitions);

        val queries = Lists.<Pair<String, String>>newArrayList();
        queries.add(Pair.newPair("", sql1));
        queries.add(Pair.newPair("", sql2));
        NExecAndComp.execAndCompare(queries, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    private long assertResultsAndScanFiles(String modelId, String sql, long numScanFiles, boolean emptyLayout, List<Pair<String, String>> expectedRanges, List<List<Long>> expectedPartitions) throws Exception {
        val df = NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        val context = ContextUtil.listContexts().get(0);
        if (emptyLayout) {
            Assert.assertTrue(context.storageContext.isEmptyLayout());
            Assert.assertNull(context.storageContext.getCuboidLayoutId());
            return numScanFiles;
        }
        df.collect();
        val actualNum = findFileSourceScanExec(df.queryExecution().sparkPlan()).metrics().get("numFiles").get().value();
        Assert.assertEquals(numScanFiles, actualNum);
        val segmentIds = context.storageContext.getPrunedSegments();
        val partitions = context.storageContext.getPrunedPartitions();
        assertPrunedSegmentRange(modelId, segmentIds, partitions, expectedRanges, expectedPartitions);
        return actualNum;
    }

    private KylinFileSourceScanExec findFileSourceScanExec(SparkPlan plan) {
        return (KylinFileSourceScanExec) plan.find(new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan p) {
                return p instanceof KylinFileSourceScanExec;
            }
        }).get();
    }

    private void assertPrunedSegmentRange(String dfId, List<NDataSegment> prunedSegments, Map<String, List<Long>> prunedPartitions, List<Pair<String, String>> expectedRanges, List<List<Long>> expectedPartitions) {
        val model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();

        if (CollectionUtils.isEmpty(expectedRanges)) {
            return;
        }
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        Assert.assertEquals(expectedPartitions.size(), prunedSegments.size());
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);

            val actualPartitions = prunedPartitions.get(segment.getId());
            Assert.assertEquals(expectedPartitions.get(i), actualPartitions);
        }
    }
}
