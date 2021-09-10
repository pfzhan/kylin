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

package io.kyligence.kap.query;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.LayoutPartition;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;

public class HeterogeneousSegmentPruningTest extends NLocalWithSparkSessionTest {

    @Test
    public void testMultiLevelPartitionMapping() throws SQLException {
        val project = "multi_level_partition";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflowCopy = dfManager.getDataflow(dfId).copy();

        // build a new partition value but without mapping value
        val newPartition = Lists.<String[]>newArrayList(new String[]{"4"});
        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2012-01-02");
        val segment1Uuid = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        val segmentRange2 = Pair.newPair("2012-01-02", "2012-01-03");
        val segment2Uuid = "d75a822c-788a-4592-a500-cf20186dded1";
        val segmentRange3 = Pair.newPair("2012-01-03", "2012-01-04");
        val segment3Uuid = "54eaf96d-6146-45d2-b94e-d5d187f89919";
        val expectedPartitionMap = Maps.<String, List<Long>>newHashMap();
        dfManager.appendPartitions(dfId, segment1Uuid, newPartition);
        dfManager.appendPartitions(dfId, segment2Uuid, newPartition);
        dfManager.appendPartitions(dfId, segment3Uuid, newPartition);
        val layout1 = dataflowCopy.getSegment(segment1Uuid).getLayout(1L);
        val layout2 = dataflowCopy.getSegment(segment2Uuid).getLayout(1L);
        val layout3 = dataflowCopy.getSegment(segment3Uuid).getLayout(1L);
        layout1.getMultiPartition().add(new LayoutPartition(4L));
        layout2.getMultiPartition().add(new LayoutPartition(4L));
        layout3.getMultiPartition().add(new LayoutPartition(4L));
        val updateOps = new NDataflowUpdate(dfId);
        updateOps.setToAddOrUpdateLayouts(layout1, layout2, layout3);
        dfManager.updateDataflow(updateOps);

        val sqlBase = "select cal_dt, sum(price) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id ";
        val andMappingSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_format_name = 'FP-non GTC' group by cal_dt";
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(1L, 4L));
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(1L, 4L));
        expectedPartitionMap.put(segment3Uuid, Lists.newArrayList(1L, 4L));
        assertPrunedSegmentsRange(project, andMappingSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        val andSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_site_id = 1 group by cal_dt";
        expectedPartitionMap.clear();
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(1L));
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(1L));
        expectedPartitionMap.put(segment3Uuid, Lists.newArrayList(1L));
        assertPrunedSegmentsRange(project, andSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
    }

    @Test
    public void testMultiLevelPartitionPruning() throws SQLException {
        // model multi-partition desc
        // column: LSTG_SITE_ID
        // partition id: [0, 1, 2, 3]
        // values: [0, 1, 2, 3]
        // mapping column: lstg_format_name
        // value mapping:
        // 0 - FP-GTC
        // 1 - FP-non GTC
        // 2 - ABIN
        // 3 - Auction

        // segment1 [2012-01-01, 2012-01-02] partition value 0, 1, 2, 3
        // segment2 [2012-01-02, 2012-01-03] partition value 0, 1, 2
        // segment3 [2012-01-03, 2012-01-04] partition value 1, 2, 3
        // segment4 [2012-01-04, 2012-01-05] partition value 0, 1
        // segment5 [2012-01-05, 2012-01-06] partition value 2, 3

        val project = "multi_level_partition";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2012-01-02");
        val segment1Uuid = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        val segmentRange2 = Pair.newPair("2012-01-02", "2012-01-03");
        val segment2Uuid = "d75a822c-788a-4592-a500-cf20186dded1";
        val segmentRange3 = Pair.newPair("2012-01-03", "2012-01-04");
        val segment3Uuid = "54eaf96d-6146-45d2-b94e-d5d187f89919";
        val segmentRange4 = Pair.newPair("2012-01-04", "2012-01-05");
        val segment4Uuid = "411f40b9-a80a-4453-90a9-409aac6f7632";
        val segmentRange5 = Pair.newPair("2012-01-05", "2012-01-06");
        val segment5Uuid = "a8318597-cb75-416f-8eb8-96ea285dd2b4";
        val expectedPartitionMap = Maps.<String, List<Long>>newHashMap();

        val sqlBase = "select cal_dt, sum(price) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id ";

        // no filter
        val noFilterSql = sqlBase + "group by cal_dt";
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedRanges.add(segmentRange4);
        expectedRanges.add(segmentRange5);
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(0L, 1L, 2L, 3L));
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(0L, 1L, 2L));
        expectedPartitionMap.put(segment3Uuid, Lists.newArrayList(1L, 2L, 3L));
        expectedPartitionMap.put(segment4Uuid, Lists.newArrayList(0L, 1L));
        expectedPartitionMap.put(segment5Uuid, Lists.newArrayList(2L, 3L));
        assertPrunedSegmentsRange(project, noFilterSql, dfId, expectedRanges, 1L, expectedPartitionMap);

        val andSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_site_id = 1 group by cal_dt";
        val andMappingSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_format_name = 'FP-non GTC' group by cal_dt";
        val andMixSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_site_id = 1 and lstg_format_name = 'FP-non GTC' group by cal_dt";
        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitionMap.clear();
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(1L));
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(1L));
        expectedPartitionMap.put(segment3Uuid, Lists.newArrayList(1L));
        assertPrunedSegmentsRange(project, andSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, andMappingSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, andMixSql0, dfId, expectedRanges, 1L, expectedPartitionMap);

        val notInSql0 = sqlBase
                + "where cal_dt > '2012-01-02' and cal_dt < '2012-01-04' and lstg_site_id not in (0, 2, 3) group by cal_dt";
        val notInMappingSql0 = sqlBase
                + "where cal_dt > '2012-01-02' and cal_dt < '2012-01-04' and lstg_format_name not in ('FP-GTC', 'ABIN', 'Auction') group by cal_dt";
        val notInMixSql0 = sqlBase
                + "where cal_dt > '2012-01-02' and cal_dt < '2012-01-04' and lstg_site_id not in (0, 2, 3) and lstg_format_name not in ('FP-GTC', 'ABIN', 'Auction') group by cal_dt";
        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitionMap.clear();
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(1L));
        expectedPartitionMap.put(segment3Uuid, Lists.newArrayList(1L));
        assertPrunedSegmentsRange(project, notInSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, notInMappingSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, notInMixSql0, dfId, expectedRanges, 1L, expectedPartitionMap);

        // return empty data case
        val emptyData = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_site_id = 5 group by cal_dt";
        val emptyDataMapping = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_format_name = 'not_exist_name' group by cal_dt";
        val emptyDataMix = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_site_id = 5 and lstg_format_name = 'not_exist_name' group by cal_dt";
        expectedRanges.clear();
        expectedPartitionMap.clear();
        assertPrunedSegmentsRange(project, emptyData, dfId, expectedRanges, -1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, emptyDataMapping, dfId, expectedRanges, -1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, emptyDataMix, dfId, expectedRanges, -1L, expectedPartitionMap);

        // query data out of current built segments range
        val inSql0 = sqlBase
                + "where cal_dt > '2011-12-30' and cal_dt < '2012-01-03' and lstg_site_id in (1, 2) group by cal_dt";
        val inMappingSql0 = sqlBase
                + "where cal_dt > '2011-12-30' and cal_dt < '2012-01-03' and lstg_format_name in ('FP-non GTC', 'ABIN') group by cal_dt";
        val inMixSql0 = sqlBase
                + "where cal_dt > '2011-12-30' and cal_dt < '2012-01-03' and lstg_site_id in (1, 2) and lstg_format_name in ('FP-non GTC', 'ABIN') group by cal_dt";
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(1L, 2L));
        expectedPartitionMap.put(segment2Uuid, Lists.newArrayList(1L, 2L));
        assertPrunedSegmentsRange(project, inSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, inMappingSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        assertPrunedSegmentsRange(project, inMixSql0, dfId, expectedRanges, 1L, expectedPartitionMap);

        val pushDownSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_site_id = 3 group by cal_dt";
        try {
            assertPrunedSegmentsRange(project, pushDownSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        } catch (NoRealizationFoundException ex) {
            //
        }
        val pushDownMappingSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_format_name = 'Auction' group by cal_dt";
        try {
            assertPrunedSegmentsRange(project, pushDownMappingSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        } catch (NoRealizationFoundException ex) {
            //
        }
        val pushDownMixSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03' and lstg_site_id = 3 and lstg_format_name = 'Auction' group by cal_dt";
        try {
            assertPrunedSegmentsRange(project, pushDownMixSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
        } catch (NoRealizationFoundException ex) {
            //
        }

        // return empty result
        val wrongMapping0 = sqlBase
                + "where cal_dt between '2012-01-01' and '2012-01-02' and lstg_site_id = 0 and lstg_format_name = 'FP-non GTC' group by cal_dt";
        assertPrunedSegmentsRange(project, wrongMapping0, dfId, null, -1L, null);

        val orSql0 = sqlBase
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-02' and (lstg_site_id = 0 or lstg_format_name = 'FP-non GTC') group by cal_dt";
        expectedRanges.clear();
        expectedPartitionMap.clear();
        expectedRanges.add(segmentRange1);
        expectedPartitionMap.put(segment1Uuid, Lists.newArrayList(0L, 1L));
        assertPrunedSegmentsRange(project, orSql0, dfId, expectedRanges, 1L, expectedPartitionMap);
    }

    @Test
    public void testWhenReadySegmentIsEmpty() throws SQLException {
        val project = "heterogeneous_segment";
        val sql = "select cal_dt, count(*) from test_kylin_fact "
                + "inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "where cal_dt >= '2012-01-03' and cal_dt < '2012-01-10' group by cal_dt";
        try {
            new QueryExec(project, getTestConfig()).executeQuery(sql);
            Assert.fail();
        } catch (SQLException ex) {
            // no ready segments
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        try {
            getTestConfig().setProperty("kylin.query.heterogeneous-segment-enabled", "false");
            new QueryExec(project, getTestConfig()).executeQuery(sql);
            Assert.fail();
        } catch (SQLException ex) {
            // no ready segments
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }
    }

    @Test
    public void testHeterogeneousSegment() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2012-01-02");
        val segmentRange2 = Pair.newPair("2012-01-02", "2012-01-03");
        val segmentRange3 = Pair.newPair("2012-01-03", "2012-01-04");
        val segmentRange4 = Pair.newPair("2012-01-04", "2012-01-05");
        val segmentRange5 = Pair.newPair("2012-01-05", "2012-01-06");
        val layout_20000000001 = 20000000001L;
        val layout_20001 = 20001L;
        val layout_10001 = 10001L;
        val layout_1 = 1L;

        val sql = "select cal_dt, sum(price) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id ";

        val no_filter = sql + "group by cal_dt";
        try {
            assertPrunedSegmentsRange(project, no_filter, dfId, null, -1L, null);
        } catch (NoRealizationFoundException ex) {
            // segments do not have common layout
        }

        val sql1_date = sql
                + "where cal_dt = DATE '2012-01-01' or (cal_dt >= DATE '2012-01-02' and cal_dt < DATE '2012-01-04') group by cal_dt";
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertPrunedSegmentsRange(project, sql1_date, dfId, expectedRanges, layout_20001, null);

        val sql1_date_string = sql
                + "where cal_dt = '2012-01-01' or (cal_dt >= '2012-01-02' and cal_dt < '2012-01-04') group by cal_dt";
        assertPrunedSegmentsRange(project, sql1_date_string, dfId, expectedRanges, layout_20001, null);

        val sql2_date = sql + "where cal_dt >= DATE '2012-01-03' and cal_dt < DATE '2012-01-10' group by cal_dt";
        expectedRanges.clear();
        expectedRanges.add(segmentRange3);
        expectedRanges.add(segmentRange4);
        expectedRanges.add(segmentRange5);
        assertPrunedSegmentsRange(project, sql2_date, dfId, expectedRanges, layout_10001, null);

        val sql2_date_string = sql + "where cal_dt >= '2012-01-03' and cal_dt < '2012-01-10' group by cal_dt";
        assertPrunedSegmentsRange(project, sql2_date_string, dfId, expectedRanges, layout_10001, null);

        val sql3_no_layout = "select trans_id from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "where cal_dt > '2012-01-03' and cal_dt < '2012-01-05'";
        try {
            assertPrunedSegmentsRange(project, sql3_no_layout, dfId, expectedRanges, -1L, null);
        } catch (NoRealizationFoundException ex) {
            // pruned segments do not have capable layout to answer
        }

        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        val sql4_table_index = "select trans_id from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "where cal_dt > '2012-01-01' and cal_dt < '2012-01-03'";
        assertPrunedSegmentsRange(project, sql4_table_index, dfId, expectedRanges, layout_20000000001, null);

        val sql5 = "select trans_id, sum(price) "
                + "from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "where cal_dt > '2012-01-03' and cal_dt < '2012-01-06' group by trans_id";
        try {
            assertPrunedSegmentsRange(project, sql5, dfId, expectedRanges, -1L, null);
        } catch (NoRealizationFoundException ex) {
            // pruned segments do not have capable layout to answer
        }
    }

    // https://olapio.atlassian.net/browse/AL-1357
    @Test
    public void testHeterogeneousSegmentFilter() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2012-01-02");
        val segmentRange2 = Pair.newPair("2012-01-02", "2012-01-03");
        val segmentRange3 = Pair.newPair("2012-01-03", "2012-01-04");
        val segmentRange4 = Pair.newPair("2012-01-04", "2012-01-05");
        val segmentRange5 = Pair.newPair("2012-01-05", "2012-01-06");
        val layout_20000000001 = 20000000001L;
        val layout_20001 = 20001L;
        val layout_10001 = 10001L;
        val layout_1 = 1L;

        val sql = "select cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id ";

        val sql1_date = sql
                + "where cal_dt = DATE '2012-01-01' and test_account.ACCOUNT_SELLER_LEVEL = 1 ";
        expectedRanges.add(segmentRange1);
        assertPrunedSegmentsRange(
                project, sql1_date, dfId, expectedRanges, layout_20000000001, null,
                "=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),=(DEFAULT.TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL, 1)");
    }

    // https://olapio.atlassian.net/browse/AL-2878
    @Test
    public void testHeterogeneousSegmentFilters() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";

        val sql = "with T1 as (select cal_dt, trans_id \n"
                + "from test_kylin_fact inner join test_account \n"
                + "on test_kylin_fact.seller_id = test_account.account_id \n"
                + "where cal_dt between date'2012-01-01' and date'2012-01-03'\n"
                + "group by cal_dt, trans_id)\n";

        { // composite filters
            val sqlFilter = sql
                    + "select * from T1\n"
                    + "where (cal_dt = DATE'2012-01-01' or (cast(cal_dt as date) = '2012-01-02' or cal_dt = '2012-01-03')) and (cal_dt is not null or cal_dt in ('2012-01-01', '2012-01-02'))";
            val context = getMatchedContexts(project, sqlFilter).get(0);
            assertFiltersAndLayout(context, null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03),"
                    + "OR(=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01), =(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02), =(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)),"
                    + "OR(IS NOT NULL(DEFAULT.TEST_KYLIN_FACT.CAL_DT), OR(=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01), =(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02)))");
        }

        { // invalid filters with or
            val sqlFilter = sql
                    + "select * from T1\n"
                    + "where trans_id = 123  or trans_id + 1 = 123 or (trans_id + 2 = 234 and trans_id = 345)";
            val context = getMatchedContexts(project, sqlFilter).get(0);
            assertFiltersAndLayout(context, null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)");
        }

        { // invalid filters with and
            val sqlFilter = sql
                    + "select * from T1\n"
                    + "where trans_id = 123 and (trans_id + 2 = 234 or trans_id = 345)";
            val context = getMatchedContexts(project, sqlFilter).get(0);
            assertFiltersAndLayout(context, null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03),"
                    + "=(DEFAULT.TEST_KYLIN_FACT.TRANS_ID, 123)");
        }

        { // invalid filters with not
            val sqlFilter = sql
                    + "select * from T1\n"
                    + "where trans_id = 123 and (not(trans_id = 234) or trans_id = 345) and (not(trans_id + 1 = 132))";
            val context = getMatchedContexts(project, sqlFilter).get(0);
            assertFiltersAndLayout(context, null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03),"
                    + "=(DEFAULT.TEST_KYLIN_FACT.TRANS_ID, 123),OR(<>(DEFAULT.TEST_KYLIN_FACT.TRANS_ID, 234), =(DEFAULT.TEST_KYLIN_FACT.TRANS_ID, 345))");
        }
    }

    // https://olapio.atlassian.net/browse/AL-2878
    @Test
    public void testHeterogeneousSegmentFilterPushdownJoin() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val seg1Id = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        val seg2Id = "d75a822c-788a-4592-a500-cf20186dded1";
        val seg3Id = "54eaf96d-6146-45d2-b94e-d5d187f89919";
        val seg4Id = "411f40b9-a80a-4453-90a9-409aac6f7632";
        val seg5Id = "a8318597-cb75-416f-8eb8-96ea285dd2b4";

        val sql = "with T1 as (select cal_dt, trans_id \n"
                + "from test_kylin_fact inner join test_account \n"
                + "on test_kylin_fact.seller_id = test_account.account_id \n"
                + "where cal_dt between date'2012-01-01' and date'2012-01-03'\n"
                + "group by cal_dt, trans_id),\n"
                + "T2 as (\n"
                + "select cal_dt \n"
                + "from test_kylin_fact inner join test_account \n"
                + "on test_kylin_fact.seller_id = test_account.account_id \n"
                + "where cal_dt between date'2012-01-01' and date'2012-01-02'\n"
                + "group by cal_dt\n"
                + ")\n";

        { // inner join
            val sqlInnerJoin = sql
                    + " select * from T1 inner join T2 on T1.cal_dt = T2.cal_dt \n"
                    + " where T1.cal_dt = '2012-01-01' and T2.cal_dt = DATE '2012-01-02'";
            val contexts = getMatchedContexts(project, sqlInnerJoin);
            assertFiltersAndLayout(contexts.get(0), seg1Id, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03),=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01)");
            assertFiltersAndLayout(contexts.get(1), seg2Id, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02),=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02)");
        }

        { // left join
            val sqlLeftJoin = sql
                    + " select * from T1 left join T2 on T1.cal_dt = T2.cal_dt \n"
                    + " where T1.cal_dt = DATE '2012-01-01' and T2.cal_dt = DATE '2012-01-02'";
            val contexts = getMatchedContexts(project, sqlLeftJoin);
            assertFiltersAndLayout(contexts.get(0), seg1Id, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03),=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01)");
            assertFiltersAndLayout(contexts.get(1), null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02)");
        }

        { // right join
            val sqlRightJoin = sql
                    + " select * from T1 right join T2 on T1.cal_dt = T2.cal_dt \n"
                    + " where T1.cal_dt = DATE '2012-01-01' and T2.cal_dt = DATE '2012-01-02'";
            val contexts = getMatchedContexts(project, sqlRightJoin);
            assertFiltersAndLayout(contexts.get(0), seg2Id, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02),=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02)");
            assertFiltersAndLayout(contexts.get(1), null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)");
        }

        { // full join
            val sqlFullJoin = sql
                    + " select * from T1 full join T2 on T1.cal_dt = T2.cal_dt \n"
                    + " where T1.cal_dt = DATE '2012-01-01' and T2.cal_dt = DATE '2012-01-02'";
            val contexts = getMatchedContexts(project, sqlFullJoin);
            assertFiltersAndLayout(contexts.get(0), null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)");
            assertFiltersAndLayout(contexts.get(1), null, ">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-02)");
        }
    }

    @Test
    public void testHeterogeneousSegmentFilterConditionLimit() throws SQLException {
        // layout 20000000001, tableindex
        // layout 20001, cal_dt & trans_id
        // layout 10001, cal_dt
        // layout 1, trans_id

        // segment1 [2012-01-01, 2012-01-02] layout 20000000001, 20001
        // segment2 [2012-01-02, 2012-01-03] layout 20000000001, 20001, 10001
        // segment3 [2012-01-03, 2012-01-04] layout 20001, 10001, 1
        // segment4 [2012-01-04, 2012-01-05] layout 10001, 1
        // segment5 [2012-01-05, 2012-01-06] layout 20000000001, 20001, 10001, 1

        val project = "heterogeneous_segment";
        val dfId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val seg1Id = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        val seg2Id = "d75a822c-788a-4592-a500-cf20186dded1";
        val seg3Id = "54eaf96d-6146-45d2-b94e-d5d187f89919";
        val seg4Id = "411f40b9-a80a-4453-90a9-409aac6f7632";
        val seg5Id = "a8318597-cb75-416f-8eb8-96ea285dd2b4";

        val sql = "with T1 as (select cal_dt, trans_id \n"
                + "from test_kylin_fact inner join test_account \n"
                + "on test_kylin_fact.seller_id = test_account.account_id \n"
                + "where cal_dt between date'2012-01-01' and date'2012-01-03'\n"
                + "group by cal_dt, trans_id)\n";

        {
            val sqlWithTooManyOrs = sql
                    + " select * from T1 where "
                    + "(cal_dt='2012-01-01' and trans_id = 1) or \n"
                    + "(cal_dt='2012-01-01' and trans_id = 2) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 3) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 4) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 5) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 6) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 7) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 8) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 9) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 10) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 12) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 13) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 14) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 15) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 16) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 17) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 18) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 19) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 20)";
            val contexts = getMatchedContexts(project, sqlWithTooManyOrs);
            Assert.assertEquals(">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)",
                    contexts.get(0).getExpandedFilterConditions().stream().map(RexNode::toString).collect(Collectors.joining(",")));
        }

        {
            val sqlWithFilter = sql
                    + " select * from T1 where "
                    + "(cal_dt='2012-01-01' and trans_id = 1) or \n"
                    + "(cal_dt='2012-01-01' and trans_id = 2) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 3) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 4) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 5) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 6) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 7) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 8) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 9) or\n"
                    + "(cal_dt='2012-01-01' and trans_id = 10)";
            val contexts = getMatchedContexts(project, sqlWithFilter);
            Assert.assertNotEquals(">=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-01),<=(DEFAULT.TEST_KYLIN_FACT.CAL_DT, 2012-01-03)",
                    contexts.get(0).getExpandedFilterConditions().stream().map(RexNode::toString).collect(Collectors.joining(",")));
        }

    }

    private List<OLAPContext> getMatchedContexts(String project, String sql) {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.smart.conf.memory-tuning", "false");
        val proposeContext = new SmartContext(KylinConfig.getInstanceFromEnv(), project, new String[]{sql});
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);

        return smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts()
                .stream().peek(ctx -> {
                    ctx.olapSchema.setConfigOnlyInTest(KylinConfig.getInstanceFromEnv().base());
                    RealizationChooser.attemptSelectCandidate(ctx);
                }).collect(Collectors.toList());
    }

    private void assertFiltersAndLayout(OLAPContext context, String segId, String expectedFilterCond) {
        if (segId != null) {
            Assert.assertEquals(segId, context.storageContext.getPrunedSegments().get(0).getId());
        }
        if (expectedFilterCond != null) {
            Assert.assertEquals(expectedFilterCond,
                    context.getExpandedFilterConditions().stream().map(RexNode::toString).collect(Collectors.joining(",")));
        }
    }

    private void assertPrunedSegmentsRange(String project, String sql, String dfId,
                                           List<Pair<String, String>> expectedRanges, long expectedLayoutId,
                                           Map<String, List<Long>> expectedPartitions) throws SQLException {
        assertPrunedSegmentsRange(project, sql, dfId, expectedRanges, expectedLayoutId, expectedPartitions, null);
    }

    private void assertPrunedSegmentsRange(String project, String sql, String dfId,
                                           List<Pair<String, String>> expectedRanges, long expectedLayoutId,
                                           Map<String, List<Long>> expectedPartitions, String expectedFilterCond) throws SQLException {
        val context = getMatchedContexts(project, sql).get(0);

        if (expectedLayoutId == -1L) {
            Assert.assertTrue(context.storageContext.isEmptyLayout());
            Assert.assertNull(context.storageContext.getLayoutId());
            return;
        }

        val prunedSegments = context.storageContext.getPrunedSegments();
        val prunedPartitions = context.storageContext.getPrunedPartitions();
        val candidate = context.storageContext.getCandidate();
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        Assert.assertEquals(expectedLayoutId, candidate.getLayoutEntity().getId());

        val model = NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);

            // assert multi-partition
            if (expectedPartitions == null)
                continue;
            Assert.assertEquals(expectedPartitions.size(), prunedPartitions.size());
            Assert.assertEquals(expectedPartitions.get(segment.getId()), prunedPartitions.get(segment.getId()));
        }

        if (expectedFilterCond != null) {
            Assert.assertEquals(expectedFilterCond,
                    context.getExpandedFilterConditions().stream().map(f -> f.toString()).collect(Collectors.joining(",")));
        }
    }
}
