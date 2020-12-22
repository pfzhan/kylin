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
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;

public class NBitmapFunctionTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "intersect_count";
    }

    @Test
    public void testBitmapFunction() throws Exception {
        fullBuildCube("741ca86a-1f13-46da-a59f-95fb68615e3b", getProject());
        fullBuildCube("741ca86a-1f13-46da-a59f-95fb68615e3z", getProject());

        testDateType();

        testMultiMeasures();

        testCommomCase1();

        testCommomCase2();

        testWithUnion();

        testWithLimit();

        testIntersectCountByCol();

        testIntersectCountByColMultiRows();

        testIntersectCount();

        testIntersectValue();

        testExplodeIntersectValue();

        testHllcCanNotAnswerBitmapUUID();

        testSubtractBitmapValue();

        testSubtractBitmapUUID();
    }

    private void testDateType() throws SQLException {
        String query = "select CAL_DT, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday "
                + "from test_kylin_fact " + "where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by CAL_DT " + "order by CAL_DT ";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-01-01,14,0,0,0,0", result.get(0));
        Assert.assertEquals("2012-01-02,0,10,0,0,0", result.get(1));
        Assert.assertEquals("2012-01-03,0,0,5,0,0", result.get(2));
    }

    private void testMultiMeasures() throws SQLException {
        String query = "select week_beg_dt as week, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC']) as a, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Auction']) as b, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Others']) as c, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction']) as ab, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Others']) as ac, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction', 'Others']) as abc, "
                + "count(distinct TEST_COUNT_DISTINCT_BITMAP) as sellers, count(*) as cnt "
                + "from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = edw.test_cal_dt.CAL_DT "
                + "where week_beg_dt in (DATE '2013-12-22', DATE '2012-06-23') group by week_beg_dt order by week_beg_dt";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-06-23,21,17,13,0,0,0,90,94", result.get(0));
        Assert.assertEquals("2013-12-22,18,22,13,0,0,0,98,99", result.get(1));
    }

    private void testCommomCase1() throws SQLException {
        String query = "select LSTG_FORMAT_NAME, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday "
                + "from test_kylin_fact where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by LSTG_FORMAT_NAME order by LSTG_FORMAT_NAME";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("ABIN,6,4,2,0,0", result.get(0));
        Assert.assertEquals("Auction,4,3,1,0,0", result.get(1));
        Assert.assertEquals("FP-GTC,2,2,0,0,0", result.get(2));
        Assert.assertEquals("FP-non GTC,2,1,0,0,0", result.get(3));
        Assert.assertEquals("Others,0,0,2,0,0", result.get(4));
    }

    private void testCommomCase2() throws SQLException {
        String query4 = "select LEAF_CATEG_ID, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day "
                + "from test_kylin_fact where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by LEAF_CATEG_ID " + "order by LEAF_CATEG_ID";
        List<String> result = NExecAndComp.queryCube(getProject(), query4).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("963,1", result.get(0));
        Assert.assertEquals("1349,1", result.get(1));
        Assert.assertEquals("11554,1", result.get(2));
        Assert.assertEquals("20485,1", result.get(3));
        Assert.assertEquals("35570,1", result.get(4));
        Assert.assertEquals("62179,2", result.get(5));
        Assert.assertEquals("95173,1", result.get(6));
        Assert.assertEquals("95672,2", result.get(7));
        Assert.assertEquals("140746,1", result.get(8));
        Assert.assertEquals("148324,1", result.get(9));
        Assert.assertEquals("158798,1", result.get(10));
        Assert.assertEquals("175750,1", result.get(11));
    }

    private void testWithUnion() throws SQLException {
        String query = "SELECT (SELECT '2012-01-01') AS sdate, "
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-01']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-02']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-03'])"
                + "FROM   test_kylin_fact WHERE cal_dt >= date '2012-01-01' AND cal_dt <  date'2012-01-07' "
                + "UNION ALL " + "SELECT (SELECT '2012-01-02') AS sdate, "
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-02']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-03']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-04'])"
                + "FROM   test_kylin_fact WHERE  cal_dt >= date '2012-01-02' AND cal_dt < date'2012-01-07'"
                + "order by sdate";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-01-01,14,1,0", result.get(0));
        Assert.assertEquals("2012-01-02,10,1,0", result.get(1));
    }

    private void testWithLimit() throws SQLException {
        String query = "select intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day "
                + "from test_kylin_fact " + "limit 1";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("14", result.get(0));
    }

    private void testIntersectCountByColMultiRows() throws SQLException {
        String query1 = "select intersect_count_by_col(Array[t1.a1]), LSTG_FORMAT_NAME from "
                + "    (select bitmap_uuid(SELLER_ID) as a1, LSTG_FORMAT_NAME "
                + "        from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME) t1 order by LSTG_FORMAT_NAME";

        List<String> result1 = NExecAndComp.queryCube(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("855,ABIN", result1.get(0));
        Assert.assertEquals("896,Auction", result1.get(1));
        Assert.assertEquals("858,FP-GTC", result1.get(2));
        Assert.assertEquals("870,FP-non GTC", result1.get(3));
        Assert.assertEquals("855,Others", result1.get(4));
    }

    private void testIntersectCountByCol() throws Exception {
        String query1 = "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others']) as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-.*GTC', 'Others'], 'REGEXP') as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as a2 " + "from TEST_KYLIN_FACT) t2";

        List<String> result1 = NExecAndComp.queryCube(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("841", result1.get(0));
        Assert.assertEquals("841", result1.get(1));
        Assert.assertEquals("841", result1.get(2));

        String query2 = "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others']) as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-.*GTC', 'Others'], 'REGEXP') as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as a2 " + "from TEST_KYLIN_FACT) t2";
        List<String> result2 = NExecAndComp.queryCube(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("862", result2.get(0));
        Assert.assertEquals("862", result2.get(1));
        Assert.assertEquals("862", result2.get(2));
    }

    private void testIntersectCount() throws SQLException {
        String query = "select "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others']) as a, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("862,862,862", result.get(0));
    }

    private void testIntersectValue() throws SQLException {
        String query = "select "
                + "intersect_value(LSTG_SITE_ID, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others']) as a, "
                + "intersect_value_v2(LSTG_SITE_ID, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_value_v2(LSTG_SITE_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact ";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("WrappedArray(0, 2, 3, 15, 23, 100, 101, 211),"
                + "WrappedArray(0, 2, 3, 15, 23, 100, 101, 211)," + "WrappedArray(0, 2, 3, 15, 23, 100, 101, 211)",
                result.get(0));
    }

    private void testExplodeIntersectValue() throws SQLException {
        String query = "select "
                + "explode(intersect_value(LSTG_SITE_ID, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others'])) as a "
                + "from test_kylin_fact ";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("0", result.get(0));
        Assert.assertEquals("2", result.get(1));
        Assert.assertEquals("3", result.get(2));
        Assert.assertEquals("15", result.get(3));
        Assert.assertEquals("23", result.get(4));
        Assert.assertEquals("100", result.get(5));
        Assert.assertEquals("101", result.get(6));
        Assert.assertEquals("211", result.get(7));
    }

    private void testHllcCanNotAnswerBitmapUUID() throws SQLException {
        String query = "select intersect_count_by_col(Array[t1.a1]), LSTG_FORMAT_NAME from"
                + " (select bitmap_uuid(SELLER_ID) as a1, LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME) t1"
                + " order by LSTG_FORMAT_NAME";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("855,ABIN", result.get(0));
        Assert.assertEquals("896,Auction", result.get(1));
        Assert.assertEquals("858,FP-GTC", result.get(2));
        Assert.assertEquals("870,FP-non GTC", result.get(3));
        Assert.assertEquals("855,Others", result.get(4));
    }

    private void testSubtractBitmapValue() throws SQLException {
        String query = "select subtract_bitmap_value("
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING'),"
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN', 'Auction'], 'RAWSTRING'))"
                + "from TEST_KYLIN_FACT";
        List<Integer> acutal = NExecAndComp.queryCube(getProject(), query).collectAsList().get(0).getList(0).stream()
                .map(row -> Integer.parseInt(row.toString())).collect(Collectors.toList());

        Dataset<Row> fg = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'FP-GTC'");
        Dataset<Row> fng = ss
                .sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'FP-non GTC'");
        Dataset<Row> ot = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'Others'");
        Dataset<Row> ab = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'ABIN'");
        Dataset<Row> au = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'Auction'");
        List<Integer> expect = fg.union(fng).intersect(ot).except(ab.intersect(au)).sort(new Column("SELLER_ID"))
                .collectAsList().stream().map(row -> row.getInt(0)).collect(Collectors.toList());
        Assert.assertEquals(expect.size(), acutal.size());
        for (int i = 0; i < acutal.size(); i++) {
            Assert.assertEquals(expect.get(i), acutal.get(i));
        }
    }

    private void testSubtractBitmapUUID() throws SQLException {
        String query = "select intersect_count_by_col(Array[t1.a1, t2.a2]) from " + "(select subtract_bitmap_uuid("
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING'),"
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN', 'Auction'], 'RAWSTRING')) as a1 "
                + "from TEST_KYLIN_FACT) t1, " + "(select bitmap_uuid(SELLER_ID) as a2 from TEST_KYLIN_FACT) t2";
        List<String> result = NExecAndComp.queryCube(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("210", result.get(0));
    }
}
