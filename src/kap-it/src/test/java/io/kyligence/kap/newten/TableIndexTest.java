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
import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.util.ExecAndComp;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;

public class TableIndexTest extends NLocalWithSparkSessionTest {

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
        return "table_index";
    }

    @Test
    public void testUseTableIndexAnswerNonRawQuery() throws Exception {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        fullBuild("acfde546-2cc9-4eec-bc92-e3bd46d4e2ee");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        query.add(Pair.newPair("query_table_index1", "select PRICE from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index2", "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index3", "select max(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index4", "select min(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index5", "select count(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(
                Pair.newPair("query_table_index6", "select count(distinct PRICE) from TEST_KYLIN_FACT group by PRICE"));

        query.add(Pair.newPair("query_table_index7", "select sum(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index8", "select max(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index9", "select min(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index10", "select count(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index11", "select count(distinct PRICE) from TEST_KYLIN_FACT"));

        query.add(Pair.newPair("query_table_index12",
                "select sum(PRICE),sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index13",
                "select max(PRICE),max(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index14",
                "select min(PRICE),min(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index15",
                "select count(PRICE),count(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index16",
                "select count(distinct PRICE),count(distinct ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));

        query.add(Pair.newPair("query_agg_index1", "select sum(ORDER_ID) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_agg_index2",
                "select sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));

        query.add(Pair.newPair("query_agg_inner_col_index1",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n"
                        + "  ) from TEST_KYLIN_FACT \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));
        query.add(Pair.newPair("query_agg_inner_col_index2",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n" + "  ) \n"
                        + "from \n" + "  (\n" + "    select \n" + "      a1.ORDER_ID - 10 as ORDER_ID, \n"
                        + "      a1.LSTG_FORMAT_NAME\n" + "    from \n" + "      TEST_KYLIN_FACT a1\n" + "  ) \n"
                        + "where \n" + "  order_id > 10 \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));

        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
    }

    @Test
    public void testUseTableIndexAnswerCountDistinctWithConvertRuleOn() throws Exception {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        fullBuild("acfde546-2cc9-4eec-bc92-e3bd46d4e2ee");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("query_count_distinct", "select\n"
                + "count(\n"
                + "    distinct(\n"
                + "        case when (a.ORDER_ID > 0)\n"
                + "        THEN a.ORDER_ID\n"
                + "        ELSE NULL\n"
                + "        end)\n"
                + "        )\n"
                + "from (\n"
                + "select ORDER_ID from TEST_KYLIN_FACT\n"
                + ") a"));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
    }

    @Test
    public void testCountDistinctQueryRetry() throws Exception {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        fullBuild("975ae5ed-e670-3613-5e80-f9def911c632");
        fullBuild("acfde546-2cc9-4eec-bc92-e3bd46d4e2bf");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("query_count_distinct", "SELECT Count(DISTINCT CASE\n"
                + "                        WHEN KYLIN_FACT.lstg_format_name = 'ABIN' THEN\n"
                + "                        KYLIN_FACT.test_count_distinct_bitmap\n"
                + "                        ELSE Cast(NULL AS VARCHAR(1))\n"
                + "                      END) AS TEMP___________299162,\n" + "       Count(DISTINCT CASE\n"
                + "                        WHEN KYLIN_FACT.lstg_format_name = 'Auction' THEN\n"
                + "                        KYLIN_FACT.order_id\n" + "                        ELSE Cast(NULL AS FLOAT)\n"
                + "                      END) AS TEMP_________,\n" + "       Count(DISTINCT lstg_format_name)\n"
                + "FROM  (SELECT price,\n" + "              lstg_site_id,\n" + "              cal_dt,\n"
                + "              test_count_distinct_bitmap,\n" + "              lstg_format_name,\n"
                + "              test_kylin_fact.order_id AS ORDER_ID,\n"
                + "              test_order.order_id      AS test_order_id\n" + "       FROM   test_kylin_fact\n"
                + "              left join test_order\n"
                + "                     ON test_kylin_fact.order_id = test_order.order_id)\n" + "      KYLIN_FACT\n"
                + "      left join(SELECT KYLIN_FACT . lstg_site_id AS X____,\n"
                + "                       SUM(KYLIN_FACT . price)   AS X_measure__0\n"
                + "                FROM  (SELECT price,\n" + "                              lstg_site_id,\n"
                + "                              cal_dt,\n"
                + "                              test_count_distinct_bitmap,\n"
                + "                              lstg_format_name,\n"
                + "                              test_kylin_fact.order_id AS ORDER_ID,\n"
                + "                              test_order.order_id      AS test_order_id\n"
                + "                       FROM   test_kylin_fact\n"
                + "                              left join test_order\n"
                + "                                     ON test_kylin_fact.order_id =\n"
                + "                                        test_order.order_id\n" + "                      )\n"
                + "                      KYLIN_FACT\n" + "                GROUP  BY KYLIN_FACT. lstg_site_id) t0\n"
                + "             ON KYLIN_FACT.lstg_site_id = t0.x____\n"
                + "WHERE  KYLIN_FACT.cal_dt = DATE '2012-01-01' "));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
    }
}
