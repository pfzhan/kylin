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
package io.kyligence.kap.engine.spark;

import java.io.Serializable;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;

@SuppressWarnings("serial")
public class NLocalSparkWithCSVDataTest extends NLocalSparkWithMetaTest implements Serializable {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        DataFrameReader read = ss.read();
        Dataset<Row> lineorder = read.csv("../examples/test_metadata/data/SSB.P_LINEORDER.csv").toDF("LO_ORDERKEY",
                "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIOTITY",
                "LO_SHIPPRIOTITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
                "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE", "V_REVENUE");
        lineorder.createOrReplaceTempView("p_lineorder");

        Dataset<Row> dates = read.csv("../examples/test_metadata/data/SSB.DATES.csv").toDF("D_DATEKEY", "D_DATE",
                "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
                "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
                "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL");
        dates.createOrReplaceTempView("dates");

        Dataset<Row> customer = read.csv("../examples/test_metadata/data/SSB.CUSTOMER.csv").toDF("C_CUSTKEY", "C_NAME",
                "C_ADDRESS", "C_CITY", "C_NATION", "C_REGION", "C_PHONE", "C_MKTSEGMENT");
        customer.createOrReplaceTempView("customer");

        Dataset<Row> part = read.csv("../examples/test_metadata/data/SSB.PART.csv").toDF("P_PARTKEY", "P_NAME",
                "P_MFGR", "P_CATEGORY", "P_BRAND", "P_COLOR", "P_TYPE", "P_SIZE", "P_CONTAINER");
        part.createOrReplaceTempView("part");

        Dataset<Row> supplier = read.csv("../examples/test_metadata/data/SSB.SUPPLIER.csv").toDF("S_SUPPKEY", "S_NAME",
                "S_ADDRESS", "S_CITY", "S_NATION", "S_REGION", "S_PHONE");
        supplier.createOrReplaceTempView("supplier");

        Dataset<Row> testKylinFact = read.csv("../examples/test_metadata/data/DEFAULT.TEST_KYLIN_FACT.csv").toDF(
                "TRANS_ID", "ORDER_ID", "CAL_DT", "LSTG_FORMAT_NAME", "LEAF_CATEG_ID", "LSTG_SITE_ID", "SLR_SEGMENT_CD",
                "SELLER_ID", "PRICE", "ITEM_COUNT", "TEST_COUNT_DISTINCT_BITMAP");
        testKylinFact.createOrReplaceTempView("TEST_KYLIN_FACT");

        Dataset<Row> testCalDt = read.csv("../examples/test_metadata/data/EDW.TEST_CAL_DT.csv").toDF("CAL_DT",
                "YEAR_BEG_DT", "QTR_BEG_DT", "MONTH_BEG_DT", "WEEK_BEG_DT", "AGE_FOR_YEAR_ID", "AGE_FOR_QTR_ID",
                "AGE_FOR_MONTH_ID", "AGE_FOR_WEEK_ID", "AGE_FOR_DT_ID", "AGE_FOR_RTL_YEAR_ID", "AGE_FOR_RTL_QTR_ID",
                "AGE_FOR_RTL_MONTH_ID", "AGE_FOR_RTL_WEEK_ID", "AGE_FOR_CS_WEEK_ID", "DAY_OF_CAL_ID", "DAY_OF_YEAR_ID",
                "DAY_OF_QTR_ID", "DAY_OF_MONTH_ID", "DAY_OF_WEEK_ID", "WEEK_OF_YEAR_ID", "WEEK_OF_CAL_ID",
                "MONTH_OF_QTR_ID", "MONTH_OF_YEAR_ID", "MONTH_OF_CAL_ID", "QTR_OF_YEAR_ID", "QTR_OF_CAL_ID",
                "YEAR_OF_CAL_ID", "YEAR_END_DT", "QTR_END_DT", "MONTH_END_DT", "WEEK_END_DT", "CAL_DT_NAME",
                "CAL_DT_DESC", "CAL_DT_SHORT_NAME", "YTD_YN_ID", "QTD_YN_ID", "MTD_YN_ID", "WTD_YN_ID", "SEASON_BEG_DT",
                "DAY_IN_YEAR_COUNT", "DAY_IN_QTR_COUNT", "DAY_IN_MONTH_COUNT", "DAY_IN_WEEK_COUNT", "RTL_YEAR_BEG_DT",
                "RTL_QTR_BEG_DT", "RTL_MONTH_BEG_DT", "RTL_WEEK_BEG_DT", "CS_WEEK_BEG_DT", "CAL_DATE", "DAY_OF_WEEK",
                "MONTH_ID", "PRD_DESC", "PRD_FLAG", "PRD_ID", "PRD_IND", "QTR_DESC", "QTR_ID", "QTR_IND", "RETAIL_WEEK",
                "RETAIL_YEAR", "RETAIL_START_DATE", "RETAIL_WK_END_DATE", "WEEK_IND", "WEEK_NUM_DESC", "WEEK_BEG_DATE",
                "WEEK_END_DATE", "WEEK_IN_YEAR_ID", "WEEK_ID", "WEEK_BEG_END_DESC_MDY", "WEEK_BEG_END_DESC_MD",
                "YEAR_ID", "YEAR_IND", "CAL_DT_MNS_1YEAR_DT", "CAL_DT_MNS_2YEAR_DT", "CAL_DT_MNS_1QTR_DT",
                "CAL_DT_MNS_2QTR_DT", "CAL_DT_MNS_1MONTH_DT", "CAL_DT_MNS_2MONTH_DT", "CAL_DT_MNS_1WEEK_DT",
                "CAL_DT_MNS_2WEEK_DT", "CURR_CAL_DT_MNS_1YEAR_YN_ID", "CURR_CAL_DT_MNS_2YEAR_YN_ID",
                "CURR_CAL_DT_MNS_1QTR_YN_ID", "CURR_CAL_DT_MNS_2QTR_YN_ID", "CURR_CAL_DT_MNS_1MONTH_YN_ID",
                "CURR_CAL_DT_MNS_2MONTH_YN_ID", "CURR_CAL_DT_MNS_1WEEK_YN_IND", "CURR_CAL_DT_MNS_2WEEK_YN_IND",
                "RTL_MONTH_OF_RTL_YEAR_ID", "RTL_QTR_OF_RTL_YEAR_ID", "RTL_WEEK_OF_RTL_YEAR_ID", "SEASON_OF_YEAR_ID",
                "YTM_YN_ID", "YTQ_YN_ID", "YTW_YN_ID", "CAL_DT_CRE_DATE", "CAL_DT_CRE_USER", "CAL_DT_UPD_DATE",
                "CAL_DT_UPD_USER");
        testCalDt.createOrReplaceTempView("TEST_CAL_DT");

        Dataset<Row> TEST_CATEGORY_GROUPINGS = read
                .csv("../examples/test_metadata/data/DEFAULT.TEST_CATEGORY_GROUPINGS.csv").toDF("LEAF_CATEG_ID",
                        "LEAF_CATEG_NAME", "SITE_ID", "CATEG_BUSN_MGR", "CATEG_BUSN_UNIT", "REGN_CATEG",
                        "USER_DEFINED_FIELD1", "USER_DEFINED_FIELD3", "GROUPINGS_CRE_DATE", "UPD_DATE",
                        "GROUPINGS_CRE_USER", "UPD_USER", "META_CATEG_ID", "META_CATEG_NAME", "CATEG_LVL2_ID",
                        "CATEG_LVL3_ID", "CATEG_LVL4_ID", "CATEG_LVL5_ID", "CATEG_LVL6_ID", "CATEG_LVL7_ID",
                        "CATEG_LVL2_NAME", "CATEG_LVL3_NAME", "CATEG_LVL4_NAME", "CATEG_LVL5_NAME", "CATEG_LVL6_NAME",
                        "CATEG_LVL7_NAME", "CATEG_FLAGS", "ADULT_CATEG_YN", "DOMAIN_ID", "USER_DEFINED_FIELD5",
                        "VCS_ID", "GCS_ID", "MOVE_TO", "SAP_CATEGORY_ID", "SRC_ID", "BSNS_VRTCL_NAME");
        TEST_CATEGORY_GROUPINGS.createOrReplaceTempView("TEST_CATEGORY_GROUPINGS");

        Dataset<Row> TEST_SITES = read.csv("../examples/test_metadata/data/EDW.TEST_SITES.csv").toDF("SITE_ID",
                "SITE_NAME", "SITE_DOMAIN_CODE", "DFAULT_LSTG_CURNCY", "EOA_EMAIL_CSTMZBL_SITE_YN_ID", "SITE_CNTRY_ID",
                "CRE_DATE", "SITES_UPD_DATE", "CRE_USER", "SITES_UPD_USER");
        TEST_SITES.createOrReplaceTempView("TEST_SITES");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }
}
