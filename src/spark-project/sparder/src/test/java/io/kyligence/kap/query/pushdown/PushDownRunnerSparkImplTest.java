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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.query.pushdown;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class PushDownRunnerSparkImplTest extends NLocalFileMetadataTestCase {

    SparkSession ss;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        SparderEnv.setSparkSession(ss);
        StructType schema = new StructType();
        schema = schema.add("TRANS_ID", DataTypes.LongType, false);
        schema = schema.add("ORDER_ID", DataTypes.LongType, false);
        schema = schema.add("CAL_DT", DataTypes.DateType, false);
        schema = schema.add("LSTG_FORMAT_NAME", DataTypes.StringType, false);
        schema = schema.add("LEAF_CATEG_ID", DataTypes.LongType, false);
        schema = schema.add("LSTG_SITE_ID", DataTypes.IntegerType, false);
        schema = schema.add("SLR_SEGMENT_CD", DataTypes.FloatType, false);
        schema = schema.add("SELLER_ID", DataTypes.LongType, false);
        schema = schema.add("PRICE", DataTypes.createDecimalType(19, 4), false);
        schema = schema.add("ITEM_COUNT", DataTypes.DoubleType, false);
        schema = schema.add("TEST_COUNT_DISTINCT_BITMAP", DataTypes.StringType, false);
        ss.read().schema(schema).csv("../../examples/test_case_data/localmeta_n/data/DEFAULT.TEST_KYLIN_FACT.csv")
                .createOrReplaceTempView("TEST_KYLIN_FACT");
    }

    @After
    public void after() throws Exception {
        ss.stop();
        cleanupTestMetadata();
    }

    @Test
    public void testCast() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        List<String> queries = new ArrayList<>();
        queries.add("SELECT cast(ORDER_ID as integer) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT cast(LSTG_SITE_ID as long) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT cast(LSTG_SITE_ID as short) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT CAST(ORDER_ID AS VARCHAR) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT CAST(ORDER_ID AS char) FROM TEST_KYLIN_FACT limit 10");
        queries.add("select SELLER_ID,ITEM_COUNT,sum(price)\n" + //
                "from (\n" + //
                "SELECT SELLER_ID, ITEM_COUNT,price\n" + //
                "\t, concat(concat(CAST(year(CAST(CAL_DT AS date)) AS varchar), '-'), CAST(month(CAST(CAL_DT AS date)) AS varchar)) AS prt_mth\n" + //
                "FROM TEST_KYLIN_FACT) \n" + //
                "group by SELLER_ID,ITEM_COUNT,price limit 10"); //

        queries.add("select SELLER_ID,ITEM_COUNT,sum(price)\n" + //
                "from (\n" + //
                "SELECT SELLER_ID, ITEM_COUNT,price\n" + //
                "\t, concat(concat(CAST(year(CAST(CAL_DT AS date)) AS char), '-'), CAST(month(CAST(CAL_DT AS date)) AS varchar)) AS prt_mth\n" + //
                "FROM TEST_KYLIN_FACT) \n" + //
                "group by SELLER_ID,ITEM_COUNT,price limit 10");

        queries.forEach(q -> {
            returnRows.clear();
            pushDownRunnerSpark.executeQuery(q, returnRows, returnColumnMeta, null);
            Assert.assertEquals(10, returnRows.size());
        });

    }

    @Test
    public void testPushDownRunnerSpark() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select * from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta, null);

        Assert.assertEquals(10000, returnRows.size());
        Assert.assertEquals(11, returnColumnMeta.size());
        Assert.assertEquals(QueryContext.PUSHDOWN_HIVE, pushDownRunnerSpark.getName());
    }

    @Test
    public void testPushDownRunnerSparkWithDotColumn() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select TEST_KYLIN_FACT.price as `TEST_KYLIN_FACT.price` from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta, null);

        Assert.assertEquals(10000, returnRows.size());
        Assert.assertEquals(1, returnColumnMeta.size());
        Assert.assertEquals(QueryContext.PUSHDOWN_HIVE, pushDownRunnerSpark.getName());
    }

    @Test
    public void testSelectTwoSameExpr() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select sum(price), sum(price) from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta, null);

        Assert.assertEquals(1, returnRows.size());
        Assert.assertEquals(2, returnColumnMeta.size());
        Assert.assertEquals(QueryContext.PUSHDOWN_HIVE, pushDownRunnerSpark.getName());
    }

    @Test
    public void testCaseSensitiveOnAlias() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String alias = "OrderId";
        String sql = "SELECT cast(ORDER_ID as integer) as " + alias + " FROM TEST_KYLIN_FACT limit 10";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta, null);
        Assert.assertEquals(returnColumnMeta.get(0).getName(), alias);
    }
}
