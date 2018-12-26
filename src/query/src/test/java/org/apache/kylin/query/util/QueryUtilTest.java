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

package org.apache.kylin.query.util;

import java.util.Properties;

import io.kyligence.kap.query.util.SparkSQLFunctionConverter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.junit.Assert;
import org.junit.Test;

public class QueryUtilTest {

    @Test
    public void testMassageSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());

        String sql = "SELECT * FROM TABLE";
        String newSql = QueryUtil.massageSql(config, sql, "", 100, 20, "");
        Assert.assertEquals("SELECT * FROM TABLE\nLIMIT 100\nOFFSET 20", newSql);

        String sql2 = "SELECT SUM({fn convert(0, INT)}) from TABLE";
        String newSql2 = QueryUtil.massageSql(config, sql2, "", 0, 0, "");
        // SUM({fn convert(0, INT)}) -> SUM(0) -> 0 * COUNT(1)
        Assert.assertEquals("SELECT 0 * COUNT(1) from TABLE", newSql2);
    }
    }

    @Test
    public void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";
        String massagedSql = QueryUtil.massagePushDownSql(config, sql, "", "default", false);
            String expectedSql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM(CAST(0 AS BIGINT)) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";
        Assert.assertEquals(expectedSql, massagedSql);
    }
    }

    @Test
    public void testInit() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());
        Assert.assertEquals(0, QueryUtil.queryTransformers.size());
        QueryUtil.initQueryTransformersIfNeeded(config);
        Assert.assertEquals(1, QueryUtil.queryTransformers.size());
        Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof DefaultQueryTransformer);

        config.setProperty("kylin.query.transformers", KeywordDefaultDirtyHack.class.getCanonicalName());
        QueryUtil.initQueryTransformersIfNeeded(config);
        Assert.assertEquals(1, QueryUtil.queryTransformers.size());
        Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof KeywordDefaultDirtyHack);
    }
}
}
