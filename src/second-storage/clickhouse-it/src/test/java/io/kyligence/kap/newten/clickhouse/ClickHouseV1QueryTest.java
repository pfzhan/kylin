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
package io.kyligence.kap.newten.clickhouse;

import java.sql.Connection;
import java.util.List;

import scala.collection.JavaConversions;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;

@Slf4j
public class ClickHouseV1QueryTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.beforeClass();
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }

    @Test
    public void testMultipleShard() throws Exception {
        boolean result = ClickHouseUtils.prepare2Instances(true,
                (JdbcDatabaseContainer<?> clickhouse1, Connection connection1,
                 JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {

            List<String> shardList =
                    ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConversions.asScalaBuffer(shardList));
            Dataset<Row> df = ss.read()
              .format("org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider")
              .option("url", clickhouse1.getJdbcUrl())
              .option("dbtable", ClickHouseUtils.PrepareTestData.db + "." + ClickHouseUtils.PrepareTestData.table)
              .option(ShardOptions$.MODULE$.SHARD_URLS(), shards).load();

            Assert.assertEquals(7, df.count());
            df.select("n3").show();
            return true;
        });
        Assert.assertTrue(result);
    }
}

