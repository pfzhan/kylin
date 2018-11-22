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

package com.github.lightcopy.api;

import java.io.File;
import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;

import com.github.lightcopy.QueryContext;

/**
 * Java API suite for tests with default index configuration.
 */
public class JavaApiSuite {
  // configuration for metastore, should be updated once changed
  static String METASTORE_LOCATION = "spark.sql.index.metastore";
  SparkSession spark;

  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Before
  public void startSparkSession() {
    if (spark != null) {
      stopSparkSession();
    }
    spark = SparkSession.
      builder().
      master("local[*]").
      appName("Java unit-tests").
      getOrCreate();
  }

  @After
  public void stopSparkSession() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // create Parquet datasource table
  public Dataset<Row> sampleParquet(SparkSession spark, String tablePath) {
    Dataset<Row> df = spark.sql(
      "select 1 as col1, 'a' as col2 union " +
      "select 2 as col1, 'b' as col2 union " +
      "select 3 as col1, 'c' as col2").coalesce(1);
    df.write().parquet(tablePath);
    return df;
  }

  // create managed Parquet table
  public Dataset<Row> sampleTable(SparkSession spark, String tableName) {
    Dataset<Row> df = spark.sql(
      "select 1 as col1, 'a' as col2 union " +
      "select 2 as col1, 'b' as col2 union " +
      "select 3 as col1, 'c' as col2").coalesce(1);
    df.write().saveAsTable(tableName);
    return df;
  }

  @Test
  public void testCreateIndexByAll() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexByAll().parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByColumnNames() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexBy(new String[] { "col1", "col2" }).parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByColumns() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    Column col1 = new Column("col1");
    Column col2 = new Column("col2");
    Column[] columns = new Column[] { col1, col2 };
    context.index().create().indexBy(columns).parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByOverwriteMode() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().mode("overwrite").indexByAll().parquet(tablePath.toString());
    context.index().create().mode(SaveMode.Overwrite).indexByAll().parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateExistsDeleteIndex() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    // create index for all available columns
    context.index().create().indexByAll().parquet(tablePath.toString());
    assertEquals(context.index().exists().parquet(tablePath.toString()), true);
    // delete index and check that it has been removed from metastore
    context.index().delete().parquet(tablePath.toString());
    assertEquals(context.index().exists().parquet(tablePath.toString()), false);
  }

  @Test
  public void testCreateQueryIndex() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexByAll().parquet(tablePath.toString());
    Dataset<Row> df = context.index().parquet(tablePath.toString()).filter("col2 = 'c'");
    assertEquals(df.count(), 1);
  }

  @Test
  public void testCreateExistsDeleteCatalogIndex() throws IOException {
    spark.conf().set("spark.sql.sources.default", "parquet");
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    String tableName = "test_parquet_table";
    sampleTable(spark, tableName);

    try {
      // create index for all available columns
      context.index().create().indexByAll().table(tableName);
      assertEquals(context.index().exists().table(tableName), true);
      // delete index and check that it has been removed from metastore
      context.index().delete().table(tableName);
      assertEquals(context.index().exists().table(tableName), false);
    } finally {
      spark.sql("drop table " + tableName);
    }
  }

  @Test
  public void testCreateQueryCatalogIndex() throws IOException {
    spark.conf().set("spark.sql.sources.default", "parquet");
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    String tableName = "test_parquet_table";
    sampleTable(spark, tableName);

    try {
      context.index().create().indexByAll().table(tableName);
      Dataset<Row> df = context.index().table(tableName).filter("col2 = 'c'");
      assertEquals(df.count(), 1);
    } finally {
      spark.sql("drop table " + tableName);
    }
  }
}
