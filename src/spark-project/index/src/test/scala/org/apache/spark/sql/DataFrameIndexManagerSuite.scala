/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.IndexedDataSource
import org.apache.spark.sql.functions.col

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class DataFrameIndexManagerSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("initialize source") {
    val manager = new DataFrameIndexManager(spark)
    manager.getSource should be (IndexedDataSource.parquet)
  }

  test("set source using format method") {
    val manager = new DataFrameIndexManager(spark)
    manager.format("source")
    manager.getSource should be ("source")
  }

  test("set source in chain of method calls") {
    val manager = new DataFrameIndexManager(spark)
    manager.format("source1").format("source2").format("source3")
    manager.getSource should be ("source3")
  }

  test("initialize options") {
    val manager = new DataFrameIndexManager(spark)
    manager.getOptions() should be (Map.empty[String, String])
  }

  test("set single string option") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key", "value")
    manager.getOptions should be (Map("key" -> "value"))
  }

  test("overwrite option with chaining of methods") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key", "value1").option("key", "value2")
    manager.getOptions should be (Map("key" -> "value2"))
  }

  test("set multiple options with chaining") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key1", "value1").option("key2", "value2").option("key3", "value3")
    manager.getOptions should be (Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))
  }

  test("set long option") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key", 123L)
    manager.getOptions should be (Map("key" -> "123"))
  }

  test("set boolean option") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key1", true).option("key2", false)
    manager.getOptions should be (Map("key1" -> "true", "key2" -> "false"))
  }

  test("set double option") {
    val manager = new DataFrameIndexManager(spark)
    manager.option("key1", 123.0).option("key2", 0.123)
    manager.getOptions should be (Map("key1" -> "123.0", "key2" -> "0.123"))
  }

  test("add bulk options") {
    val manager = new DataFrameIndexManager(spark)
    manager.options(Map("key1" -> "value1", "key2" -> "value2"))
    manager.getOptions should be (Map("key1" -> "value1", "key2" -> "value2"))
  }

  test("set shard by column") {
    val manager = new DataFrameIndexManager(spark)
    manager.shardBy(Array("name"))
    manager.getOptions should be (Map("shardByColumn" -> "name", "support" -> "shard"))
  }

  test("create command - init") {
    val manager = new DataFrameIndexManager(spark)
    val ddl = manager.create
    ddl.getSource should be (manager.getSource)
    ddl.getOptions should be (manager.getOptions)
    ddl.getMode should be (SaveMode.ErrorIfExists)
    ddl.getColumns should be (Nil)
  }

  test("create command - update mode (append)") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.mode("append")
    ddl.getMode should be (SaveMode.Append)
  }

  test("create command - update mode (overwrite)") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.mode("overwrite")
    ddl.getMode should be (SaveMode.Overwrite)
  }

  test("create command - update mode (ignore)") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.mode("ignore")
    ddl.getMode should be (SaveMode.Ignore)
  }

  test("create command - update mode (error)") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.mode("error")
    ddl.getMode should be (SaveMode.ErrorIfExists)
  }

  test("create command - update mode (wrong mode)") {
    val ddl = new DataFrameIndexManager(spark).create
    val err = intercept[UnsupportedOperationException] {
      ddl.mode("wrong mode")
    }
    assert(err.getMessage.contains("Unsupported mode wrong mode"))
  }

  test("create command - update typed mode") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.mode(SaveMode.Overwrite).getMode should be (SaveMode.Overwrite)
    ddl.mode(SaveMode.Append).getMode should be (SaveMode.Append)
    ddl.mode(SaveMode.Ignore).getMode should be (SaveMode.Ignore)
    ddl.mode(SaveMode.ErrorIfExists).getMode should be (SaveMode.ErrorIfExists)
  }

  test("create command - indexBy") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.indexBy(col("a"), col("b"), col("c")).getColumns should be (
      Seq(col("a"), col("b"), col("c")))
  }

  test("create command - indexBy, strings") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.indexBy("a", "b", "c").getColumns should be (Seq(col("a"), col("b"), col("c")))
  }

  test("create command - indexBy, array of columns") {
    val ddl = new DataFrameIndexManager(spark).create
    val columns = Array(col("a"), col("b"), col("c"))
    ddl.indexBy(columns).getColumns should be (Seq(col("a"), col("b"), col("c")))
  }

  test("create command - indexBy, array of columns is empty") {
    val ddl = new DataFrameIndexManager(spark).create
    val err = intercept[IllegalArgumentException] {
      ddl.indexBy(Array.empty[Column])
    }
    assert(err.getMessage.contains("At least one column is required"))
  }

  test("create command - indexBy, array of strings") {
    val ddl = new DataFrameIndexManager(spark).create
    val columns = Array[String]("a", "b", "c")
    ddl.indexBy(columns).getColumns should be (Seq(col("a"), col("b"), col("c")))
  }

  test("create command - indexBy, array of strings is empty") {
    val ddl = new DataFrameIndexManager(spark).create
    val err = intercept[IllegalArgumentException] {
      ddl.indexBy(Array.empty[String])
    }
    assert(err.getMessage.contains("At least one column is required"))
  }

  test("create command - indexBy, java list of strings") {
    val ddl = new DataFrameIndexManager(spark).create
    val columns = new java.util.ArrayList[String]()
    columns.add("a")
    columns.add("b")
    columns.add("c")
    ddl.indexBy(columns).getColumns should be (Seq(col("a"), col("b"), col("c")))
  }

  test("create command - indexBy, java list of strings is empty") {
    val ddl = new DataFrameIndexManager(spark).create
    val err = intercept[IllegalArgumentException] {
      ddl.indexBy(new java.util.ArrayList[String]())
    }
    assert(err.getMessage.contains("At least one column is required"))
  }

  test("create command - indexByAll") {
    val ddl = new DataFrameIndexManager(spark).create
    ddl.indexByAll().getColumns should be (Nil)
  }

  test("exists command - init") {
    val manager = new DataFrameIndexManager(spark).option("key", "value")
    val ddl = manager.exists
    ddl.getSource should be (manager.getSource)
    ddl.getOptions should be (manager.getOptions)
  }

  test("delete command - init") {
    val manager = new DataFrameIndexManager(spark).option("key", "value")
    val ddl = manager.delete
    ddl.getSource should be (manager.getSource)
    ddl.getOptions should be (manager.getOptions)
  }
}
