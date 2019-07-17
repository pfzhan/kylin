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
package org.apache.spark.sql

import java.util.TimeZone

import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class SparkTimezoneTest extends SparderBaseFunSuite with SharedSparkSession {

  val defaultTimezone: TimeZone = TimeZone.getDefault

  override def afterAll(): Unit = {
    TimeZone.setDefault(defaultTimezone)
  }

  val schema = StructType(
    Array(
      StructField("id", IntegerType, nullable = true),
      StructField("birth", DateType, nullable = true),
      StructField("time", TimestampType, nullable = true)
    ))

  test("read csv") {
    val path = "./src/test/resources/persisted_df/timezone/csv"
    assertTime(path, "csv", shouldBeEqual = false)
  }

  test("read orc") {
    val path = "./src/test/resources/persisted_df/timezone/orc"
    assertTime(path, "orc", shouldBeEqual = false)
  }

  test("read csv written by spark ") {
    val path = "./src/test/resources/persisted_df/timezone/spark_write_csv"
    assertTime(path, "csv", shouldBeEqual = true)
  }

  test("read parquet") {
    val path = "./src/test/resources/persisted_df/timezone/parquet"
    assertTime(path, "parquet", shouldBeEqual = true)
  }

  def assertTime(path: String, source: String, shouldBeEqual: Boolean): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val ts0 = getTime(spark.read.schema(schema).format(source).load(path))

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    val ts1 = getTime(spark.read.schema(schema).format(source).load(path))

    if (shouldBeEqual) {
      assert(ts0 == ts1)
    } else {
      assert(ts0 != ts1)
    }
  }

  private def getTime(df: DataFrame): Long = {
    df.select(col("time")).head().getTimestamp(0).getTime
  }

}
