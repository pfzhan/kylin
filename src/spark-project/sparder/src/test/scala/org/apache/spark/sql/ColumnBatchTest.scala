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

import java.sql.{Date, Timestamp}

import com.google.common.collect.Lists
import org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate
import org.apache.spark.sql.catalyst.expressions.KapSubtractMonths
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

// scalastyle:off
class ColumnBatchTest extends SparderBaseFunSuite with SharedSparkSession {
  test("basic") {
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("birth", DateType, nullable = true),
        StructField("time", TimestampType, nullable = true)
      ))

    val data = Seq(
      // only String : Caused by: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of date
      Row(1,
        Date.valueOf("2012-12-12"),
        Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(2,
        Date.valueOf("2016-12-14"),
        Timestamp.valueOf("2016-12-14 03:03:00"))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-12 00:00:00.12345"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    df.select(org.apache.spark.sql.functions.add_months(lit("2012-01-31"), 1))
      .take(1)
      .foreach(println)
    df.select(
      org.apache.spark.sql.functions.add_months(lit("2012-01-31 10:10:10"),
        1))
      .take(1)
      .foreach(println)
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-12 00:00:00"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    //    df.select(from_utc_timestamp(to_utc_timestamp(lit("2012-12-11 16:00:00"), TimeZone.getTimeZone("UTC").getID), TimeZone.getDefault.getID).cast(LongType)).take(1).foreach(println)
    spark.close()
  }

  test("addMonths") {
    println(KapDateTimeUtils.dateAddMonths(ymdToUnixDate(2012, 3, 31), 23))
    println(ymdToUnixDate(2012, 2, 29))
    println(ymdToUnixDate(2014, 2, 28))

  }

  test("KapSubtractMonths") {
    val df = mockDFForLit
    val tsc1 = lit("2012-11-12 12:12:12.0").cast(TimestampType)
    val tsc2 = lit("2012-12-12 12:12:12.0").cast(TimestampType)
    val column = Column(KapSubtractMonths(tsc1.expr, tsc2.expr))
    assert(true, df.select(column).head().getInt(0) == -1)
  }

  def mockDFForLit: DataFrame = {
    val row = Row("a")
    val df = spark.createDataFrame(
      Lists.newArrayList(row),
      StructType(Array(StructField("m", StringType, nullable = true))))
    df
  }
}