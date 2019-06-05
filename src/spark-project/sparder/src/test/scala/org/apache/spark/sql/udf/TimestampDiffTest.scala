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

package org.apache.spark.sql.udf

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.TimestampDiff
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class TimestampDiffTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    val function = FunctionEntity(expression[TimestampDiff]("TIMESTAMPDIFF"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test diff between date and date") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , date'2017-02-28')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , date'2017-02-27')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , date'2016-04-01')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , date'2016-03-31')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , date'2016-03-30')", Seq("1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , date'2016-01-30')", Seq("-1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-28' , date'2016-01-30')", Seq("0"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-28' , date'2017-02-28')", Seq("12"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', date'2016-02-01' , date'2016-02-07')", Seq("0"))
    verifyResult("select timestampdiff('WEEK', date'2016-02-01' , date'2016-02-08')", Seq("1"))

    // DAY
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , date'2016-02-02')", Seq("1"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , date'2016-02-02')", Seq("24"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , date'2016-02-02')", Seq("1440"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , date'2016-02-02')", Seq("86400"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , date'2016-02-01')", Seq("0"))
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , date'2016-02-02')", Seq("86400000"))
  }

  test("test diff between date and timestamp") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , timestamp'2017-02-28 00:00:00.000')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', date'2016-02-29' , timestamp'2017-02-27 00:00:00.000')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , timestamp'2016-04-01 00:00:00.000')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', date'2016-01-01' , timestamp'2016-03-31 00:00:00.000')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , timestamp'2016-01-29 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('MONTH', date'2016-02-29' , timestamp'2016-01-29 00:00:00.001')", Seq("0"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', date'2016-02-08' , timestamp'2016-02-01 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('WEEK', date'2016-02-08' , timestamp'2016-02-01 00:00:00.001')", Seq("0"))

    // DAY
    verifyResult("select timestampdiff('DAY', date'2016-02-02' , timestamp'2016-02-01 00:00:00.000')", Seq("-1"))
    verifyResult("select timestampdiff('DAY', date'2016-02-02' , timestamp'2016-02-01 00:00:00.001')", Seq("0"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', date'2016-02-01' , timestamp'2016-02-01 01:00:00.000')", Seq("1"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', date'2016-02-01' , timestamp'2016-02-01 00:01:00.000')", Seq("1"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', date'2016-02-01' , timestamp'2016-02-01 00:00:01.000')", Seq("1"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', date'2016-02-01' , timestamp'2016-02-01 00:00:00.001')", Seq("1"))
  }

  test("test diff between timestamp and date") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , date'2017-02-28')", Seq("1"))
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , date'2017-02-27')", Seq("0"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , date'2016-04-01')", Seq("1"))
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , date'2016-03-31')", Seq("0"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', timestamp'2016-02-29 00:00:00.000' , date'2016-01-29')", Seq("-1"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', timestamp'2016-02-08 00:00:00.000' , date'2016-02-01')", Seq("-1"))

    // DAY
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-02 00:00:00.000' , date'2016-02-01')", Seq("-1"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 01:00:00.000' , date'2016-02-01')", Seq("-1"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:01:00.000' , date'2016-02-01')", Seq("-1"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:01.000' , date'2016-02-01')", Seq("-1"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', timestamp'2016-02-01 00:00:00.001' , date'2016-02-01')", Seq("-1"))
  }

  test("test diff between timestamp and timestamp") {
    // YEAR
    verifyResult("select timestampdiff('YEAR', timestamp'2016-02-29 00:00:00.000' , timestamp'2017-02-28 00:00:00.000')", Seq("1"))

    // QUARTER
    verifyResult("select timestampdiff('QUARTER', timestamp'2016-01-01 00:00:00.000' , timestamp'2016-04-01 00:00:00.000')", Seq("1"))

    // MONTH
    verifyResult("select timestampdiff('MONTH', timestamp'2016-01-29 00:00:00.000' , timestamp'2016-02-29 00:00:00.000')", Seq("1"))

    // WEEK
    verifyResult("select timestampdiff('WEEK', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-08 00:00:00.000')", Seq("1"))

    // DAY
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-02 00:00:00.000')", Seq("1"))

    // HOUR
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-02 01:00:00.000')", Seq("25"))
    verifyResult("select timestampdiff('HOUR', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-02 01:00:00.000')", Seq("24"))

    // MINUTE
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 01:01:00.000')", Seq("61"))
    verifyResult("select timestampdiff('MINUTE', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-01 01:01:00.000')", Seq("60"))

    // SECOND
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:01:01.000')", Seq("61"))
    verifyResult("select timestampdiff('SECOND', timestamp'2016-02-01 00:00:00.001' , timestamp'2016-02-01 00:01:01.000')", Seq("60"))

    // FRAC_SECOND
    verifyResult("select timestampdiff('FRAC_SECOND', timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:00:00.011')", Seq("11"))
  }

  test("test null and illegal argument") {
    verifyResult("select timestampdiff(null, timestamp'2016-02-01 00:00:00.000' , timestamp'2016-02-01 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff(null, date'2016-02-01' , timestamp'2016-02-01 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff(null, timestamp'2016-02-01 00:00:00.000' , date'2016-02-01')", Seq("null"))
    verifyResult("select timestampdiff(null, date'2016-02-01' , date'2016-02-01')", Seq("null"))

    verifyResult("select timestampdiff('DAY', null, timestamp'2016-02-02 00:00:00.011')", Seq("null"))
    verifyResult("select timestampdiff('DAY', null, date'2016-02-01')", Seq("null"))
    verifyResult("select timestampdiff('DAY', timestamp'2016-02-01 00:00:00.000' , null)", Seq("null"))
    verifyResult("select timestampdiff('DAY', date'2016-02-01' , null)", Seq("null"))

    try {
      verifyResult("select timestampdiff('ILLEGAL', date'2016-02-01', date'2016-01-31')", Seq("0"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Illegal unit: ILLEGAL, only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH," +
          " WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY, HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND," +
          " FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("unit", StringType),
      StructField("timestamp1", TimestampType),
      StructField("timestamp2", TimestampType),
      StructField("date1", DateType),
      StructField("date2", DateType)
    ))
    val rdd = sc.parallelize(Seq(
      Row("MONTH", Timestamp.valueOf("2016-01-31 01:01:01.001"), Timestamp.valueOf("2016-02-29 01:01:01.001"),
        Date.valueOf("2016-01-31"), Date.valueOf("2016-02-29"))
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_timestamp_diff")
    verifyResult("select timestampdiff(unit, date1, date2) from global_temp.test_timestamp_diff", Seq("1"))
    verifyResult("select timestampdiff(unit, date1, timestamp2) from global_temp.test_timestamp_diff", Seq("1"))
    verifyResult("select timestampdiff(unit, timestamp1, date2) from global_temp.test_timestamp_diff", Seq("0"))
    verifyResult("select timestampdiff(unit, timestamp1, timestamp2) from global_temp.test_timestamp_diff", Seq("1"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(row => row.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
