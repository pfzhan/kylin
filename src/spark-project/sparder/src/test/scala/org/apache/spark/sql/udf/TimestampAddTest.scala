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
import org.apache.spark.sql.catalyst.expressions.TimestampAdd
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class TimestampAddTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()

    val function = FunctionEntity(expression[TimestampAdd]("TIMESTAMPADD"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test add on date") {
    // YEAR
    verifyResult("select timestampadd('YEAR', 1 , date'2016-02-29')", Seq("2017-02-28"))

    // QUARTER
    verifyResult("select timestampadd('QUARTER', 1L , date'2016-02-29')", Seq("2016-05-29"))

    // MONTH
    verifyResult("select timestampadd('MONTH', 1 , date'2016-01-31')", Seq("2016-02-29"))

    // WEEK
    verifyResult("select timestampadd('WEEK', 1L , date'2016-01-31')", Seq("2016-02-07"))

    // DAY
    verifyResult("select timestampadd('DAY', 1 , date'2016-01-31')", Seq("2016-02-01"))

    // HOUR
    verifyResult("select timestampadd('HOUR', 1L , date'2016-01-31')", Seq("2016-01-31"))

    // MINUTE
    verifyResult("select timestampadd('MINUTE', 1 , date'2016-01-31')", Seq("2016-01-31"))

    // SECOND
    verifyResult("select timestampadd('SECOND', 1L , date'2016-01-31')", Seq("2016-01-31"))

    // FRAC_SECOND
    verifyResult("select timestampadd('FRAC_SECOND', 1 , date'2016-01-31')", Seq("2016-01-31"))
  }

  test("test add on timestamp") {
    // YEAR
    verifyResult("select timestampadd('YEAR', 1 , timestamp'2016-02-29 01:01:01.001')", Seq("2017-02-28 01:01:01.001"))

    // QUARTER
    verifyResult("select timestampadd('QUARTER', 1L , timestamp'2016-02-29 01:01:01.001')", Seq("2016-05-29 01:01:01.001"))

    // MONTH
    verifyResult("select timestampadd('MONTH', 1 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-29 01:01:01.001"))

    // WEEK
    verifyResult("select timestampadd('WEEK', 1L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-07 01:01:01.001"))

    // DAY
    verifyResult("select timestampadd('DAY', 1 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-01 01:01:01.001"))

    // HOUR
    verifyResult("select timestampadd('HOUR', 25L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-02-01 02:01:01.001"))

    // MINUTE
    verifyResult("select timestampadd('MINUTE', 61 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 02:02:01.001"))

    // SECOND
    verifyResult("select timestampadd('SECOND', 61L , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 01:02:02.001"))

    // FRAC_SECOND
    verifyResult("select timestampadd('FRAC_SECOND', 1001 , timestamp'2016-01-31 01:01:01.001')", Seq("2016-01-31 01:01:02.002"))
  }

  test("test null and illegal argument") {
    verifyResult("select timestampadd(null, 1 , timestamp'2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd(null, 1L , date'2016-01-31')", Seq("null"))

    verifyResult("select timestampadd('DAY', null , timestamp'2016-01-31 01:01:01.001')", Seq("null"))
    verifyResult("select timestampadd('DAY', null , date'2016-01-31')", Seq("null"))

    verifyResult("select timestampadd('DAY', 1 , null)", Seq("null"))

    try {
      verifyResult("select timestampadd('ILLEGAL', 1 , date'2016-01-31')", Seq("null"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Illegal unit: ILLEGAL, only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH," +
          " WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY, HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND," +
          " FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }

    try {
      verifyResult("select timestampadd('ILLEGAL', 2147483648, date'2016-01-31')", Seq("0"))
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage == "Increment(2147483648) is greater than Int.MaxValue")
    }
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("c_long", LongType),
      StructField("c_int", IntegerType),
      StructField("unit", StringType),
      StructField("c_timestamp", TimestampType),
      StructField("c_date", DateType)
    ))
    val rdd = sc.parallelize(Seq(
      Row(1L, 2, "YEAR", Timestamp.valueOf("2016-02-29 01:01:01.001"), Date.valueOf("2016-02-29"))
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_timestamp_add")
    verifyResult("select timestampadd(unit, c_long, c_timestamp) from global_temp.test_timestamp_add", Seq("2017-02-28 01:01:01.001"))
    verifyResult("select timestampadd(unit, c_int, c_date) from global_temp.test_timestamp_add", Seq("2018-02-28"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(row => row.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
