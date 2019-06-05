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

import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.Truncate
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{FunctionEntity, Row}
import org.scalatest.BeforeAndAfterAll

class TruncateTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    val function = FunctionEntity(expression[Truncate]("TRUNCATE"))
    spark.sessionState.functionRegistry.registerFunction(function.name, function.info, function.builder)
  }

  test("test truncate long") {
    verifyResult("select truncate(12345L, 10)", Seq("12345"))
  }

  test("test truncate int") {
    verifyResult("select truncate(12345, 10)", Seq("12345"))
  }

  test("test truncate double") {
    verifyResult("select truncate(123.45d, 3)", Seq("123.45"))
    verifyResult("select truncate(123.45d, 2)", Seq("123.45"))
    verifyResult("select truncate(123.45d, 1)", Seq("123.4"))
    verifyResult("select truncate(123.45d, 0)", Seq("123.0"))
  }

  test("test truncate decimal") {
    verifyResult("select truncate(123.45, 3)", Seq("123.45"))
    verifyResult("select truncate(123.45, 2)", Seq("123.45"))
    verifyResult("select truncate(123.45, 1)", Seq("123.40"))
    verifyResult("select truncate(123.45, 0)", Seq("123.00"))
  }

  test("test null") {
    verifyResult("select truncate(null, 3)", Seq("null"))
    verifyResult("select truncate(12345L, null)", Seq("null"))
    verifyResult("select truncate(12345, null)", Seq("null"))
    verifyResult("select truncate(123.45d, null)", Seq("null"))
    verifyResult("select truncate(123.45, null)", Seq("null"))
  }

  test("test codegen") {
    val schema = StructType(List(
      StructField("c_int", IntegerType),
      StructField("c_long", LongType),
      StructField("c_double", DoubleType),
      StructField("c_decimal", DecimalType(5, 2)),
      StructField("c_int2", IntegerType)

    ))
    val rdd = sc.parallelize(Seq(
      Row(12345, 12345L, 123.45d, BigDecimal.apply(123.45), 1)
    ))
    spark.sqlContext.createDataFrame(rdd, schema).createOrReplaceGlobalTempView("test_truncate")
    verifyResult("select truncate(c_int, c_int2) from global_temp.test_truncate", Seq("12345"))
    verifyResult("select truncate(c_long, c_int2) from global_temp.test_truncate", Seq("12345"))
    verifyResult("select truncate(c_double, c_int2) from global_temp.test_truncate", Seq("123.4"))
    verifyResult("select truncate(c_decimal, c_int2) from global_temp.test_truncate", Seq("123.40"))
  }

  def verifyResult(sql: String, expect: Seq[String]): Unit = {
    val actual = spark.sql(sql).collect().map(_.toString()).mkString(",")
    assert(actual == "[" + expect.mkString(",") + "]")
  }
}
