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

import org.apache.spark.sql.FunctionEntity
import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.{CeilDateTime, FloorDateTime}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.scalatest.BeforeAndAfterAll

class CeilFloorTest extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()

    val ceil = FunctionEntity(expression[CeilDateTime]("ceil_datetime"))
    val floor = FunctionEntity(expression[FloorDateTime]("floor_datetime"))
    spark.sessionState.functionRegistry.registerFunction(ceil.name, ceil.info, ceil.builder)
    spark.sessionState.functionRegistry.registerFunction(floor.name, floor.info, floor.builder)
  }

  test("test ceil") {
    query("select ceil_datetime(date'2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime(date'2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select ceil_datetime('2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select ceil_datetime('2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime(timestamp'2012-02-29 23:59:59.1', 'second')").equals("[2012-03-01 00:00:00.0]")

    query("select ceil_datetime('2012-02-29 23:59:59.1', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'DAY')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'hour')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'minute')").equals("[2012-03-01 00:00:00.0]")
    query("select ceil_datetime('2012-02-29 23:59:59.1', 'second')").equals("[2012-03-01 00:00:00.0]")
  }

  test("test floor") {
    query("select floor_datetime(date'2012-02-29', 'year')").equals("[2013-01-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'quarter')").equals("[2012-04-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'month')").equals("[2012-03-01 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'week')").equals("[2012-03-05 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(date'2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select floor_datetime('2012-02-29', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'month')").equals("[2012-02-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'hour')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'minute')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29', 'second')").equals("[2012-02-29 00:00:00.0]")

    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'month')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'hour')").equals("[2012-02-29 23:00:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'minute')").equals("[2012-02-29 23:59:00.0]")
    query("select floor_datetime(timestamp'2012-02-29 23:59:59.1', 'second')").equals("[2012-02-29 23:59:59.0]")

    query("select floor_datetime('2012-02-29 23:59:59.1', 'year')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'quarter')").equals("[2012-01-01 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'month')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'week')").equals("[2012-02-27 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'DAY')").equals("[2012-02-29 00:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'hour')").equals("[2012-02-29 23:00:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'minute')").equals("[2012-02-29 23:59:00.0]")
    query("select floor_datetime('2012-02-29 23:59:59.1', 'second')").equals("[2012-02-29 23:59:59.0]")
  }

  test("test spark ceil/floor") {
    query("select floor(-3.12)").equals("[3]")
  }

  def query(sql: String): String = {
    spark.sql(sql).collect().map(row => row.toString()).mkString
  }
}
