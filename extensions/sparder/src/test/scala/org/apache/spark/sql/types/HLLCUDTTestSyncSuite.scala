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

package org.apache.spark.sql.types

import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.spark.sql.{Row, SparkSqlFunSuite}

class HLLCUDTTestSyncSuite extends SparkSqlFunSuite {


  test("HllcType") {
    val sparkContext = spark.sparkContext
    UDTRegistration.register(classOf[HLLCounter].getName, classOf[HLLCUDT].getName)
    spark.conf.set("spark.sql.codegen.wholeStage", value = false)
    val rowRDD1 = sparkContext.parallelize(Seq(Row(1, new HLLCounter(9))))
    rowRDD1.take(1).foreach(println)
    val schema1 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", HLLCUDT(9))))
    val rowRDD2 = sparkContext.parallelize(Seq(Row(2, new HLLCounter(9))))
    val schema2 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", HLLCUDT(11))))
    val df1 = spark.createDataFrame(rowRDD1, schema1)
    df1.show(10)
    val df2 = spark.createDataFrame(rowRDD2, schema2)
    val rows = df1.union(df2).orderBy("label").take(10)
    rows.foreach(println)
    checkAnswer(
      df1.union(df2).orderBy("label"),
      Seq(Row(1, new HLLCounter(9)), Row(2, new HLLCounter(11)))
    )
  }

  case class TestSer(t1: String, t2: String)

  test("test case class ") {
    val ser = TestSer("1", "2")
    val str = JsonUtil.writeValueAsString(ser)
    val ser1 = JsonUtil.readValue(str, classOf[TestSer])
    TestSer("1", "2")
    TestSer("1", "2")
  }

}
