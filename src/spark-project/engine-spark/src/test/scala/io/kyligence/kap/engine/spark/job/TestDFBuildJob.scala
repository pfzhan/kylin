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

package io.kyligence.kap.engine.spark.job

import com.google.common.collect.Lists
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.unsafe.types.UTF8String

class TestDFBuildJob extends SparderBaseFunSuite with SharedSparkSession {

  def generateOriginData(): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("1", StringType)
    schema = schema.add("2", StringType)
    val data = Seq(
      Row("0", "a"),
      Row("1", "b"),
      Row("2", "a"),
      Row("3", "b"),
      Row("4", "a"),
      Row("5", "b"),
      Row("6", "a"),
      Row("7", "b"),
      Row("8", "a"),
      Row("9", "b"))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  test("repartitionDataSet - repartition by partitionNum and ShardByColumns when both are effective") {
    val partitionNum = 2
    val origin = generateOriginData()
    val actualDS = DFBuildJob.repartitionDataSet(origin, partitionNum, Lists.newArrayList(Integer.valueOf(2)))
    val actual = actualDS.rdd.mapPartitionsWithIndex {
      case (id, iterator) =>
        iterator.map(row => Seq(id, row.getString(0), row.getString(1)).mkString(","))
    }.collect()

    val expect = origin.collect().map {
      row => Seq(mockSparkHashPartitionMethod(row.getString(1), partitionNum), row.getString(0), row.getString(1)).mkString(",")
    }

    assert(actualDS.rdd.partitions.length == partitionNum)
    assert(actual.sorted sameElements expect.sorted)
  }

  test("repartitionDataSet - repartition by partitionNum when ShardByColumns is null or empty") {
    val partitionNum = 2
    val origin = generateOriginData()
    var actualDS = DFBuildJob.repartitionDataSet(origin, partitionNum, null)
    assert(actualDS.rdd.partitions.length == partitionNum)

    actualDS = DFBuildJob.repartitionDataSet(origin, partitionNum, Lists.newArrayList())
    assert(actualDS.rdd.partitions.length == partitionNum)
  }

  def mockSparkHashPartitionMethod(key: String, partitionNum: Int): Int = {
    UTF8String.fromString(key).hashCode() % partitionNum
  }

}
