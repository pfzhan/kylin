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

package org.apache.spark.sql.udaf

import com.esotericsoftware.kryo.io.{Input, KryoDataInput}
import org.apache.spark.sql.Column
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.{col, split, _}
import org.roaringbitmap.longlong.Roaring64NavigableMap

class OptIntersectCountSuite extends SparderBaseFunSuite with SharedSparkSession {

  import testImplicits._

  test("happy path") {
    val singersDF = Seq(
      (19, 1L, "rich|tall"),
      (19, 2L, "handsome|rich"),
      (21, 2L, "rich|tall|handsome")
    ).toDF("age", "id", "tag")

    val df = singersDF.withColumn(
      "tag",
      split(col("tag"), "\\|")
    ).repartition(1)

    val data = df.groupBy(col("age")).agg(new Column(OptIntersectCount(col("id").expr, col("tag").expr)
        .toAggregateExpression())
        .alias("intersect_count"))
    val ret = data.select(col("age"), explode('intersect_count)).collect.map(rows => {
      s"${rows.get(0).toString}, ${rows.get(1).toString}, ${dser(rows.get(2).asInstanceOf[Array[Byte]])}"
    }).mkString("|")
    assert("19, tall, {1}|19, rich, {1,2}|19, handsome, {2}|21, tall, {2}|21, rich, {2}|21, handsome, {2}".equals(ret))
  }

  def dser(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitmap = new Roaring64NavigableMap
    bitmap.deserialize(new KryoDataInput(new Input(bytes)))
    bitmap
  }
}
