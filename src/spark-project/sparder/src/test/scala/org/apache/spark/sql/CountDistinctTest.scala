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

import java.nio.ByteBuffer

import org.apache.kylin.measure.bitmap.RoaringBitmapCounter
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.common.{
  SharedSparkSession,
  SparderBaseFunSuite,
  SparderQueryTest
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.KapFunctions._

// scalastyle:off
class CountDistinctTest extends SparderBaseFunSuite with SharedSparkSession {

  import testImplicits._

  test("test precise_count_distinct without grouping keys") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 (null, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")

    val frame = df.coalesce(1).select(precise_count_distinct($"col2")).collect()
    checkAnswer(df.coalesce(1).select(precise_count_distinct($"col2")),
                Seq(Row(4)))
  }

  test("test precise_count_distinct with grouping keys") {
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")
    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_count_distinct($"col2")),
      Seq(Row("a", 4), Row("b", 3))
    )
  }

  test("test precise_count_distinct fallback to sort-based aggregation") {
    spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, 2)
    SparkSession.setActiveSession(spark)
    val array1: Array[Byte] = getBitmapArray(1, 2)
    val array2: Array[Byte] = getBitmapArray(1, 3, 5)
    val array3: Array[Byte] = getBitmapArray(1, 3, 5, 6, 7)
    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("c": String, array3),
                 ("a": String, array2),
                 ("a": String, array3))
      .toDF("col1", "col2")

    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(precise_count_distinct($"col2")),
      Seq(Row("a", 6), Row("b", 3), Row("c", 5))
    )
  }

  private def getBitmapArray(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    val count = new RoaringBitmapCounter
    values.foreach(count.add)
    count.write(buffer)
    buffer.array()
  }

  private def getHllcArray(values: Int*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024 * 1024)
    val hllc = new HLLCounter(14)
    values.foreach(hllc.add)
    hllc.writeRegisters(buffer)
    buffer.array()
  }

  protected def checkAnswer(df: => DataFrame,
                            expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df
    catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    SparderQueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None               =>
    }
  }

  test("test approx_count_distinct without grouping keys") {
    val array1: Array[Byte] = getHllcArray(1, 2)
    val array2: Array[Byte] = getHllcArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 (null, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")

    checkAnswer(df.coalesce(1).select(approx_count_distinct($"col2", 14)),
                Seq(Row(4)))
  }

  test("test approx_count_distinct with grouping keys") {
    val array1: Array[Byte] = getHllcArray(1, 2)
    val array2: Array[Byte] = getHllcArray(1, 3, 5)

    val df = Seq(("a": String, array1),
                 ("b": String, array2),
                 ("a": String, array2)).toDF("col1", "col2")
    checkAnswer(
      df.coalesce(1).groupBy($"col1").agg(approx_count_distinct($"col2", 14)),
      Seq(Row("a", 4), Row("b", 3))
    )
  }

}
