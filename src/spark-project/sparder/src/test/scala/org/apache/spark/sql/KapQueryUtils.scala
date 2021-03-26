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
import org.apache.spark.sql.common.SparderQueryTest
import org.scalatest.Assertions

trait KapQueryUtils {
  this : Assertions =>

  protected def getBitmapArray(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    val count = new RoaringBitmapCounter
    values.foreach(count.add)
    count.write(buffer)
    buffer.array()
  }

  protected def getHllcArray(values: Int*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024 * 1024)
    val hllc = new HLLCounter(14)
    values.foreach(hllc.add)
    hllc.writeRegisters(buffer)
    buffer.array()
  }

  protected def checkAnswer(
      df: => DataFrame,
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
    SparderQueryTest.checkAnswerBySeq(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}
