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

import org.apache.kylin.measure.percentile.PercentileCounter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType, Decimal, DoubleType}

import java.nio.{BufferOverflowException, ByteBuffer}
import scala.annotation.tailrec

case class Percentile(aggColumn: Expression,
                      precision: Int,
                      quantile: Option[Expression] = None,
                      outputType: DataType = BinaryType,
                      mutableAggBufferOffset: Int = 0,
                      inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[PercentileCounter] with Serializable with Logging {

  override def children: Seq[Expression] = quantile match {
    case None => aggColumn :: Nil
    case Some(q) => aggColumn :: q :: Nil
  }

  override def nullable: Boolean = false

  override def createAggregationBuffer(): PercentileCounter = new PercentileCounter(precision)

  override def serialize(buffer: PercentileCounter): Array[Byte] = {
    serialize(buffer, new Array[Byte](1024 * 1024))
  }

  @tailrec
  private def serialize(buffer: PercentileCounter, bytes: Array[Byte]): Array[Byte] = {
    try {
      val output = ByteBuffer.wrap(bytes)
      buffer.writeRegisters(output)
      output.array().slice(0, output.position())
    } catch {
      case _: BufferOverflowException =>
        serialize(buffer, new Array[Byte](bytes.length * 2))
      case e =>
        throw e
    }
  }

  override def deserialize(bytes: Array[Byte]): PercentileCounter = {
    val counter = new PercentileCounter(precision)
    if (!bytes.isEmpty) {
      counter.readRegisters(ByteBuffer.wrap(bytes))
    }
    counter
  }


  override def merge(buffer: PercentileCounter, input: PercentileCounter): PercentileCounter = {
    buffer.merge(input)
    buffer
  }

  override def prettyName: String = "percentile"

  override def dataType: DataType = outputType

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def update(buffer: PercentileCounter, input: InternalRow): PercentileCounter = {
    val colValue = aggColumn.eval(input)
    colValue match {
      case d: Number =>
        buffer.add(d.doubleValue())
      case array: Array[Byte] =>
        buffer.merge(deserialize(array))
      case d: Decimal =>
        buffer.add(d.toDouble)
      case _ =>
        logDebug(s"unknown value $colValue")
    }
    buffer
  }

  override def eval(buffer: PercentileCounter): Any = {
    outputType match {
      case BinaryType =>
        serialize(buffer)
      case DoubleType =>
        val counter = quantile match {
          case Some(Literal(value, _)) =>
            val evalQuantile = value match {
              case d: Decimal => d.toDouble
              case i: Integer => i.toDouble
              case _ => -1
            }
            val counter2 = new PercentileCounter(buffer.getCompression, evalQuantile)
            counter2.merge(buffer)
            counter2
          case None => buffer
        }
        counter.getResultEstimate
    }
  }
}

