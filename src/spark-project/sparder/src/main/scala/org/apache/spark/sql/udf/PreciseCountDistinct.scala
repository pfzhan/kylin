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

package org.apache.spark.sql.udf

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.kylin.measure.bitmap.{BitmapAggregator, BitmapSerializer, RoaringBitmapCounter}
import org.apache.kylin.measure.hllc.{HLLCAggregator, HLLCSerializer, HLLCounter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

// scalastyle:off
@ExpressionDescription(usage = "PreciseCountDistinct(expr)")
case class PreciseCountDistinct(child: Expression,
                                mutableAggBufferOffset: Int = 0,
                                inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[BitMapCounter.State] with Logging {

  private val serializer: BitmapSerializer = new BitmapSerializer(null)

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): BitMapCounter.State = BitMapCounter.State(new BitmapAggregator)

  override def update(buffer: BitMapCounter.State, input: InternalRow): BitMapCounter.State = {
    val colValue = child.eval(input)
    if (colValue != null) {
      val value = serializer.deserialize(ByteBuffer.wrap(colValue.asInstanceOf[Array[Byte]]))
      buffer.bitmap.aggregate(value)
    }
    buffer
  }

  override def merge(buffer: BitMapCounter.State, input: BitMapCounter.State): BitMapCounter.State = {
    if (input.bitmap.getState != null) {
      buffer.bitmap.aggregate(input.bitmap.getState)
      buffer
    } else {
      buffer
    }
  }

  override def eval(buffer: BitMapCounter.State): Any = {
    if (buffer.bitmap.getState == null) {
      0L
    } else {
      buffer.bitmap.getState.getCount
    }
  }

  override def serialize(buffer: BitMapCounter.State): Array[Byte] = {
    if (buffer.bitmap.getState != null) {
      val bos = new ByteArrayOutputStream(1024)
      buffer.bitmap.getState.write(bos)
      bos.toByteArray
    } else {
      Array.empty[Byte]
    }
  }

  override def deserialize(storageFormat: Array[Byte]): BitMapCounter.State = {
    val aggregator = new BitmapAggregator
    if (storageFormat.nonEmpty) {
      val counter = new RoaringBitmapCounter
      counter.readFields(ByteBuffer.wrap(storageFormat))
      aggregator.aggregate(counter)
    }
    BitMapCounter.State(aggregator)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "precise_count_distinct"
}

object BitMapCounter {

  case class State(var bitmap: BitmapAggregator)

}

@ExpressionDescription(usage = "PreciseCountDistinct(expr)")
case class ApproxCountDistinct(child: Expression,
                               precision: Int,
                               mutableAggBufferOffset: Int = 0,
                               inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HLLCCounter.State] with Logging {

  private val serializer: HLLCSerializer = new HLLCSerializer(precision)

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def dataType: DataType = LongType

  override def nullable: Boolean = true

  lazy val buf: ByteBuffer = ByteBuffer.allocate(1024 * 1024)

  override def createAggregationBuffer(): HLLCCounter.State = HLLCCounter.State(new HLLCAggregator(precision))

  override def update(buffer: HLLCCounter.State, input: InternalRow): HLLCCounter.State = {
    val colValue = child.eval(input)
    if (colValue != null) {
      val value = serializer.deserialize(ByteBuffer.wrap(colValue.asInstanceOf[Array[Byte]]))
      buffer.hllc.aggregate(value)
    }
    buffer
  }

  override def merge(buffer: HLLCCounter.State, input: HLLCCounter.State): HLLCCounter.State = {
    if (input.hllc.getState != null) {
      buffer.hllc.aggregate(input.hllc.getState)
      buffer
    } else {
      buffer
    }
  }

  override def eval(buffer: HLLCCounter.State): Any = {
    if (buffer.hllc.getState == null) {
      0L
    } else {
      buffer.hllc.getState.getCountEstimate
    }
  }

  override def serialize(buffer: HLLCCounter.State): Array[Byte] = {
    if (buffer.hllc.getState != null) {
      buf.clear()
      buffer.hllc.getState.writeRegisters(buf)
      buf.array()
    } else {
      Array.empty[Byte]
    }
  }

  override def deserialize(storageFormat: Array[Byte]): HLLCCounter.State = {
    val aggregator = new HLLCAggregator(precision)
    if (storageFormat.nonEmpty) {
      val counter = new HLLCounter(precision)
      val buffer = ByteBuffer.wrap(storageFormat)
      counter.readRegisters(buffer)
      aggregator.aggregate(counter)
    }
    HLLCCounter.State(aggregator)

  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "approx_count_distinct"
}

object HLLCCounter {

  case class State(var hllc: HLLCAggregator)

}