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

import java.nio.ByteBuffer

import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.measure.{BufferedMeasureCodec, MeasureAggregator}
import org.apache.kylin.metadata.datatype.DataTypeSerializer

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types._

class SparderAggFun(args: AggraFuncArgs2) extends UserDefinedAggregateFunction {

  private var _inputDataType = StructType(
    Seq(StructField("inputBinary", BinaryType)))

  private var _bufferSchema = StructType(
    Seq(StructField("bufferBinary", BinaryType)))

  private var _returnDataType = BinaryType

  private var byteBuffer: ByteBuffer = _
  private var init = false
  private var gtInfo: GTInfo = _
  private var measureAggregator: MeasureAggregator[Any] = _
  private var colId: Int = _
  private var serializer: DataTypeSerializer[Any] = _

  override def inputSchema: StructType = _inputDataType

  override def bufferSchema: StructType = _bufferSchema

  override def dataType: DataType = _returnDataType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, null)
    measureAggregator = MeasureAggregator
      .create(args.funcName, args.dataTp)
      .asInstanceOf[MeasureAggregator[Any]]

    byteBuffer = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE)
    serializer = DataTypeSerializer
      .create(args.dataTp)
      .asInstanceOf[DataTypeSerializer[Any]]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      measureAggregator.reset()
      byteBuffer.clear()
      val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
      val newValue = serializer.deserialize(ByteBuffer.wrap(byteArray))
      measureAggregator.aggregate(newValue)
      if (buffer.isNullAt(0)) {
        buffer.update(0, byteArray)
      } else {

        val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
        val oldValue = serializer.deserialize(ByteBuffer.wrap(bytes))
        measureAggregator.aggregate(oldValue)
        val aggregatored = measureAggregator.getState
        serializer.serialize(aggregatored, byteBuffer)
        buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
      }
    }
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      measureAggregator.reset()
      byteBuffer.clear()
      val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
      val newValue = serializer.deserialize(ByteBuffer.wrap(byteArray))
      measureAggregator.aggregate(newValue)
      if (buffer.isNullAt(0)) {
        buffer.update(0, byteArray)
      } else {

        val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
        val oldValue = serializer.deserialize(ByteBuffer.wrap(bytes))
        measureAggregator.aggregate(oldValue)
        val aggregatored = measureAggregator.getState
        serializer.serialize(aggregatored, byteBuffer)
        buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
      }
    }
  }

  override def evaluate(buffer: Row): Array[Byte] = {
    if (buffer.isNullAt(0)) {
      // If the buffer value is still null, we return null.
      Array.emptyByteArray
    } else {
      // Otherwise, the intermediate sum is the final result.
      //      hllBuf.clear()
      buffer.apply(0).asInstanceOf[Array[Byte]]
      //      val value = serializer.deserialize(ByteBuffer.wrap(bytes))
      //      if(value.isInstanceOf[java.math.BigDecimal])
      //      println("udf"+value)
      //      println("udf"+bytes.length)
      //      newHLLCounter.clear()
      //      newHLLCounter.readRegisters(ByteBuffer.wrap(bytes))
      //      newHLLCounter.getCountEstimate
    }
  }
}
