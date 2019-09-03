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

import org.apache.kylin.measure.bitmap.intersect.{IntersectBitmapCounter, IntersectSerializer}
import org.apache.kylin.measure.bitmap.{BitmapCounter, BitmapSerializer}
import org.apache.kylin.metadata.datatype
import org.apache.kylin.metadata.datatype.DataTypeSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, _}

@java.lang.Deprecated
class SparderExternalAggFunc(dateType: String) extends UserDefinedAggregateFunction with Logging {
  protected val dataTp: datatype.DataType =
    org.apache.kylin.metadata.datatype.DataType.getType("bitmap")

  protected val _inputDataType = StructType(
    Seq(
      StructField("inputBinary", BinaryType),
      StructField("key", StringType),
      StructField("filterColumnkey", ArrayType(StringType))))

  protected val _bufferSchema: StructType = StructType(
    Seq(
      StructField("inputBinary", BinaryType),
      StructField("key", StringType),
      StructField("filterColumn", ArrayType(StringType))))

  protected val _returnDataType: DataType = LongType

  protected var byteBuffer: ByteBuffer = null
  protected var init = false
  protected var bitmmapCounterSerializer: DataTypeSerializer[BitmapCounter] = _
  protected var intersectBitmapSerializer: DataTypeSerializer[IntersectBitmapCounter] = _

  protected var filterList: java.util.List[String] = _

  override def inputSchema: StructType = _inputDataType

  override def bufferSchema: StructType = _bufferSchema

  override def dataType: DataType = _returnDataType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, null)
    buffer.update(1, null)
    buffer.update(2, null)
    bitmmapCounterSerializer = new BitmapSerializer(dataTp)
    intersectBitmapSerializer = new IntersectSerializer(dataTp)
    val i = intersectBitmapSerializer.maxLength()
    if (byteBuffer == null) {
      byteBuffer = ByteBuffer.allocate(1024 * 1024 * 10)
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!input.isNullAt(0)) {
      byteBuffer.clear()
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
        val value =
          bitmmapCounterSerializer.deserialize(ByteBuffer.wrap(byteArray))
        val key = input.getString(1)
        val filters = input.getList(2).asInstanceOf[java.util.List[String]]
        if (buffer.isNullAt(0)) {
          filterList = filters
          val counter =
            IntersectBitmapCounter.wrap(key, filters, value)
          intersectBitmapSerializer.serialize(counter, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
          buffer.update(1, key)
          buffer.update(2, filters)
        } else {
          val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
          val oldValue =
            intersectBitmapSerializer.deserialize(ByteBuffer.wrap(bytes))
          oldValue.add(key, filters, value)
          intersectBitmapSerializer.serialize(oldValue, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
        }
      } catch {
        case e: Exception =>
          throw new Exception(
            "error data is: " + input
              .apply(0)
              .asInstanceOf[Array[Byte]]
              .mkString(","),
            e)
      }
    }

  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      byteBuffer.clear()
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
        val newValue =
          intersectBitmapSerializer.deserialize(ByteBuffer.wrap(byteArray))
        val key = input.getString(1)
        val filterlister = input.getList(2).asInstanceOf[java.util.List[String]]
        if (buffer.isNullAt(0)) {
          buffer.update(0, byteArray)
          buffer.update(1, key)
          buffer.update(2, filterlister)
          filterList = filterlister
        } else {
          val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
          val oldValue =
            intersectBitmapSerializer.deserialize(ByteBuffer.wrap(bytes))
          oldValue.setKeyList(filterlister)
          oldValue.merge(newValue)
          intersectBitmapSerializer.serialize(oldValue, byteBuffer)
          buffer.update(0, byteBuffer.array().slice(0, byteBuffer.position()))
        }
      } catch {
        case e: Exception =>
          throw new Exception(
            "error data is: " + input
              .apply(0)
              .asInstanceOf[Array[Byte]]
              .mkString(","),
            e)
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.isNullAt(0)) {
      // If the buffer value is still null, we return 0.
      0
    } else {
      // Otherwise, the intermediate sum is the final result.
      val bytes = buffer.apply(0).asInstanceOf[Array[Byte]]
      val oldValue = intersectBitmapSerializer.deserialize(ByteBuffer.wrap(bytes))
      oldValue.setKeyList(buffer.getList(2))
      oldValue.result()

    }
  }

  override def toString: String = {
    s"SparderAggFun@bitmap${dataType.toString}"
  }
}
