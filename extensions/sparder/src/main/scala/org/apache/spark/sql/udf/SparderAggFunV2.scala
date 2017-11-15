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

import java.math.BigDecimal
import java.nio.ByteBuffer

import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.measure.MeasureAggregator
import org.apache.kylin.measure.bitmap.BitmapCounter
import org.apache.kylin.measure.dim.DimCountDistinctCounter
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.kylin.metadata.datatype.DataTypeSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.unsafe.types.UTF8String

class SparderAggFunV2(funcName: String,
                      dataTp: org.apache.kylin.metadata.datatype.DataType)
    extends UserDefinedAggregateFunction
    with Logging {

  private val _inputDataType = StructType(
    Seq(StructField("inputBinary", BinaryType)))

  private val _bufferSchema = StructType(
    Seq(StructField("bufferBinary", BinaryType)))

  private val _returnDataType = SparderTypeUtil.kylinTypeToSparkType(dataTp)

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
      .create(funcName, dataTp)
      .asInstanceOf[MeasureAggregator[Any]]

    serializer =
      DataTypeSerializer.create(dataTp).asInstanceOf[DataTypeSerializer[Any]]
    val i = serializer.maxLength()
    byteBuffer = ByteBuffer.allocate(i)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      measureAggregator.reset()
      byteBuffer.clear()
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
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
      } catch {
        case e: Exception =>
          throw new Exception("error data is: " + input
                                .apply(0)
                                .asInstanceOf[Array[Byte]]
                                .mkString(","),
                              e)
      }
    }

  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      measureAggregator.reset()
      byteBuffer.clear()
      try {
        val byteArray = input.apply(0).asInstanceOf[Array[Byte]]
        if (byteArray.length == 0) {
          return
        }
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
      } catch {
        case e: Exception =>
          throw new Exception("error data is: " + input
                                .apply(0)
                                .asInstanceOf[Array[Byte]]
                                .mkString(","),
                              e)
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.isNullAt(0)) {
      // If the buffer value is still null, we return null.
      null
    } else {
      // Otherwise, the intermediate sum is the final result.
      //      hllBuf.clear()

      val x = serializer.deserialize(
        ByteBuffer.wrap(buffer.apply(0).asInstanceOf[Array[Byte]]))
      //scalastyle:off
      val ret = dataTp.getName match {
        case "hllc"   => x.asInstanceOf[HLLCounter].getCountEstimate
        case "bitmap" => x.asInstanceOf[BitmapCounter].getCount
        case "dim_dc" => x.asInstanceOf[DimCountDistinctCounter].result()
        case _        => null
      }

      if (ret != null)
        return ret

      val s = x.toString

      dataTp.getName match {
        case "decimal" =>
          Decimal(new BigDecimal(s), dataTp.getPrecision, dataTp.getScale)
        case "date"      => s.toLong
        case "time"      => s.toLong
        case "timestamp" => s.toLong
        case "datetime"  => s.toLong
        case "tinyint"   => s.toByte
        case "smallint"  => s.toShort
        case "integer"   => s.toInt
        case "int4"      => s.toInt
        case "bigint"    => s.toLong
        case "long8"     => s.toLong
        case "float"     => s.toFloat
        case "double"    => s.toDouble
        case "varchar"   => UTF8String.fromString(toString)
        case _           => throw new IllegalArgumentException
      }
    }
  }

  override def toString: String = {
    s"SparderAggFun@$funcName${dataType.toString}"
  }
}
