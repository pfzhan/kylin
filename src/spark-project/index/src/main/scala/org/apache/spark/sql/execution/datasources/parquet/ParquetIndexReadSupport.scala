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

package org.apache.spark.sql.execution.datasources.parquet

import java.nio.ByteOrder
import java.sql.{Date => SQLDate, Timestamp => SQLTimestamp}
import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration

import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.io.api._

import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext

import org.apache.spark.sql.catalyst.util.DateTimeUtils

// Parquet read support and simple record materializer for index statistics and filters, e.g. bloom
// filters. File schema is MessageType (GroupType), record materializer defines GroupConverter that
// allows to traverse fields and invoke either group converter or primitive converter.
//
// Current implementation of containers supports traversal of GroupType, but since we only support
// primitive top level fields, `ParquetIndexGroupConverter` has a check to fail if there is any
// child group found and traverse only primitive types.

abstract class RecordContainer {
  def setParquetBinary(ordinal: Int, field: PrimitiveType, value: Binary): Unit
  def setParquetInteger(ordinal: Int, field: PrimitiveType, value: Int): Unit
  def setString(ordinal: Int, value: String): Unit
  def setBoolean(ordinal: Int, value: Boolean): Unit
  def setDouble(ordinal: Int, value: Double): Unit
  def setInt(ordinal: Int, value: Int): Unit
  def setLong(ordinal: Int, value: Long): Unit
  def setDate(ordinal: Int, value: SQLDate): Unit
  def setTimestamp(ordinal: Int, value: SQLTimestamp): Unit
  def getByIndex(ordinal: Int): Any
  def init(numFields: Int): Unit
  def close(): Unit
}

private[parquet] class BufferRecordContainer extends RecordContainer {
  // buffer to store values in container, column index - value
  private var buffer: Array[Any] = null

  override def setParquetBinary(ordinal: Int, field: PrimitiveType, value: Binary): Unit = {
    field.getPrimitiveTypeName match {
      case INT96 =>
        assert(value.length() == 12,
          "Timestamps (with nanoseconds) are expected to be stored " +
          s"in 12-byte long binaries, but got a ${value.length()}-byte binary")
        val buf = value.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
        val timeOfDayNanos = buf.getLong
        val julianDay = buf.getInt
        // microseconds since epoch
        val micros = DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)
        setTimestamp(ordinal, DateTimeUtils.toJavaTimestamp(micros))
      // all other types are treated as UTF8 string
      case _ =>
        setString(ordinal, value.toStringUsingUTF8)
    }
  }

  override def setParquetInteger(ordinal: Int, field: PrimitiveType, value: Int): Unit = {
    field.getPrimitiveTypeName match {
      case INT32 =>
        field.getOriginalType match {
          case DATE =>
            setDate(ordinal, DateTimeUtils.toJavaDate(value))
          // all other values are parsed as signed int32
          case _ => setInt(ordinal, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Field $field with value $value at position $ordinal " +
          "cannot be parsed as Parquet Integer")
    }
  }

  override def setString(ordinal: Int, value: String): Unit = buffer(ordinal) = value
  override def setBoolean(ordinal: Int, value: Boolean): Unit = buffer(ordinal) = value
  override def setDouble(ordinal: Int, value: Double): Unit = buffer(ordinal) = value
  override def setInt(ordinal: Int, value: Int): Unit = buffer(ordinal) = value
  override def setLong(ordinal: Int, value: Long): Unit = buffer(ordinal) = value
  override def setDate(ordinal: Int, value: SQLDate): Unit = buffer(ordinal) = value
  override def setTimestamp(ordinal: Int, value: SQLTimestamp): Unit = buffer(ordinal) = value
  override def getByIndex(ordinal: Int): Any = buffer(ordinal)

  // Initialize map before every read, allows to have access to current record and provides cleanup
  // for every scan.
  override def init(numFields: Int): Unit = {
    if (buffer == null || numFields != buffer.length) {
      buffer = new Array[Any](numFields)
    } else {
      for (i <- 0 until buffer.length) {
        buffer(i) = null
      }
    }
  }

  override def close(): Unit = { }

  override def toString(): String = {
    val str = if (buffer == null) "null" else buffer.mkString("[", ", ", "]")
    s"${getClass.getSimpleName}(buffer=$str)"
  }
}

class ParquetIndexPrimitiveConverter(
    val ordinal: Int,
    val field: PrimitiveType,
    val updater: RecordContainer)
  extends PrimitiveConverter {

  override def addBinary(value: Binary): Unit = updater.setParquetBinary(ordinal, field, value)
  override def addInt(value: Int): Unit = updater.setParquetInteger(ordinal, field, value)
  override def addLong(value: Long): Unit = updater.setLong(ordinal, value)
  override def addDouble(value: Double): Unit = updater.setDouble(ordinal, value)
  override def addBoolean(value: Boolean): Unit = updater.setBoolean(ordinal, value)
}

class ParquetIndexGroupConverter(
    private val updater: RecordContainer,
    private val schema: GroupType)
  extends GroupConverter {

  private val converters = prepareConverters(schema)

  private def prepareConverters(schema: GroupType): Array[Converter] = {
    val arr = new Array[Converter](schema.getFieldCount)
    for (i <- 0 until arr.length) {
      val tpe = schema.getType(i)
      assert(tpe.isPrimitive, s"Only primitive types are supported, found schema $schema")
      arr(i) = new ParquetIndexPrimitiveConverter(i, tpe.asPrimitiveType, updater)
    }
    arr
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // Initialize container with known fields in advance, so we can resize it
  override def start(): Unit = updater.init(schema.getFieldCount)

  override def end(): Unit = updater.close()
}

class ParquetIndexRecordMaterializer(
    private val schema: MessageType)
  extends RecordMaterializer[RecordContainer] {

  private val updater = new BufferRecordContainer()

  override def getCurrentRecord(): RecordContainer = updater

  override def getRootConverter(): GroupConverter = {
    new ParquetIndexGroupConverter(updater, schema)
  }
}

class ParquetIndexReadSupport extends ReadSupport[RecordContainer] {
  override def init(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType): ReadContext = {
    val partialSchemaString = conf.get(ParquetMetastoreSupport.READ_SCHEMA)
    val requestedProjection = ReadSupport.getSchemaForRead(fileSchema, partialSchemaString)
    new ReadContext(requestedProjection)
  }

  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType,
      context: ReadContext): RecordMaterializer[RecordContainer] = {
    new ParquetIndexRecordMaterializer(context.getRequestedSchema)
  }
}
