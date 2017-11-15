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
package org.apache.spark.sql.execution.datasources.sparder.batch.reader

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import io.kyligence.kap.common.util.ExpandableBytesVector
import org.apache.kylin.common.util.{
  ByteArray,
  BytesUtil,
  DateFormat,
  Dictionary
}
import org.apache.kylin.metadata.datatype.DataTypeSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.vectorized.{ColumnVector, ColumnarBatch}

abstract class ColumnDecodeReader(
    vectorizedSparderRecordReader: VectorizedSparderRecordReader,
    columnarBatch: ColumnarBatch,
    columnId: Int)
    extends Logging {
  var vector: ExpandableBytesVector = _
  val array = new ByteArray()

  var totalTime = 0L
  var totalCount = 1L //  ensure  no NAN
  def hasNext: Boolean = {
    vector = vectorizedSparderRecordReader.nextPage(columnId)
    if (vector == null) {
      return false
    }
    true
  }

  def printStatistics(): Unit

  def fill(): Unit
}

class MeasureColumnDecodeReader(
    vectorizedSparderRecordReader: VectorizedSparderRecordReader,
    columnVector: ColumnVector,
    columnarBatch: ColumnarBatch,
    columnId: Int)
    extends ColumnDecodeReader(
      vectorizedSparderRecordReader: VectorizedSparderRecordReader,
      columnarBatch: ColumnarBatch,
      columnId: Int) {

  override def fill(): Unit = {
    val start = System.nanoTime()
    columnarBatch.setNumRows(vector.getRowCount)
    for (rowId <- Range(0, vector.getRowCount)) {
      val i = vector.getLength(rowId)
      if (i == 0) {
        columnVector.putNull(rowId)
      } else {
        columnVector.putByteArray(rowId,
                                  vector.getData,
                                  vector.getOffset(rowId),
                                  vector.getLength(rowId))
      }
    }
    val l = System.nanoTime() - start
    totalTime += l
    totalCount += vector.getRowCount
  }

  override def printStatistics(): Unit = {
    logInfo(s" measure total time : ${totalTime / 1000000}")
    logInfo(s" measure avg time : ${totalTime / totalCount}")

  }
}

class DictionaryColumnDecodeReader(
    isDateType: Boolean,
    dictionary: Dictionary[String],
    range: (Int, Int),
    vectorizedSparderRecordReader: VectorizedSparderRecordReader,
    columnVector: ColumnVector,
    columnarBatch: ColumnarBatch,
    columnId: Int)
    extends ColumnDecodeReader(
      vectorizedSparderRecordReader: VectorizedSparderRecordReader,
      columnarBatch: ColumnarBatch,
      columnId: Int) {

  override def fill(): Unit = {
    columnarBatch.setNumRows(vector.getRowCount)
    val length = range._2 - range._1
    val start = System.nanoTime()
    if (isDateType) {
      for (rowId <- Range(0, vector.getRowCount)) {
        val dictionaryId = BytesUtil.readUnsigned(
          vector.getData,
          vector.getOffset(rowId) + range._1,
          length)
        val value = dictionary.getValueFromId(dictionaryId)
        if (value == null) {
          columnVector.putNull(rowId)
        } else {
          val stringValue = String.valueOf(DateFormat.stringToMillis(value))
          columnVector.putByteArray(
            rowId,
            stringValue.getBytes(StandardCharsets.UTF_8))
        }
      }
    } else {
      for (rowId <- Range(0, vector.getRowCount)) {
        val dictionaryId = BytesUtil.readUnsigned(
          vector.getData,
          vector.getOffset(rowId) + range._1,
          length)
//                        val bytes = dictionary.getValueFromId(dictionaryId)
        val bytes = dictionary.getValueByteFromId(dictionaryId)
        if (bytes == null) {
          columnVector.putNull(rowId)
        } else {
          columnVector.putByteArray(rowId, bytes)
//  columnVector.putByteArray(rowId, bytes.getBytes(StandardCharsets.UTF_8))
        }
      }
    }
    val l = System.nanoTime() - start
    totalTime += l
    totalCount += vector.getRowCount

  }

  override def printStatistics(): Unit = {
    logInfo(s" Dictionary total time : ${totalTime / 1000000}")
    logInfo(s" Dictionary avg time : ${totalTime / totalCount}")
    dictionary.printlnStatistics()
  }

}

class DataTypeColumnDecodeReader(
    needDecode: Boolean,
    isDateType: Boolean,
    serializer: DataTypeSerializer[Any],
    range: (Int, Int),
    vectorizedSparderRecordReader: VectorizedSparderRecordReader,
    columnVector: ColumnVector,
    columnarBatch: ColumnarBatch,
    columnId: Int)
    extends ColumnDecodeReader(
      vectorizedSparderRecordReader: VectorizedSparderRecordReader,
      columnarBatch: ColumnarBatch,
      columnId: Int) {

  override def fill(): Unit = {
    columnarBatch.setNumRows(vector.getRowCount)
    if (needDecode) {
      decodeFill()
    } else {
      normalFill()
    }
  }

  def normalFill(): Unit = {
    val start = System.nanoTime()
    val i = range._2 - range._1
    for (rowId <- Range(0, vector.getRowCount)) {
      val l = ByteBuffer
        .wrap(vector.getData, range._1 + vector.getOffset(rowId), i)
        .getLong()
      columnVector.putLong(rowId, l)
    }
    val l = System.nanoTime() - start
    totalTime += l
    totalCount += vector.getRowCount
  }

  def decodeFill(): Unit = {
    val start = System.nanoTime()
    val i = range._2 - range._1
    if (isDateType) {
      for (rowId <- Range(0, vector.getRowCount)) {
        val value = serializer.deserialize(
          ByteBuffer
            .wrap(vector.getData, range._1 + vector.getOffset(rowId), i))
        if (value == null) {
          columnVector.putNull(rowId)
        } else {
          val stringValue =
            String.valueOf(DateFormat.stringToMillis(value.toString))
          columnVector.putByteArray(
            rowId,
            stringValue.getBytes(StandardCharsets.UTF_8))
        }
      }
    } else {
      for (rowId <- Range(0, vector.getRowCount)) {
        val value = serializer.deserialize(
          ByteBuffer
            .wrap(vector.getData, range._1 + vector.getOffset(rowId), i))
        if (value == null) {
          columnVector.putNull(rowId)
        } else {
          columnVector.putByteArray(
            rowId,
            value.toString.getBytes(StandardCharsets.UTF_8))
        }
      }
    }
    val l = System.nanoTime() - start
    totalTime += l
    totalCount += vector.getRowCount
  }

  override def printStatistics(): Unit = {
    logInfo(s" datatType total time : ${totalTime / 1000000}")
    logInfo(s" datatType avg time : ${totalTime / totalCount}")
  }
}

class PrimaryColumnDecodeReader(
    vectorizedSparderRecordReader: VectorizedSparderRecordReader,
    columnarBatch: ColumnarBatch,
    columnDecodeReaders: List[ColumnDecodeReader],
    columnId: Int)
    extends ColumnDecodeReader(
      vectorizedSparderRecordReader: VectorizedSparderRecordReader,
      columnarBatch: ColumnarBatch,
      columnId: Int) {

  override def fill(): Unit = {
    columnDecodeReaders.foreach(_.vector = vector)
    columnDecodeReaders.foreach(_.fill())
  }

  override def printStatistics(): Unit = {
    columnDecodeReaders.foreach(_.printStatistics())
  }
}
