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

import io.kyligence.kap.storage.parquet.format.file.ParquetMetrics
import org.apache.kylin.metadata.datatype.DataTypeSerializer
import org.apache.spark.sql.execution.utils.RowTearer
import org.apache.spark.sql.execution.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SparderFillReader(
                         vectorizedSparderRecordReader: VectorizedSparderRecordReader,
                         rowTearerV2: RowTearer,
                         columnarBatch: ColumnarBatch) {
  val columnReaders: ListBuffer[ColumnDecodeReader] = {
    val sparderColumnReaders = ListBuffer.empty[ColumnDecodeReader]
    var ordinal = 0
    for (col <- rowTearerV2.scanColumns.asScala) {
      if (ordinal == 0 && rowTearerV2.hasPrimaryKey) {
        val primaryColumnReaders = ListBuffer.empty[ColumnDecodeReader]
        for (primarykey <- rowTearerV2.requireDimensions) {
          val columnId = rowTearerV2.colToRowindex.apply(ordinal)
          val vector = columnarBatch.column(columnId)
          val range = rowTearerV2.primaryKeyRange.apply(ordinal)
          val isDateTime = rowTearerV2.datetimeFamilyColsInRow.contains(ordinal)
          if (rowTearerV2.dictionaryMapping.contains(ordinal)) {
            val dictionary = rowTearerV2.dictionaryMapping.apply(ordinal)
            primaryColumnReaders append new DictionaryColumnDecodeReader(
              isDateTime,
              dictionary,
              range,
              vectorizedSparderRecordReader,
              vector,
              columnarBatch,
              col)
          } else {
            val gtInfoIndex = rowTearerV2.rowToGTInfo.apply(ordinal)
            val dataType = rowTearerV2.info.getColumnType(gtInfoIndex)
            val serializer = rowTearerV2.codeSystem
              .getSerializer(gtInfoIndex)
              .asInstanceOf[DataTypeSerializer[Any]]
            primaryColumnReaders append new DataTypeColumnDecodeReader(
              true,
              isDateTime,
              serializer,
              range,
              vectorizedSparderRecordReader,
              vector,
              columnarBatch,
              col)
          }
          ordinal = ordinal + 1
        }
        sparderColumnReaders append new PrimaryColumnDecodeReader(
          vectorizedSparderRecordReader,
          columnarBatch,
          primaryColumnReaders.toList,
          col)
      } else {
        val columnId = rowTearerV2.colToRowindex.apply(ordinal)
        val vector = columnarBatch.column(columnId)
        sparderColumnReaders append new MeasureColumnDecodeReader(
          vectorizedSparderRecordReader,
          vector,
          columnarBatch,
          col)
        ordinal = ordinal + 1
      }
    }
    sparderColumnReaders
  }

  def hasNext: Boolean = {
    val booleans = columnReaders.map(_.hasNext)
    if (!booleans.head) {
      vectorizedSparderRecordReader.nextBatch()
      val head = columnReaders.map(_.hasNext)
      return head.head
    }
    booleans.head
  }

  def nextKeyValue(): ColumnarBatch = {
    columnarBatch.reset()
    columnReaders.foreach(_.fill())
    columnarBatch
  }

  def close(): Unit = {
    if (vectorizedSparderRecordReader != null) {
      vectorizedSparderRecordReader.close()
    }
    columnReaders.foreach(_.printStatistics())
    ParquetMetrics.get.print(System.err)

  }
}
