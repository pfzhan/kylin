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
package org.apache.spark.sql.execution.datasources.sparder

import java.nio.ByteBuffer
import java.util

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader
import io.kyligence.kap.storage.parquet.format.pageIndex.{
  ParquetPageIndexSpliceReader,
  ParquetPageIndexTable
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{
  InputSplit,
  RecordReader,
  TaskAttemptContext
}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.kylin.common.util.{ByteArray, JsonUtil}
import org.apache.kylin.gridtable.{GTInfo, GTUtil}
import org.apache.parquet.io.api.Binary
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.utils.{HexUtils, RowTearer}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types.StructType
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

import scala.collection.JavaConverters._

class SparderRecordReader extends RecordReader[Void, UnsafeRow] with Logging {
  val DEFAULT_MEM_MODE = MemoryMode.OFF_HEAP // We found that off heap has better performance.

  var inputStream: FSDataInputStream = _

  var conf: Configuration = _
  var columnarBatch: ColumnarBatch = _
  var reader: ParquetBundleReader = _
  var currentRow: UnsafeRow = _
  var converter: UnsafeProjection = _
  var row: GenericInternalRow = _

  var rowTearer: RowTearer = _
  var indexTable: ParquetPageIndexTable = _
  var fileSystem: FileSystem = _
  var path: Path = _

  def init(options: Map[String, String],
           requiredSchema: StructType,
           partitionSchema: StructType,
           file: PartitionedFile): Unit = {
    val gtinfo = GTInfo.serializer.deserialize(
      ByteBuffer.wrap(HexUtils.toBytes(
        options.apply(SparderConstants.KYLIN_SCAN_GTINFO_BYTES))))
    converter = UnsafeProjection.create(requiredSchema)
    val dictMap = JsonUtil
      .readValueAsMap(options.apply(SparderConstants.DICT))
      .asScala
      .toMap
    converter = UnsafeProjection.create(requiredSchema)
    rowTearer = RowTearer(requiredSchema, gtinfo, dictMap)
    row = new GenericInternalRow(requiredSchema.size)
    val cuboidId = options.apply("cuboid_id").toLong
    val indexLength = inputStream.readLong
    val pageIndexSpliceReader = new ParquetPageIndexSpliceReader(
      inputStream,
      indexLength,
      ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE)
    var pageBitmap: ImmutableRoaringBitmap = null
    val filterStr = options.get("filter_push_down")
    if (filterStr.nonEmpty) {
      val filterPushDown =
        GTUtil.deserializeGTFilter(HexUtils.toBytes(filterStr.get), gtinfo)
      var mutableRoaringBitmap: MutableRoaringBitmap = null
      for (divIndexReader <- pageIndexSpliceReader
             .getIndexReaderByCuboid(cuboidId)
             .asScala) {
        indexTable = new ParquetPageIndexTable(fileSystem, path, divIndexReader)

        if (mutableRoaringBitmap == null) {

          mutableRoaringBitmap = new MutableRoaringBitmap(
            indexTable.lookup(filterPushDown).toRoaringBitmap)
        } else {
          mutableRoaringBitmap.or(indexTable.lookup(filterPushDown))
        }
        indexTable.closeWithoutStream()
      }
      mutableRoaringBitmap.add(1)
      pageBitmap = mutableRoaringBitmap.toImmutableRoaringBitmap

    }
    if (pageBitmap == null && cuboidId >= 0) {
      pageBitmap = pageIndexSpliceReader.getFullBitmap(cuboidId)
    }

    columnarBatch = ColumnarBatch.allocate(requiredSchema, DEFAULT_MEM_MODE)
    reader = new ParquetBundleReader.Builder()
      .setConf(conf)
      .setPath(new Path(file.filePath))
      .setColumnsBitmap(rowTearer.scanColumns)
      .setPageBitset(pageBitmap)
      .setFileOffset(indexLength)
      .build
  }

  override def getCurrentKey: Void = {
    null
  }

  override def getProgress: Float = {
    0
  }

  override def nextKeyValue(): Boolean = {
    if (reader == null) {
      return false
    }
    var data: util.List[Object] = null
    while (data == null) {
      data = reader.read()
      if (data == null) {
        return false
      }
    }
    buildUnsafe(data.asScala.toList)
    true

  }

  def buildUnsafeFromByteArray(data: Array[ByteArray]): Unit = {
    var ordinal = 0
    for (col <- data) {
      if ((ordinal == 0 && rowTearer.hasPrimaryKey) || rowTearer.requireDimensions.isEmpty) {
        for (primarykey <- rowTearer.requireDimensions) {
          val range = rowTearer.primaryKeyRange.apply(primarykey)
          col.reset(col.array(), col.offset() + range._1, range._2);
          row.update(ordinal, col.toBytes)
          ordinal = ordinal + 1
        }

      } else {
        row.update(ordinal, col.toBytes)
        ordinal = ordinal + 1
      }

    }
    currentRow = converter.apply(row)
  }

  def buildUnsafe(data: List[Object]): Unit = {

    var ordinal = 0
    for (col <- data) {
      if ((ordinal == 0 && rowTearer.hasPrimaryKey) || rowTearer.requireDimensions.isEmpty) {
        val binary = col.asInstanceOf[Binary]
        for (primarykey <- rowTearer.requireDimensions) {
          val range = rowTearer.primaryKeyRange.apply(primarykey)
          row.update(ordinal, binary.slice(range._1, range._2).getBytes)
          ordinal = ordinal + 1
        }

      } else {
        row.update(ordinal, col.asInstanceOf[Binary].getBytes)
        ordinal = ordinal + 1
      }

    }
    currentRow = converter.apply(row)
  }

  override def getCurrentValue: UnsafeRow = {
    currentRow
  }

  override def initialize(split: InputSplit,
                          context: TaskAttemptContext): Unit = {
    conf = context.getConfiguration
    val fileSplit = split.asInstanceOf[FileSplit]
    path = fileSplit.getPath
    fileSystem = path.getFileSystem(conf)
    inputStream = fileSystem.open(path)
  }

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
    if (indexTable != null) {
      indexTable.close()
    }
  }

}
