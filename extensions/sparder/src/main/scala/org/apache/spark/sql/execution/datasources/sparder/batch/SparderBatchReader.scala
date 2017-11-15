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
package org.apache.spark.sql.execution.datasources.sparder.batch

import java.nio.ByteBuffer

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader
import io.kyligence.kap.storage.parquet.format.filter.{
  BinaryFilter,
  BinaryFilterSerializer
}
import io.kyligence.kap.storage.parquet.format.pageIndex.{
  ParquetPageIndexSpliceReader,
  ParquetPageIndexTable
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{
  InputSplit,
  RecordReader,
  TaskAttemptContext
}
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.gridtable.{GTInfo, GTUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.sparder.SparderConstants
import org.apache.spark.sql.execution.datasources.sparder.batch.reader.{
  SparderFileReader,
  SparderFillReader,
  VectorizedSparderRecordReader
}
import org.apache.spark.sql.execution.utils.{HexUtils, RowTearer}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types.StructType
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

import scala.collection.JavaConverters._

class SparderBatchReader extends RecordReader[Void, Object] with Logging {
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
  var rowTearerV2: RowTearer = _
  var binaryFilter: BinaryFilter = _
  var readTime: Long = _
  var initTime: Long = _
  var decodeTime: Long = 0L
  var binaryFilterTime: Long = 0L
  var batchReader: SparderFillReader = _

  def init(options: Map[String, String],
           requiredSchema: StructType,
           partitionSchema: StructType,
           file: PartitionedFile): Unit = {

    initTime = System.currentTimeMillis()
    val gtinfo = GTInfo.serializer.deserialize(
      ByteBuffer.wrap(HexUtils.toBytes(
        options.apply(SparderConstants.KYLIN_SCAN_GTINFO_BYTES))))
    val dictMap = JsonUtil
      .readValueAsMap(options.apply(SparderConstants.DICT))
      .asScala
      .toMap
    converter = UnsafeProjection.create(requiredSchema)
    rowTearerV2 = RowTearer(requiredSchema, gtinfo, dictMap)
    row = new GenericInternalRow(requiredSchema.size)
    val indexLength = inputStream.readLong
    val pageIndexSpliceReader = new ParquetPageIndexSpliceReader(
      inputStream,
      indexLength,
      ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE)
    val binaryFilterHex = options(SparderConstants.BINARY_FILTER_PUSH_DOWN)
    if (binaryFilterHex.nonEmpty) {
      binaryFilter = BinaryFilterSerializer
        .deserialize(ByteBuffer.wrap(HexUtils.toBytes(binaryFilterHex)))
    }
    val cuboidId = options.apply(SparderConstants.CUBOID_ID).toLong

    val filteredBitmap = pageFilter(options, gtinfo, pageIndexSpliceReader)
    columnarBatch =
      ColumnarBatch.allocate(requiredSchema, MemoryMode.ON_HEAP, 10240)
    reader = new ParquetBundleReader.Builder()
      .setConf(conf)
      .setPath(new Path(file.filePath))
      .setColumnsBitmap(rowTearerV2.scanColumns)
      .setPageBitset(filteredBitmap)
      .setFileOffset(indexLength)
      .build
    log.info(s"initTime :${System.currentTimeMillis() - initTime}")
    readTime = System.currentTimeMillis()

    val sparderBatchReader = new SparderFileReader(conf,
                                                   new Path(file.filePath),
                                                   null,
                                                   null,
                                                   indexLength,
                                                   filteredBitmap,
                                                   rowTearerV2.scanColumns)
    val vtReader = new VectorizedSparderRecordReader(sparderBatchReader,
                                                     rowTearerV2.scanColumns,
                                                     columnarBatch)
    batchReader = new SparderFillReader(vtReader, rowTearerV2, columnarBatch)

  }

  def pageFilter(options: Map[String, String],
                 gTInfo: GTInfo,
                 pageIndexSpliceReader: ParquetPageIndexSpliceReader)
    : ImmutableRoaringBitmap = {
    var pageBitmap: ImmutableRoaringBitmap = null
    val cuboidId = options.apply(SparderConstants.CUBOID_ID).toLong
    val filterStr = options(SparderConstants.FILTER_PUSH_DOWN)
    if (filterStr.nonEmpty) {
      val filterPushDown =
        GTUtil.deserializeGTFilter(HexUtils.toBytes(filterStr), gTInfo)
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
      pageBitmap = mutableRoaringBitmap.toImmutableRoaringBitmap
    }
    if (pageBitmap == null && cuboidId >= 0) {
      pageBitmap = pageIndexSpliceReader.getFullBitmap(cuboidId)
    }
    pageBitmap
  }

  override def getCurrentKey: Void = {
    null
  }

  override def getProgress: Float = {
    0
  }

  var rowId = 0

  override def nextKeyValue(): Boolean = {
    if (batchReader == null) {
      return false
    }
    if (rowId >= (columnarBatch.numRows() - 1)) {
      if (batchReader.hasNext) {
        val start = System.nanoTime()
        batchReader.nextKeyValue()
        decodeTime += System.nanoTime() - start
        rowId = 0
        true
      } else {
        false
      }
    } else {
      rowId += 1
      true
    }

  }

  override def getCurrentValue: Object = {
    columnarBatch.getRow(rowId)
  }

  // for batch read
  //  override def nextKeyValue(): Boolean = {
  //    batchReader.hasNext
  //  }
  //
  //  override def getCurrentValue: Object = {
  //    batchReader.nextKeyValue()
  //  }

  override def close(): Unit = {
    logInfo(s"read time : ${System.currentTimeMillis() - readTime}")
    logInfo(s"decode time : ${decodeTime / 1000000}")
    logInfo(s"binary filter time : ${binaryFilterTime / 1000000}")
    if (inputStream != null) {
      inputStream.close()
    }
    if (indexTable != null) {
      indexTable.close()
    }
    if (batchReader != null) {
      batchReader.close()
    }
    if (columnarBatch != null) {
      columnarBatch.close()
      columnarBatch = null
    }
  }

  override def initialize(split: InputSplit,
                          context: TaskAttemptContext): Unit = {
    conf = context.getConfiguration
    val fileSplit = split.asInstanceOf[FileSplit]
    path = fileSplit.getPath
    fileSystem = path.getFileSystem(conf)
    inputStream = fileSystem.open(path)
  }
}
