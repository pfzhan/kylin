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

package org.apache.spark.sql.execution.datasources.sparder.v2

import java.nio.ByteBuffer
import java.util

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader
import io.kyligence.kap.storage.parquet.format.filter.{BinaryFilter, BinaryFilterSerializer}
import io.kyligence.kap.storage.parquet.format.pageIndex.{ParquetPageIndexSpliceReader, ParquetPageIndexTable}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.gridtable.{GTInfo, GTUtil}
import org.apache.parquet.io.api.Binary
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.sparder.{SparderConstants, SparderRecordReader}
import org.apache.spark.sql.execution.utils.{HexUtils, RowTearer}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

import scala.collection.JavaConverters._


class SparderRecordReaderV2 extends SparderRecordReader with Logging {
  var rowTearerV2: RowTearer = _
  var binaryFilter: BinaryFilter = _

  override def init(options: Map[String, String], requiredSchema: StructType, partitionSchema: StructType, file: PartitionedFile): Unit = {
    val gtinfo = GTInfo.serializer.deserialize(ByteBuffer.wrap(HexUtils.toBytes(options.apply(SparderConstants.KYLIN_SCAN_GTINFO_BYTES))))
    val dictMap = JsonUtil.readValueAsMap(options.apply(SparderConstants.DICT)).asScala.toMap
    converter = UnsafeProjection.create(requiredSchema)
    rowTearerV2 = RowTearer(requiredSchema, gtinfo, dictMap)
    row = new GenericInternalRow(requiredSchema.size)
    val indexLength = inputStream.readLong
    val pageIndexSpliceReader = new ParquetPageIndexSpliceReader(inputStream, indexLength, ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE)
    //todo binary filter
    val binaryFilterHex = options(SparderConstants.BINARY_FILTER_PUSH_DOWN)
    if (binaryFilterHex.nonEmpty) {
      binaryFilter = BinaryFilterSerializer
        .deserialize(ByteBuffer.wrap(HexUtils.toBytes(binaryFilterHex)))
    }
    val filteredBitmap = pageFilter(options, gtinfo, pageIndexSpliceReader)
    //todo batch
    columnarBatch = ColumnarBatch.allocate(requiredSchema, DEFAULT_MEM_MODE)
    reader = new ParquetBundleReader.Builder().setConf(conf).setPath(new Path(file.filePath)).setColumnsBitmap(rowTearerV2.scanColumns).setPageBitset(filteredBitmap).setFileOffset(indexLength).build
    //todo miss columar

    //todo check file schema
    //todo count *
  }

  def pageFilter(options: Map[String, String], gTInfo: GTInfo, pageIndexSpliceReader: ParquetPageIndexSpliceReader): ImmutableRoaringBitmap = {
    var pageBitmap: ImmutableRoaringBitmap = null
    val cuboidId = options.apply(SparderConstants.CUBOID_ID).toLong
    val filterStr = options(SparderConstants.FILTER_PUSH_DOWN)
    if (filterStr.nonEmpty) {
      val filterPushDown = GTUtil.deserializeGTFilter(HexUtils.toBytes(filterStr), gTInfo)
      var mutableRoaringBitmap: MutableRoaringBitmap = null
      for (divIndexReader <- pageIndexSpliceReader.getIndexReaderByCuboid(cuboidId).asScala) {
        indexTable = new ParquetPageIndexTable(fileSystem, path, divIndexReader)
        if (mutableRoaringBitmap == null) {
          mutableRoaringBitmap = new MutableRoaringBitmap(indexTable.lookup(filterPushDown).toRoaringBitmap)
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

  override def nextKeyValue(): Boolean = {
    if (reader == null)
      return false
    var data: util.List[Object] = null
    while (data == null) {
      data = reader.read()
      if (data == null) {
        return false
      }
      val bytes = data.get(0).asInstanceOf[Binary].getBytes
      //      if (binaryFilter != null && !binaryFilter.isMatch(bytes)) {
      //        data = null
      //      }
    }
    buildUnsafe(data.asScala.toList)
    true
  }

  override def buildUnsafe(data: List[Object]): Unit = {

    var ordinal = 0
    for (col <- data) {
      if (ordinal == 0 && rowTearerV2.hasPrimaryKey) {
        val binary = col.asInstanceOf[Binary].getBytes
        for (primarykey <- rowTearerV2.requireDimensions) {
          val value = rowTearerV2.decodePrimaryKey(ordinal, binary)
          if (value != null) {
            row.update(rowTearerV2.colToRowindex.apply(ordinal), UTF8String.fromString(value.toString))
          } else {
            row.update(rowTearerV2.colToRowindex.apply(ordinal), null)
          }
          ordinal = ordinal + 1
        }
      } else {

        row.update(rowTearerV2.colToRowindex.apply(ordinal), col.asInstanceOf[Binary].getBytes)
        ordinal = ordinal + 1
      }

    }
    currentRow = converter.apply(row)
  }

  override def getCurrentValue: UnsafeRow = {
    currentRow
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
