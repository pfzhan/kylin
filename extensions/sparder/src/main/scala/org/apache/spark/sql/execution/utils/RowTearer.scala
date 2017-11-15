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
package org.apache.spark.sql.execution.utils

import java.nio.ByteBuffer

import org.apache.kylin.common.util.{ByteArray, BytesUtil, DateFormat, Dictionary}
import org.apache.kylin.gridtable.{GTInfo, IGTCodeSystem}
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.manager.SparderSourceDictionary
import org.apache.spark.sql.types._

class RowTearer(requireSchema: StructType,
                gTInfo: GTInfo,
                dictMap: Map[String, String]) extends Logging{
  val colToRowindex: Map[Int, Int] = requireSchema.fieldNames
    .map(name => SchemaProcessor.parseFactTableSchemaName(name))
    .zipWithIndex
    .sortBy(_._1._2)
    .zipWithIndex
    .map(tuple => (tuple._2, tuple._1._2))
    .toMap

  val requireDimensions: List[Int] = findPrimaryColumn(requireSchema, gTInfo)
  val hasPrimaryKey: Boolean = requireDimensions.nonEmpty
  val (scanColumns: ImmutableRoaringBitmap,
       rowToGTInfo: Map[Int, Int],
       gTInfoToRow: Map[Int, Int]) =
    generateScanColumns(requireSchema, requireDimensions, gTInfo)

  val dictionaryMapping: Map[Int, Dictionary[String]] = {
    //  这里的int是真实的列

    dictMap.map { entry =>
      (gTInfoToRow.getOrElse(entry._1.toInt, -1),
       SparderSourceDictionary.getOrCreate(entry._2).getDictionaryObject)
    }

  }

  val datetimeFamilyColsInRow: Set[Int] = requireDimensions
    .filter(id => gTInfo.getColumnType(id).isDateTimeFamily)
    .map(id => gTInfoToRow.apply(id))
    .toSet

  var primaryKeyRange: mutable.Map[Int, (Int, Int)] = {
    var map = mutable.HashMap.empty[Int, (Int, Int)]
    var position = 0
    for (key <- gTInfo.getPrimaryKey.asScala) {
      val i = key.intValue()
      val length = gTInfo.getCodeSystem.getDimEnc(i).getLengthOfEncoding
      if (requireDimensions.contains(i)) {
        map.put(gTInfoToRow(i), (position, position + length))
      }
      position = position + length
    }
    map
  }

  def generateScanColumns(requiredSchema: StructType,
                          requiredDimensions: List[Int],
                          gTInfo: GTInfo)
    : (ImmutableRoaringBitmap, Map[Int, Int], Map[Int, Int]) = {
    // must be orderly
    var rowToGTInfo = mutable.LinkedHashMap.empty[Int, Int]

    if (requiredDimensions.size == requiredSchema.size) {
      //  查询的都是rowkey
      val bitmap = new MutableRoaringBitmap()
      bitmap.add(0)
      val rowToGTInfo = requiredDimensions.indices.zip(requiredDimensions).toMap
      val gTInfoToRow = rowToGTInfo.map(tuple => (tuple._2, tuple._1))
      return (bitmap, rowToGTInfo, gTInfoToRow)
    }

    val array = gTInfo.getPrimaryKey.asScala.toArray
    //  meansure
    val requiredNonPrimaryGtBlocks = requiredSchema
      .map(
        schema =>
          SchemaProcessor
            .parseFactTableSchemaName(schema.name)
            ._2 - array.length + 1)
      .filter(_ > 0)
    val ans = new MutableRoaringBitmap
    if (requiredDimensions.nonEmpty) {
      rowToGTInfo ++= requiredDimensions.indices.zip(requiredDimensions).toMap
      ans.add(0)
    }
    var ordinal = requiredDimensions.size
    for (number <- requiredNonPrimaryGtBlocks) {
      ans.add(number)
      val gtinfoOrd = number + array.length - 1
      rowToGTInfo += (ordinal -> gtinfoOrd)
      ordinal = ordinal + 1
    }
    val gTInfoToRow = rowToGTInfo.map(tuple => (tuple._2, tuple._1)).toMap
    (ans, rowToGTInfo.toMap, gTInfoToRow)
  }

  def findPrimaryColumn(requiredSchema: StructType,
                        gTInfo: GTInfo): List[Int] = {
    val array = gTInfo.getPrimaryKey.asScala.toArray
    requiredSchema
      .map(schema => SchemaProcessor.parseFactTableSchemaName(schema.name)._2)
      .filter(array.contains(_))
      .toList
  }

  def decodePrimaryKey(colIndexInRow: Int, byte: Array[Byte]): Object = {
    val start = System.nanoTime()
    val range = primaryKeyRange.apply(colIndexInRow)
    if (dictionaryMapping.contains(colIndexInRow)) {
      val dictionaryId =
        BytesUtil.readUnsigned(byte, range._1, range._2 - range._1)
      var value =
        dictionaryMapping.apply(colIndexInRow).getValueFromId(dictionaryId)

      if (datetimeFamilyColsInRow.contains(colIndexInRow)) {
        value = String.valueOf(DateFormat.stringToMillis(value))
      }
      value
    } else {
      var value = gTInfo.getCodeSystem.decodeColumnValue(
        rowToGTInfo.apply(colIndexInRow),
        ByteBuffer.wrap(byte, range._1, range._2 - range._1))
      if (datetimeFamilyColsInRow.contains(colIndexInRow)) {
        value = String.valueOf(DateFormat.stringToMillis(value.toString))
      }
      value
    }
  }

  var count = 0L
  var totalTime = 0L
  val codeSystem: IGTCodeSystem = gTInfo.getCodeSystem
  val info = gTInfo

  def decodePrimaryKey(index: Int, byte: ByteArray): Object = {
    val start = System.nanoTime()
    val range = primaryKeyRange.apply(index)
    if (dictionaryMapping.contains(index)) {
      val dictionaryId =
        BytesUtil.readUnsigned(byte, range._1, range._2 - range._1)
      var value = dictionaryMapping.apply(index).getValueFromId(dictionaryId)
      if (datetimeFamilyColsInRow.contains(index)) {
        value = String.valueOf(DateFormat.stringToMillis(value))
      }
      count += 1
      val time = System.nanoTime() - start
      totalTime += time
      if (count % 100000 == 0) {
        logInfo(totalTime / count + "diction")
      }
      value
    } else {
      var value = codeSystem.decodeColumnValue(
        rowToGTInfo.apply(index),
        ByteBuffer.wrap(byte.array(),
                        range._1 + byte.offset(),
                        range._2 - range._1))
      if (datetimeFamilyColsInRow.contains(index)) {
        value = String.valueOf(DateFormat.stringToMillis(value.toString))
      }
      count += 1
      val time = System.nanoTime() - start
      totalTime += time
      if (count % 100000 == 0) {
        logInfo(totalTime / count + "normal")
      }
      value
    }
  }

}

object RowTearer {
  def apply(requireSchema: StructType,
            gTInfo: GTInfo,
            dictMap: Map[String, String]): RowTearer =
    new RowTearer(requireSchema, gTInfo, dictMap)
}
