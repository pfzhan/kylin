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

package io.kyligence.kap.streaming

import java.sql.{Date, Timestamp}
import java.util.Locale

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder.{CreateFlatTable, NBuildSourceInfo}
import io.kyligence.kap.engine.spark.job.FlatTableHelper
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel
import io.kyligence.kap.source.kafka.NSparkKafkaSource
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.{DateFormat, JsonUtil}
import org.apache.kylin.metadata.model._
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class CreateStreamingFlatTable(flatTable: IJoinedFlatTableDesc,
                               seg: NDataSegment,
                               toBuildTree: NSpanningTree,
                               ss: SparkSession,
                               sourceInfo: NBuildSourceInfo) extends CreateFlatTable(flatTable, seg, toBuildTree, ss, sourceInfo) {

  import io.kyligence.kap.engine.spark.builder.CreateFlatTable._

  def generateStreamingDataset(needJoin: Boolean = true, duration: Int, maxRatePerPartition: Int): Dataset[Row] = {

    val model = flatTable.getDataModel
    val tableDesc = model.getRootFactTable.getTableDesc
    val kafkaParam = tableDesc.getKafkaConfig.getKafkaParam

    if (maxRatePerPartition > 0) {
      val source = SourceFactory.getSource(1).asInstanceOf[NSparkKafkaSource]
      val maxOffsetsPerTrigger = duration / 1000 * maxRatePerPartition * source.getPartitions(kafkaParam)

      kafkaParam.put("maxOffsetsPerTrigger", String.valueOf(maxOffsetsPerTrigger))
    }

    val originFactTable = SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, kafkaParam)

    val schema =
      StructType(
        tableDesc.getColumns.map { columnDescs =>
          StructField(columnDescs.getName, SparderTypeUtil.toSparkType(columnDescs.getType, false))
        }
      )

    val factTable = CreateStreamingFlatTable.castDF(originFactTable, schema)
    val ccCols = model.getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet

    var rootFactDataset: Dataset[Row] = factTable.alias(model.getRootFactTable.getAlias)

    rootFactDataset = changeSchemaToAliasDotName(rootFactDataset, model.getRootFactTable.getAlias)

    val cleanLookupCC = cleanComputColumn(ccCols.toSeq, rootFactDataset.columns.toSet)
    val lookupTablesGlobal = generateLookupTableDataset(model, cleanLookupCC, ss)
    lookupTablesGlobal.foreach { case (_, df) =>
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }

    joinFactTableWithLookupTables(rootFactDataset, lookupTablesGlobal, model, ss)
  }

  def encodeStreamingDataset(seg: NDataSegment, model: NDataModel, batchDataset: Dataset[Row]): Dataset[Row] = {
    val ccCols = model.getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    val (dictCols, encodeCols): GlobalDictType = assemblyGlobalDictTuple(seg, toBuildTree)
    val encodedDataset = encodeWithCols(batchDataset, ccCols, dictCols, encodeCols)
    val filterEncodedDataset = FlatTableHelper.applyFilterCondition(flatTable, encodedDataset, true)

    flatTable match {
      case joined: NCubeJoinedFlatTableDesc =>
        changeSchemeToColumnIndice(filterEncodedDataset, joined)
      case unsupported =>
        throw new UnsupportedOperationException(
          s"Unsupported flat table desc type : ${unsupported.getClass}.")
    }
  }
}

object CreateStreamingFlatTable {
  def apply(flatTable: IJoinedFlatTableDesc,
            seg: NDataSegment,
            toBuildTree: NSpanningTree,
            ss: SparkSession,
            sourceInfo: NBuildSourceInfo): CreateStreamingFlatTable = {
    new CreateStreamingFlatTable(flatTable, seg, toBuildTree, ss, sourceInfo)
  }

  def castDF(df: DataFrame, parsedSchema: StructType): DataFrame = {
    df.selectExpr("CAST(value AS STRING) as rawValue")
      .mapPartitions { rows =>
        val newRows = new PartitionRowIterator(rows, parsedSchema)
        newRows.filter(row => row.size == parsedSchema.length)
      }(RowEncoder(parsedSchema))
  }
}

class PartitionRowIterator(iter: Iterator[Row], parsedSchema: StructType) extends Iterator[Row] {
  val logger = LoggerFactory.getLogger(classOf[PartitionRowIterator])

  val EMPTY_ROW = Row()

  val DATE_PATTERN = Array[String](DateFormat.COMPACT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS)

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Row = {
    val row = iter.next
    val csvString = row.get(0)
    if (csvString == null || StringUtils.isEmpty(csvString.toString)) {
      EMPTY_ROW
    } else {
      try {
        val json = JsonUtil.readValueAsMap(csvString.toString())
        Row((0 to parsedSchema.fields.length - 1).map { index =>
          val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
          if (!json.containsKey(colName)) {
            null
          } else {
            parsedSchema.fields(index).dataType match {
              case ShortType => json.get(colName).toShort
              case IntegerType => json.get(colName).toInt
              case LongType => json.get(colName).toLong
              case DoubleType => json.get(colName).toDouble
              case FloatType => json.get(colName).toFloat
              case BooleanType => json.get(colName).toBoolean
              case TimestampType => new Timestamp(DateUtils.parseDate(json.get(colName), DATE_PATTERN).getTime)
              case DateType => new Date(DateUtils.parseDate(json.get(colName), DATE_PATTERN).getTime)
              case _ => json.get(colName)
            }
          }
        }: _*)
      } catch {
        case e: Exception =>
          logger.error(s"parse json text fail ${e.toString}  stackTrace is: " +
            s"${e.getStackTrace.toString} line is: ${csvString}")
          EMPTY_ROW
      }
    }
  }
}
