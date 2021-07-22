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

import com.google.gson.JsonParser
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder.{CreateFlatTable, NBuildSourceInfo}
import io.kyligence.kap.engine.spark.job.{FlatTableHelper, NSparkCubingUtil}
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.metadata.model._
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class CreateStreamingFlatTable(flatTable: IJoinedFlatTableDesc,
                               seg: NDataSegment,
                               toBuildTree: NSpanningTree,
                               ss: SparkSession,
                               sourceInfo: NBuildSourceInfo,
                               partitionColumn: String,
                               watermark: String) extends CreateFlatTable(flatTable, seg, toBuildTree, ss, sourceInfo) {

  import io.kyligence.kap.engine.spark.builder.CreateFlatTable._

  private val MAX_OFFSETS_PER_TRIGGER = "maxOffsetsPerTrigger"
  private val STARTING_OFFSETS = "startingOffsets"

  def generateStreamingDataset(config: KylinConfig): Dataset[Row] = {
    val model = flatTable.getDataModel
    val tableDesc = model.getRootFactTable.getTableDesc
    val kafkaParam = tableDesc.getKafkaConfig.getKafkaParam
    val kafkaJobParams = config.getStreamingKafkaConfigOverride.asScala
    kafkaJobParams.foreach { param =>
      param._1 match {
        case MAX_OFFSETS_PER_TRIGGER => if (param._2.toInt > 0) {
          val maxOffsetsPerTrigger = param._2.toInt
          kafkaParam.put("maxOffsetsPerTrigger", String.valueOf(maxOffsetsPerTrigger))
        }
        case STARTING_OFFSETS => if (!StringUtils.isEmpty(param._2)) {
          kafkaParam.put("startingOffsets", param._2)
        }
        case _ => kafkaParam.put(param._1, param._2)
      }
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
    val rootFactTable = changeSchemaToAliasDotName(
      CreateStreamingFlatTable.castDF(originFactTable, schema).alias(model.getRootFactTable.getAlias),
      model.getRootFactTable.getAlias)

    val factTable =
      if (!StringUtils.isEmpty(watermark)) {
        import org.apache.spark.sql.functions._
        val cols = model.getRootFactTable.getColumns.asScala.map(item => {
          col(NSparkCubingUtil.convertFromDot(item.getAliasDotName))
        }).toList
        rootFactTable.withWatermark(partitionColumn, watermark).groupBy(cols: _*).count()
      } else {
        rootFactTable
      }

    val ccCols = model.getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    val cleanLookupCC = cleanComputColumn(ccCols.toSeq, factTable.columns.toSet)
    val lookupTablesGlobal = generateLookupTableDataset(model, cleanLookupCC, ss)
    lookupTablesGlobal.foreach { case (_, df) =>
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }

    joinFactTableWithLookupTables(factTable, lookupTablesGlobal, model, ss)
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
            sourceInfo: NBuildSourceInfo,
            partitionColumn: String,
            watermark: String): CreateStreamingFlatTable = {
    new CreateStreamingFlatTable(flatTable, seg, toBuildTree, ss, sourceInfo, partitionColumn, watermark)
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
    val csvString = iter.next.get(0)
    val parser = new JsonParser()
    if (csvString == null || StringUtils.isEmpty(csvString.toString)) {
      EMPTY_ROW
    } else {
      try {
        convertJson2Row(csvString.toString(), parser)
      } catch {
        case e: Exception =>
          logger.error(s"parse json text fail ${e.toString}  stackTrace is: " +
            s"${e.getStackTrace.toString} line is: ${csvString}")
          EMPTY_ROW
      }
    }
  }

  def convertJson2Row(jsonStr: String, parser: JsonParser): Row = {
    val jsonMap = new mutable.HashMap[String, String]()
    val jsonObj = parser.parse(jsonStr).getAsJsonObject
    val entries = jsonObj.entrySet().asScala
    entries.foreach { entry =>
      jsonMap.put(entry.getKey.toLowerCase(Locale.ROOT), entry.getValue.getAsString)
    }
    Row((0 to parsedSchema.fields.length - 1).map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      if (!jsonMap.contains(colName)) { // key not exist
        null
      } else {
        val value = jsonMap.get(colName).getOrElse(null) // value not exist
        parsedSchema.fields(index).dataType match {
          case ShortType => if (value == null || value.equals("")) null else value.toShort
          case IntegerType => if (value == null || value.equals("")) null else value.toInt
          case LongType => if (value == null || value.equals("")) null else value.toLong
          case DoubleType => if (value == null || value.equals("")) null else value.toDouble
          case FloatType => if (value == null || value.equals("")) null else value.toFloat
          case BooleanType => if (value == null || value.equals("")) null else value.toBoolean
          case TimestampType => if (value == null || value.equals("")) null
          else new Timestamp(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DateType => if (value == null || value.equals("")) null
          else new Date(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DecimalType() => if (StringUtils.isEmpty(value)) null else BigDecimal(value)
          case _ => value
        }
      }
    }: _*)
  }

}
