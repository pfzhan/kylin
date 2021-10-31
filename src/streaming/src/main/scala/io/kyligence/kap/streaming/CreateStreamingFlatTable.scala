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

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder.{CreateFlatTable, NBuildSourceInfo}
import io.kyligence.kap.engine.spark.job.{FlatTableHelper, NSparkCubingUtil}
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.cube.utils.StreamingUtils
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model._
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

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
  var lookupTablesGlobal: mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = null
  var factTableDataset: Dataset[Row] = null
  var tableRefreshInterval = -1L

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

    factTableDataset =
      if (!StringUtils.isEmpty(watermark)) {
        import org.apache.spark.sql.functions._
        val cols = model.getRootFactTable.getColumns.asScala.map(item => {
          col(NSparkCubingUtil.convertFromDot(item.getAliasDotName))
        }).toList
        rootFactTable.withWatermark(partitionColumn, watermark).groupBy(cols: _*).count()
      } else {
        rootFactTable
      }
    tableRefreshInterval = StreamingUtils.parseTableRefreshInterval(config.getStreamingTableRefreshInterval())
    loadLookupTables()
    joinFactTableWithLookupTables(factTableDataset, lookupTablesGlobal, model, ss)
  }

  def loadLookupTables(): Unit = {
    val ccCols = model().getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    val cleanLookupCC = cleanComputColumn(ccCols.toSeq, factTableDataset.columns.toSet)
    lookupTablesGlobal = generateLookupTableDataset(model, cleanLookupCC, ss)
    lookupTablesGlobal.foreach { case (_, df) =>
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }
  }

  def shouldRefreshTable(): Boolean = {
    tableRefreshInterval > 0
  }

  def model(): NDataModel = {
    flatTable.getDataModel
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