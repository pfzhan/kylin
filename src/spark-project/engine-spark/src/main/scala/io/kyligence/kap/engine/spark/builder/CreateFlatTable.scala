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

package io.kyligence.kap.engine.spark.builder

import java.util

import com.google.common.collect.{Maps, Sets}
import io.kyligence.kap.engine.spark.builder.CreateFlatTable.GlobalDictType
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.metadata.model._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CreateFlatTable(val flatTable: IJoinedFlatTableDesc,
                      val seg: NDataSegment,
                      val toBuildTree: NSpanningTree,
                      val ss: SparkSession) extends Logging {

  def generateDataset(needEncode: Boolean = false, needJoin: Boolean = true): Dataset[Row] = {
    val model = flatTable.getDataModel

    var rootFactDataset: Dataset[Row] = CreateFlatTable.generateFactTableDataset(model, ss)
    rootFactDataset = CreateFlatTable.applyFilterCondition(flatTable, rootFactDataset)

    logInfo(s"Create flattable need join lookup tables ${needJoin}, need encode cols ${needEncode}")
    (needJoin, needEncode) match {
      case (true, true) =>
        var lookupTableDatasetMap = CreateFlatTable.generateLookupTableDataset(model.getJoinTables, ss)
        val globalDictTuple: GlobalDictType = CreateFlatTable.assemblyGlobalDictTuple(seg, toBuildTree)
        rootFactDataset = applyEncodeOperation(rootFactDataset, model.getRootFactTable.getAlias, globalDictTuple)
        lookupTableDatasetMap = applyEncodeOperation(lookupTableDatasetMap, globalDictTuple)
        rootFactDataset = CreateFlatTable.joinFactTableWithLookupTables(rootFactDataset, lookupTableDatasetMap, model, ss)
      case (true, false) =>
        val lookupTableDatasetMap = CreateFlatTable.generateLookupTableDataset(model.getJoinTables, ss)
        rootFactDataset = CreateFlatTable.joinFactTableWithLookupTables(rootFactDataset, lookupTableDatasetMap, model, ss)
      case (false, true) =>
        val globalDictTuple: GlobalDictType = CreateFlatTable.assemblyGlobalDictTuple(seg, toBuildTree)
        rootFactDataset = applyEncodeOperation(rootFactDataset, model.getRootFactTable.getAlias, globalDictTuple)
      case _ =>
    }

    flatTable match {
      case joined: NCubeJoinedFlatTableDesc =>
        CreateFlatTable.changeSchemeToColumnIndice(rootFactDataset, joined)
      case unsupported =>
        throw new UnsupportedOperationException(
          s"Unsupported flat table desc type : ${unsupported.getClass}.")
    }
  }

  private def applyEncodeOperation(dataset: Dataset[Row], tableName: String, globalDictTuple: GlobalDictType): Dataset[Row] = {
    buildDict(dataset, tableName, globalDictTuple)
    encodeTable(dataset, tableName, globalDictTuple)
  }

  private def applyEncodeOperation(lookupTables: util.Map[JoinTableDesc, Dataset[Row]],
                                   globalDictTuple: GlobalDictType): util.Map[JoinTableDesc, Dataset[Row]] = {
    lookupTables.asScala.foreach(
      tuple => {
        lookupTables.put(tuple._1, applyEncodeOperation(tuple._2, tuple._1.getTableRef.getTableName, globalDictTuple))
      }
    )
    lookupTables
  }

  private def buildDict(ds: Dataset[Row], tableName: String, globalDictTuple: GlobalDictType): Unit = {
    globalDictTuple._1.asScala.get(tableName) match {
      case Some(cols) =>
        val dictionaryBuilder = new DFDictionaryBuilder(ds, seg, ss, cols)
        dictionaryBuilder.buildDictionary()
      case None => None
    }
  }

  private def encodeTable(ds: Dataset[Row], tableName: String, globalDictTuple: GlobalDictType): Dataset[Row] = {
    globalDictTuple._2.asScala.get(tableName) match {
      case Some(cols) =>
        DFTableEncoder.encodeTable(ds, seg, cols)
      case None => ds
    }
  }
}


object CreateFlatTable extends Logging {
  type GlobalDictType = (util.Map[String, util.Set[TblColRef]], util.Map[String, util.Set[TblColRef]])

  def generateFullFlatTable(model: NDataModel, ss: SparkSession): Dataset[Row] = {
    val rootFactDataset: Dataset[Row] = generateFactTableDataset(model, ss)
    val lookupTableDataset: util.Map[JoinTableDesc, Dataset[Row]] = generateLookupTableDataset(model.getJoinTables, ss)
    joinFactTableWithLookupTables(rootFactDataset, lookupTableDataset, model, ss)
  }

  private def generateFactTableDataset(model: NDataModel, ss: SparkSession): Dataset[Row] = {
    val rootFactDesc = model.getRootFactTable.getTableDesc
    val rootFactDataset = ss.table(rootFactDesc).alias(model.getRootFactTable.getAlias)
    changeSchemaToAliasDotName(rootFactDataset, model.getRootFactTable.getAlias)
  }

  private def generateLookupTableDataset(joinTableDesc: util.List[JoinTableDesc],
                                         ss: SparkSession): util.Map[JoinTableDesc, Dataset[Row]] = {
    val lookupTables = Maps.newLinkedHashMap[JoinTableDesc, Dataset[Row]]()
    joinTableDesc.asScala.foreach(
      joinDesc => {
        var lookupTable = ss.table(joinDesc.getTableRef.getTableDesc).alias(joinDesc.getAlias)
        logInfo(s"Table schema ${lookupTable.schema.treeString}")
        lookupTable = changeSchemaToAliasDotName(lookupTable, joinDesc.getAlias)
        lookupTables.put(joinDesc, lookupTable)
      }
    )
    lookupTables
  }

  private def applyFilterCondition(flatTable: IJoinedFlatTableDesc, ds: Dataset[Row]): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      var afterConvertCondition = model.getFilterCondition
      afterConvertCondition = replaceDot(model.getFilterCondition, model)
      logInfo(s"Filter condition is $afterConvertCondition")
      afterFilter = afterFilter.where(afterConvertCondition)
    }

    val partDesc = model.getPartitionDesc
    if (partDesc != null && partDesc.getPartitionDateColumn != null) {
      val segRange = flatTable.getSegRange
      if (segRange != null && !segRange.isInfinite) {
        var afterConvertPartition = partDesc.getPartitionConditionBuilder
          .buildDateRangeCondition(partDesc, null, segRange)
        afterConvertPartition = replaceDot(afterConvertPartition, model)
        logInfo(s"Partition filter $afterConvertPartition")
        afterFilter = afterFilter.where(afterConvertPartition) // TODO: mp not supported right now
      }
    }
    afterFilter
  }

  def joinFactTableWithLookupTables(rootFactDataset: Dataset[Row],
                                    lookupTableDatasetMap: util.Map[JoinTableDesc, Dataset[Row]],
                                    model: NDataModel,
                                    ss: SparkSession): Dataset[Row] = {
    lookupTableDatasetMap.asScala.foldLeft(rootFactDataset)(
      (joinedDataset: Dataset[Row], tuple: (JoinTableDesc, Dataset[Row])) =>
        joinTableDataset(model.getRootFactTable.getTableDesc, tuple._1, joinedDataset, tuple._2, ss))
  }

  def joinTableDataset(rootFactDesc: TableDesc,
                       lookupDesc: JoinTableDesc,
                       rootFactDataset: Dataset[Row],
                       lookupDataset: Dataset[Row],
                       ss: SparkSession): Dataset[Row] = {
    var afterJoin = rootFactDataset
    val join = lookupDesc.getJoin
    if (join != null && !StringUtils.isEmpty(join.getType)) {
      val joinType = join.getType.toUpperCase
      val pk = join.getPrimaryKeyColumns
      val fk = join.getForeignKeyColumns
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$lookupDesc, pk: ${pk.mkString(",")}")
      }
      val condition = fk.zip(pk).map(joinKey =>
        col(NSparkCubingUtil.convertFromDot(joinKey._1.getIdentity))
          .equalTo(col(NSparkCubingUtil.convertFromDot(joinKey._2.getIdentity))))
        .reduce(_.and(_))
      logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")
      logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
      afterJoin = afterJoin.join(lookupDataset, condition, joinType)
    }
    afterJoin
  }

  def changeSchemeToColumnIndice(ds: Dataset[Row], flatTable: NCubeJoinedFlatTableDesc): Dataset[Row] = {
    val structType = ds.schema
    val colIndices = flatTable.getIndices.asScala
    val columnNameToIndex = flatTable.getAllColumns
      .asScala
      .map(column => NSparkCubingUtil.convertFromDot(column.getExpressionInSourceDB))
      .zip(colIndices)
    val columnToIndexMap = columnNameToIndex.toMap
    val encodeSeq = structType.filter(_.name.endsWith(DFTableEncoder.ENCODE_SUFFIX)).map {
      tp =>
        val originNam = tp.name.replaceFirst(DFTableEncoder.ENCODE_SUFFIX, "")
        val index = columnToIndexMap.apply(originNam)
        col(tp.name).alias(index.toString + DFTableEncoder.ENCODE_SUFFIX)
    }
    val columns = columnNameToIndex.map(tp => expr(tp._1).alias(tp._2.toString))
    logInfo(s"Select model column is ${columns.mkString(",")}")
    logInfo(s"Select model encoding column is ${encodeSeq.mkString(",")}")
    val selectedColumns = columns ++ encodeSeq
    logInfo(s"Select model all column is ${selectedColumns.mkString(",")}")
    ds.select(selectedColumns: _*)
  }

  def replaceDot(original: String, model: NDataModel): String = {
    val sb = new StringBuilder(original)

    for (namedColumn <- model.getAllNamedColumns.asScala) {
      var start = 0
      while (sb.toString.toLowerCase.indexOf(
        namedColumn.getAliasDotColumn.toLowerCase) != -1) {
        start = sb.toString.toLowerCase
          .indexOf(namedColumn.getAliasDotColumn.toLowerCase)
        sb.replace(start,
          start + namedColumn.getAliasDotColumn.length,
          NSparkCubingUtil.convertFromDot(namedColumn.getAliasDotColumn))
      }
    }
    sb.toString()
  }

  def assemblyGlobalDictTuple(seg: NDataSegment, toBuildTree: NSpanningTree): GlobalDictType = {
    val toBuildDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(seg, toBuildTree)
    val toBuildDictMap: util.Map[String, util.Set[TblColRef]] = convert(toBuildDictSet)

    val globalDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree)
    val globalDictMap: util.Map[String, util.Set[TblColRef]] = convert(globalDictSet)

    (toBuildDictMap, globalDictMap)
  }

  def convert(colSet: util.Set[TblColRef]): util.Map[String, util.Set[TblColRef]] = {
    val encodeColMap: util.Map[String, util.Set[TblColRef]] = Maps.newHashMap[String, util.Set[TblColRef]]()
    colSet.asScala.foreach {
      col =>
        val tableName = col.getTableRef.getAlias
        if (encodeColMap.containsKey(tableName)) {
          encodeColMap.get(tableName).add(col)
        } else {
          encodeColMap.put(tableName, Sets.newHashSet(col))
        }
    }
    encodeColMap
  }

  def changeSchemaToAliasDotName(original: Dataset[Row],
                                 alias: String): Dataset[Row] = {
    val sf = original.schema.fields
    val newSchema = sf
      .map(field => NSparkCubingUtil.convertFromDot(alias + "." + field.name))
      .toSeq
    val newdf = original.toDF(newSchema: _*)
    logInfo(s"After change alias from ${original.schema.treeString} to ${newdf.schema.treeString}")
    newdf
  }

  /*
   * Convert IJoinedFlatTableDesc to SQL statement
   */
  def generateSelectDataStatement(flatDesc: IJoinedFlatTableDesc,
                                  singleLine: Boolean,
                                  skipAs: Array[String]): String = {
    val sep: String = {
      if (singleLine) " "
      else "\n"
    }
    val skipAsList = {
      if (skipAs == null) ListBuffer.empty[String]
      else skipAs.toList
    }
    val sql: StringBuilder = new StringBuilder
    sql.append("SELECT" + sep)
    for (i <- 0 until flatDesc.getAllColumns.size()) {
      val col: TblColRef = flatDesc.getAllColumns.get(i)
      sql.append(",")
      val colTotalName: String =
        String.format("%s.%s", col.getTableRef.getTableName, col.getName)
      if (skipAsList.contains(colTotalName)) {
        sql.append(col.getExpressionInSourceDB + sep)
      } else {
        sql.append(col.getExpressionInSourceDB + " as " + colName(col) + sep)
      }
    }
    appendJoinStatement(flatDesc, sql, singleLine)
    appendWhereStatement(flatDesc, sql, singleLine)
    sql.toString
  }

  def appendJoinStatement(flatDesc: IJoinedFlatTableDesc,
                          sql: StringBuilder,
                          singleLine: Boolean): Unit = {
    val sep: String =
      if (singleLine) " "
      else "\n"
    val dimTableCache: util.Set[TableRef] = Sets.newHashSet[TableRef]
    val model: NDataModel = flatDesc.getDataModel
    val rootTable: TableRef = model.getRootFactTable
    sql.append(
      "FROM " + flatDesc.getDataModel.getRootFactTable.getTableIdentity + " as " + rootTable.getAlias + " " + sep)
    for (lookupDesc <- model.getJoinTables.asScala) {
      val join: JoinDesc = lookupDesc.getJoin
      if (join != null && join.getType == "" == false) {
        val joinType: String = join.getType.toUpperCase
        val dimTable: TableRef = lookupDesc.getTableRef
        if (!dimTableCache.contains(dimTable)) {
          val pk: Array[TblColRef] = join.getPrimaryKeyColumns
          val fk: Array[TblColRef] = join.getForeignKeyColumns
          if (pk.length != fk.length) {
            throw new RuntimeException(
              "Invalid join condition of lookup table:" + lookupDesc)
          }
          sql.append(
            joinType + " JOIN " + dimTable.getTableIdentity + " as " + dimTable.getAlias + sep)
          sql.append("ON ")
          var i: Int = 0
          while ( {
            i < pk.length
          }) {
            if (i > 0) sql.append(" AND ")
            sql.append(
              fk(i).getExpressionInSourceDB + " = " + pk(i).getExpressionInSourceDB)

            {
              i += 1
              i - 1
            }
          }
          sql.append(sep)
          dimTableCache.add(dimTable)
        }
      }
    }
  }

  private def appendWhereStatement(flatDesc: IJoinedFlatTableDesc,
                                   sql: StringBuilder,
                                   singleLine: Boolean): Unit = {
    val sep: String =
      if (singleLine) " "
      else "\n"
    val whereBuilder: StringBuilder = new StringBuilder
    whereBuilder.append("WHERE 1=1")
    val model: NDataModel = flatDesc.getDataModel
    if (StringUtils.isNotEmpty(model.getFilterCondition)) {
      whereBuilder
        .append(" AND (")
        .append(model.getFilterCondition)
        .append(") ")
    }
    val partDesc: PartitionDesc = model.getPartitionDesc
    val segRange: SegmentRange[_ <: Comparable[_]] = flatDesc.getSegRange
    if (flatDesc.getSegment != null && partDesc != null
      && partDesc.getPartitionDateColumn != null && segRange != null && !segRange.isInfinite) {
      val builder =
        flatDesc.getDataModel.getPartitionDesc.getPartitionConditionBuilder
      if (builder != null) {
        whereBuilder.append(" AND (")
        whereBuilder.append(
          builder
            .buildDateRangeCondition(partDesc, flatDesc.getSegment, segRange))
        whereBuilder.append(")" + sep)
      }

      sql.append(whereBuilder.toString)
    }
  }

  def colName(col: TblColRef): String = {
    col.getTableAlias + "_" + col.getName
  }

}
