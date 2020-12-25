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

import com.google.common.collect.Sets
import io.kyligence.kap.engine.spark.builder.DFBuilderHelper._
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import io.kyligence.kap.engine.spark.job.{FiltersUtil, TableMetaManager}
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import io.kyligence.kap.metadata.cube.model.{NDataSegment, SegmentFlatTableDesc}
import io.kyligence.kap.metadata.model.NDataModel
import io.kyligence.kap.query.util.KapQueryUtil
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util.{Locale, Objects}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

class SegmentFlatTable(private val sparkSession: SparkSession, //
                       private val tableDesc: SegmentFlatTableDesc) extends LogEx {

  import SegmentFlatTable._

  protected final val project = tableDesc.getProject
  protected final val spanningTree = tableDesc.getSpanningTree
  protected final val dataSegment = tableDesc.getDataSegment
  protected final val dataModel = tableDesc.getDataModel
  protected final val indexPlan = tableDesc.getIndexPlan
  protected final val segmentRange = tableDesc.getSegmentRange
  protected lazy final val flatTablePath = tableDesc.getFlatTablePath
  protected lazy final val factTableViewPath = tableDesc.getFactTableViewPath
  protected lazy final val workingDir = tableDesc.getWorkingDir
  protected lazy final val sampleRowCount = tableDesc.getSampleRowCount

  protected final val segmentId = dataSegment.getId

  protected lazy final val fullDS = newDS()
  private lazy final val fastDS = newFastDS()

  private val rootFactTable = dataModel.getRootFactTable

  // Flat table.
  private lazy val shouldPersistFT = tableDesc.shouldPersistFlatTable()
  private lazy val isFTReady = dataSegment.isFlatTableReady && HadoopUtil.getWorkingFileSystem.exists(flatTablePath)

  // Fact table view.
  private lazy val isFTV = rootFactTable.getTableDesc.isView
  private lazy val shouldPersistFTV = tableDesc.shouldPersistView()
  private lazy val isFTVReady = dataSegment.isFactViewReady && HadoopUtil.getWorkingFileSystem.exists(factTableViewPath)

  // Global dictionary.
  private lazy val isGDReady = dataSegment.isDictReady

  private lazy val needJoin = {
    val join = tableDesc.shouldJoinLookupTables
    logInfo(s"FLAT-TABLE NEED-JOIN $join")
    join
  }

  private lazy val factTableDS = newFactTableDS()
  private lazy val fastFactTableDS = newFastFactTableDS()

  // By design, COMPUTED-COLUMN could only be defined on fact table.
  private lazy val factTableCCs = rootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet

  def getFastDS(): Dataset[Row] = {
    fastDS
  }

  def getDS(): Dataset[Row] = {
    fullDS
  }

  def gatherStatistics(): Statistics = {
    logInfo(s"Gather statistics FLAT-TABLE segment $segmentId")
    sparkSession.sparkContext.setJobDescription("Gather statistics FLAT-TABLE.")
    val statistics = gatherStatistics(fullDS)
    sparkSession.sparkContext.setJobDescription(null)
    statistics
  }

  protected def newFastDS(): Dataset[Row] = {
    val recoveredDS = tryRecoverFTDS()
    if (recoveredDS.nonEmpty) {
      return recoveredDS.get
    }
    var flatTableDS = if (needJoin) {
      val lookupTableCCs = cleanComputedColumns(factTableCCs, fastFactTableDS.columns.toSet)
      val lookupTableDSMap = newLookupTableDS(lookupTableCCs)
      if (inferFiltersEnabled) {
        FiltersUtil.initFilters(tableDesc, lookupTableDSMap)
      }
      val jointDS = joinFactTableWithLookupTables(fastFactTableDS, lookupTableDSMap, dataModel, sparkSession)
      withColumn(jointDS, factTableCCs)
    } else {
      fastFactTableDS
    }
    flatTableDS = applyFilterCondition(flatTableDS)
    changeSchemeToColumnId(flatTableDS, tableDesc)
  }

  protected def newDS(): Dataset[Row] = {
    val recoveredDS = tryRecoverFTDS()
    if (recoveredDS.nonEmpty) {
      return recoveredDS.get
    }

    val encodeCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(dataSegment, spanningTree).asScala.toSet
    val dictCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(dataSegment, spanningTree).asScala.toSet
    val encodedFactTableDS = encodeWithCols(factTableDS, factTableCCs, dictCols, encodeCols)

    var flatTableDS = if (needJoin) {
      val lookupTableCCs = cleanComputedColumns(factTableCCs, encodedFactTableDS.columns.toSet)
      val encodedLookupTableDSMap = newLookupTableDS(lookupTableCCs)
        .map(lp => (lp._1, encodeWithCols(lp._2, lookupTableCCs, dictCols, encodeCols)))

      if (encodedLookupTableDSMap.nonEmpty) {
        generateDimensionTableMeta(encodedLookupTableDSMap)
      }
      val allTableDS = Seq(encodedFactTableDS) ++ encodedLookupTableDSMap.values

      if (inferFiltersEnabled) {
        FiltersUtil.initFilters(tableDesc, encodedLookupTableDSMap)
      }

      val jointDS = joinFactTableWithLookupTables(encodedFactTableDS, encodedLookupTableDSMap, dataModel, sparkSession)
      encodeWithCols(jointDS,
        filterCols(allTableDS, factTableCCs),
        filterCols(allTableDS, dictCols),
        filterCols(allTableDS, encodeCols))
    } else {
      encodedFactTableDS
    }

    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setDictReady(true))

    flatTableDS = applyFilterCondition(flatTableDS)
    flatTableDS = changeSchemeToColumnId(flatTableDS, tableDesc)
    tryPersistFTDS(flatTableDS)
  }

  private def newFastFactTableDS(): Dataset[Row] = {
    val partDS = newPartitionedFTDS(needFast = true)
    fulfillDS(partDS, factTableCCs, rootFactTable)
  }

  private def newFactTableDS(): Dataset[Row] = {
    val partDS = newPartitionedFTDS()
    fulfillDS(partDS, factTableCCs, rootFactTable)
  }

  private def newPartitionedFTDS(needFast: Boolean = false): Dataset[Row] = {
    if (isFTVReady) {
      logInfo(s"Skip FACT-TABLE-VIEW segment $segmentId.")
      return sparkSession.read.parquet(factTableViewPath.toString)
    }
    val tableDS = newTableDS(rootFactTable)
    val partDS = applyPartitionDesc(tableDS)
    if (needFast || !isFTV) {
      return partDS
    }
    tryPersistFTVDS(partDS)
  }

  private def newLookupTableDS(cols: Set[TblColRef]): mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = {
    val ret = mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]()
    dataModel.getJoinTables.asScala
      .foreach { joinDesc =>
        val tableRef = joinDesc.getTableRef
        val tableDS = newTableDS(tableRef)
        ret.put(joinDesc, fulfillDS(tableDS, cols, tableRef))
      }
    ret
  }

  protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    if (Objects.isNull(descDRP) //
      || Objects.isNull(descDRP.getPartitionDateColumn) //
      || Objects.isNull(segmentRange) //
      || segmentRange.isInfinite) {
      logInfo(s"No available PARTITION-CONDITION segment $segmentId")
      return originDS
    }

    val condition = descDRP.getPartitionConditionBuilder //
      .buildDateRangeCondition(descDRP, null, segmentRange)
    logInfo(s"Apply PARTITION-CONDITION $condition segment $segmentId")
    originDS.where(condition)
  }

  private def applyFilterCondition(originDS: Dataset[Row]): Dataset[Row] = {
    if (StringUtils.isBlank(dataModel.getFilterCondition)) {
      logInfo(s"No available FILTER-CONDITION segment $segmentId")
      return originDS
    }
    val expression = KapQueryUtil.massageExpression(dataModel, project, //
      dataModel.getFilterCondition, null)
    val converted = replaceDot(expression, dataModel)
    val condition = s" (1=1) AND ($converted)"
    logInfo(s"Apply FILTER-CONDITION: $condition segment $segmentId")
    originDS.where(condition)
  }

  private def tryPersistFTVDS(tableDS: Dataset[Row]): Dataset[Row] = {
    if (!shouldPersistFTV) {
      return tableDS
    }
    logInfo(s"Persist FACT-TABLE-VIEW $factTableViewPath")
    sparkSession.sparkContext.setJobDescription("Persist FACT-TABLE-VIEW.")
    tableDS.write.mode(SaveMode.Overwrite).parquet(factTableViewPath.toString)
    // Checkpoint fact table view.
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setFactViewReady(true))
    val newDS = sparkSession.read.parquet(factTableViewPath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    newDS
  }

  private def tryPersistFTDS(tableDS: Dataset[Row]): Dataset[Row] = {
    if (!shouldPersistFT) {
      return tableDS
    }
    logInfo(s"Persist FLAT-TABLE $flatTablePath")
    sparkSession.sparkContext.setJobDescription("Persist FLAT-TABLE.")
    tableDS.write.mode(SaveMode.Overwrite).parquet(flatTablePath.toString)
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => {
      copied.setFlatTableReady(true)
      if (dataSegment.isFlatTableReady) {
        // KE-14714 if flat table is updated, there might be some data inconsistency across indexes
        copied.setStatus(SegmentStatusEnum.WARNING)
      }
    })
    val newDS = sparkSession.read.parquet(flatTablePath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    newDS
  }

  private def tryRecoverFTDS(): Option[Dataset[Row]] = {
    if (!isFTReady) {
      logInfo(s"No available FLAT-TABLE segment $segmentId")
      return None
    }
    // +----------+---+---+---+---+-----------+-----------+
    // |         0|  2|  3|  4|  1|2_KE_ENCODE|4_KE_ENCODE|
    // +----------+---+---+---+---+-----------+-----------+
    val tableDS = sparkSession.read.parquet(flatTablePath.toString)
    // ([2_KE_ENCODE,4_KE_ENCODE], [0,1,2,3,4])
    val (coarseEncodes, noneEncodes) = tableDS.schema.map(sf => sf.name).partition(_.endsWith(ENCODE_SUFFIX))
    val encodes = coarseEncodes.map(_.stripSuffix(ENCODE_SUFFIX))
    val nones = tableDesc.getColumnIds.asScala //
      .sorted
      .map(String.valueOf) //
      .filterNot(noneEncodes.contains) ++
      // [xx_KE_ENCODE]
      tableDesc.getMeasures.asScala //
        .map(DictionaryBuilderHelper.needGlobalDict) //
        .filter(Objects.nonNull) //
        .map(colRef => dataModel.getColumnIdByColumnName(colRef.getIdentity)) //
        .map(String.valueOf) //
        .filterNot(encodes.contains)
        .map(id => id + ENCODE_SUFFIX)

    if (nones.nonEmpty) {
      // The previous flat table missed some columns.
      // Flat table would be updated at afterwards step.
      logInfo(s"Update FLAT-TABLE columns should have been included " + //
        s"${nones.mkString("[", ",", "]")} segment $segmentId")
      return None
    }
    // The previous flat table could be reusable.
    logInfo(s"Skip FLAT-TABLE segment $segmentId")
    Some(tableDS)
  }

  private def newTableDS(tableRef: TableRef): Dataset[Row] = {
    // By design, why not try recovering from table snapshot.
    // If fact table is a view and its snapshot exists, that will benefit.
    logInfo(s"Load source table ${tableRef.getTableIdentity}")
    sparkSession.table(tableRef.getTableDesc).alias(tableRef.getAlias)
  }

  protected final def gatherStatistics(tableDS: Dataset[Row]): Statistics = {
    val totalRowCount = tableDS.count()
    if (!shouldPersistFT) {
      // By design, evaluating column bytes should be based on existed flat table.
      logInfo(s"Flat table not persisted, only compute row count.")
      return Statistics(totalRowCount, Map.empty[String, Long])
    }
    // zipWithIndex before filter
    val canonicalIndices = tableDS.columns //
      .zipWithIndex //
      .filterNot(_._1.endsWith(ENCODE_SUFFIX)) //
      .map { case (name, index) =>
        val canonical = tableDesc.getCanonicalName(Integer.parseInt(name))
        (canonical, index)
      }.filterNot(t => Objects.isNull(t._1))
    logInfo(s"CANONICAL INDICES ${canonicalIndices.mkString("[", ", ", "]")}")
    // By design, action-take is not sampling.
    val sampled = tableDS.take(sampleRowCount).flatMap(row => //
      canonicalIndices.map { case (canonical, index) => //
        val bytes = utf8Length(row.get(index))
        (canonical, bytes) //
      }).groupBy(_._1).mapValues(_.map(_._2).sum)
    val evaluated = evaluateColumnBytes(totalRowCount, sampled)
    Statistics(totalRowCount, evaluated)
  }

  private def evaluateColumnBytes(totalCount: Long, //
                                  sampled: Map[String, Long]): Map[String, Long] = {
    val multiple = if (totalCount < sampleRowCount) 1f else totalCount.toFloat / sampleRowCount
    sampled.mapValues(bytes => (bytes * multiple).toLong)
  }

  // Copied from DFChooser.
  private def utf8Length(value: Any): Long = {
    if (Objects.isNull(value)) {
      return 0L
    }
    var i = 0
    var bytes = 0L
    val sequence = value.toString
    while (i < sequence.length) {
      val c = sequence.charAt(i)
      if (c <= 0x7F) bytes += 1
      else if (c <= 0x7FF) bytes += 2
      else if (Character.isHighSurrogate(c)) {
        bytes += 4
        i += 1
      }
      else bytes += 3
      i += 1
    }
    bytes
  }

  // ====================================== Dividing line, till the bottom. ====================================== //
  // Historical debt.
  // Waiting for reconstruction.

  private def encodeWithCols(ds: Dataset[Row],
                             ccCols: Set[TblColRef],
                             dictCols: Set[TblColRef],
                             encodeCols: Set[TblColRef]): Dataset[Row] = {
    val ccDataset = withColumn(ds, ccCols)
    if (isGDReady) {
      logInfo(s"Skip DICTIONARY segment $segmentId")
    } else {
      buildDict(ccDataset, dictCols)
    }
    encodeColumn(ccDataset, encodeCols)
  }

  private def withColumn(ds: Dataset[Row], withCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = filterCols(ds, withCols)
    var withDs = ds
    matchedCols.foreach(m => withDs = withDs.withColumn(convertFromDot(m.getIdentity),
      expr(convertFromDot(m.getExpressionInSourceDB))))
    withDs
  }

  private def buildDict(ds: Dataset[Row], dictCols: Set[TblColRef]): Unit = {
    val matchedCols = if (dataSegment.getIndexPlan.isSkipEncodeIntegerFamilyEnabled) {
      filterOutIntegerFamilyType(ds, dictCols)
    } else {
      filterCols(ds, dictCols)
    }
    val builder = new DFDictionaryBuilder(ds, dataSegment, sparkSession, Sets.newHashSet(matchedCols.asJavaCollection))
    builder.buildDictSet()
  }

  private def encodeColumn(ds: Dataset[Row], encodeCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = filterCols(ds, encodeCols)
    var encodeDs = ds
    if (matchedCols.nonEmpty) {
      encodeDs = DFTableEncoder.encodeTable(ds, dataSegment, matchedCols.asJava)
    }
    encodeDs
  }

  // For lookup table, CC column may be duplicate of the flat table when it doesn't belong to one specific table like '1+2'
  private def cleanComputedColumns(cc: Set[TblColRef], flatCols: Set[String]): Set[TblColRef] = {
    var cleanCols = cc
    if (flatCols != null) {
      cleanCols = cc.filter(col => !flatCols.contains(convertFromDot(col.getIdentity)))
    }
    cleanCols
  }
}

object SegmentFlatTable extends LogEx {

  import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._

  private val conf = KylinConfig.getInstanceFromEnv
  var inferFiltersEnabled: Boolean = conf.inferFiltersEnabled()

  def fulfillDS(originDS: Dataset[Row], cols: Set[TblColRef], tableRef: TableRef): Dataset[Row] = {
    // wrap computed columns, filter out valid columns
    val computedColumns = chooseSuitableCols(originDS, cols)
    // wrap alias
    val newDS = wrapAlias(originDS, tableRef.getAlias)
    val selectedColumns = newDS.schema.fields.map(tp => col(tp.name)) ++ computedColumns
    logInfo(s"Table SCHEMA ${tableRef.getTableIdentity} ${newDS.schema.treeString}")
    newDS.select(selectedColumns: _*)
  }

  private def wrapAlias(originDS: Dataset[Row], alias: String): Dataset[Row] = {
    val newFields = originDS.schema.fields.map(f => convertFromDot(alias + "." + f.name)).toSeq
    val newDS = originDS.toDF(newFields: _*)
    logInfo(s"Wrap ALIAS ${originDS.schema.treeString} TO ${newDS.schema.treeString}")
    newDS
  }


  def joinFactTableWithLookupTables(rootFactDataset: Dataset[Row],
                                    lookupTableDatasetMap: mutable.Map[JoinTableDesc, Dataset[Row]],
                                    model: NDataModel,
                                    ss: SparkSession): Dataset[Row] = {
    lookupTableDatasetMap.foldLeft(rootFactDataset)(
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
      val joinType = join.getType.toUpperCase(Locale.ROOT)
      val pk = join.getPrimaryKeyColumns
      val fk = join.getForeignKeyColumns
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$lookupDesc, pk: ${pk.mkString(",")}")
      }
      val equiConditionColPairs = fk.zip(pk).map(joinKey =>
        col(convertFromDot(joinKey._1.getIdentity))
          .equalTo(col(convertFromDot(joinKey._2.getIdentity))))
      logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")

      if (join.getNonEquiJoinCondition != null) {
        var condition = NonEquiJoinConditionBuilder.convert(join.getNonEquiJoinCondition)
        if (!equiConditionColPairs.isEmpty) {
          condition = condition && equiConditionColPairs.reduce(_ && _)
        }
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, non-equi condition: ${condition.toString()}")
        afterJoin = afterJoin.join(lookupDataset, condition, joinType)
      } else {
        val condition = equiConditionColPairs.reduce(_ && _)
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
        if (inferFiltersEnabled) {
          afterJoin = afterJoin.join(FiltersUtil.inferFilters(pk, lookupDataset), condition, joinType)
        } else {
          afterJoin = afterJoin.join(lookupDataset, condition, joinType)
        }
      }
    }
    afterJoin
  }

  def changeSchemeToColumnId(ds: Dataset[Row], tableDesc: SegmentFlatTableDesc): Dataset[Row] = {
    val structType = ds.schema
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns
      .asScala
      .map(column => convertFromDot(column.getIdentity))
      .zip(columnIds)
    val columnName2IdMap = columnName2Id.toMap
    val encodeSeq = structType.filter(_.name.endsWith(ENCODE_SUFFIX)).map {
      tp =>
        val columnName = tp.name.stripSuffix(ENCODE_SUFFIX)
        val columnId = columnName2IdMap.apply(columnName)
        col(tp.name).alias(columnId.toString + ENCODE_SUFFIX)
    }
    val columns = columnName2Id.map(tp => expr(tp._1).alias(tp._2.toString))
    logInfo(s"Select model column is ${columns.mkString(",")}")
    logInfo(s"Select model encoding column is ${encodeSeq.mkString(",")}")
    val selectedColumns = columns ++ encodeSeq

    logInfo(s"Select model all column is ${selectedColumns.mkString(",")}")
    ds.select(selectedColumns: _*)
  }

  private def generateDimensionTableMeta(lookupTables: mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]): Unit = {
    val lookupTablePar = lookupTables.par
    lookupTablePar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(lookupTablePar.size))
    lookupTablePar.foreach { case (joinTableDesc, dataset) =>
      val tableIdentity = joinTableDesc.getTable
      logTime(s"count $tableIdentity") {
        val rowCount = dataset.count()
        TableMetaManager.putTableMeta(tableIdentity, 0L, rowCount)
        logInfo(s"put meta table: $tableIdentity , count: $rowCount")
      }
    }
  }

  def replaceDot(original: String, model: NDataModel): String = {
    val sb = new StringBuilder(original)

    for (namedColumn <- model.getAllNamedColumns.asScala) {
      val colName = namedColumn.getAliasDotColumn.toLowerCase(Locale.ROOT)
      doReplaceDot(sb, colName, namedColumn.getAliasDotColumn)

      // try replacing quoted identifiers if any
      val quotedColName = colName.split('.').mkString("`", "`.`", "`");
      if (quotedColName.nonEmpty) {
        doReplaceDot(sb, quotedColName, namedColumn.getAliasDotColumn)
      }
    }
    sb.toString()
  }

  private def doReplaceDot(sb: StringBuilder, namedCol: String, colAliasDotColumn: String): Unit = {
    var start = sb.toString.toLowerCase(Locale.ROOT).indexOf(namedCol)
    while (start != -1) {
      sb.replace(start,
        start + namedCol.length,
        convertFromDot(colAliasDotColumn))
      start = sb.toString.toLowerCase(Locale.ROOT)
        .indexOf(namedCol)
    }
  }

  case class Statistics(totalCount: Long, columnBytes: Map[String, Long])

}
