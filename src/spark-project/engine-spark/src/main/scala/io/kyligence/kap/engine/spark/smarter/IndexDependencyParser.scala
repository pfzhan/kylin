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
package io.kyligence.kap.engine.spark.smarter

import com.google.common.collect.{Lists, Maps, Sets}
import io.kyligence.kap.engine.spark.builder.SegmentFlatTable
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.metadata.cube.model.LayoutEntity
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.metadata.model.{FunctionDesc, JoinTableDesc, TableRef, TblColRef}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, Row, SparderEnv, SparkSession}

import java.util
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.mutable

class IndexDependencyParser(val model: NDataModel) {

  private val ccTableNameAliasMap = Maps.newHashMap[String, util.Set[String]]
  private val joinTableAliasMap = Maps.newHashMap[String, util.Set[String]]
  private val allTablesAlias = Sets.newHashSet[String]
  initTableNames()

  def getRelatedTablesAlias(layouts: util.Collection[LayoutEntity]): util.List[String] = {
    val relatedTables = Sets.newHashSet[String]
    layouts.asScala.foreach(layout => relatedTables.addAll(getRelatedTablesAlias(layout)))
    val relatedTableList: util.List[String] = Lists.newArrayList(relatedTables)
    Collections.sort(relatedTableList)
    relatedTableList
  }

  def getRelatedTables(layoutEntity: LayoutEntity): util.List[String] = {
    val relatedTablesAlias = getRelatedTablesAlias(layoutEntity)
    val relatedTables = relatedTablesAlias.asScala
      .map(alias => model.getAliasMap.get(alias).getTableIdentity)
      .toSet

    val relatedTableList: util.List[String] = Lists.newArrayList(relatedTables.asJava)
    Collections.sort(relatedTableList)
    relatedTableList
  }

  def getRelatedTablesAlias(layoutEntity: LayoutEntity): util.Set[String] = {
    val relatedTablesAlias: util.Set[String] = Sets.newHashSet(allTablesAlias)
    layoutEntity.getColOrder.asScala.foreach((id: Integer) => {
      if (id < NDataModel.MEASURE_ID_BASE) {
        val ref = model.getEffectiveCols.get(id)
        val tablesFromColumn = getTableIdentitiesFromColumn(ref)
        relatedTablesAlias.addAll(tablesFromColumn)
      } else if (isValidMeasure(id)) {
        val tablesFromMeasure = model.getEffectiveMeasures.get(id) //
          .getFunction.getParameters.asScala //
          .filter(_.getType == FunctionDesc.PARAMETER_TYPE_COLUMN) //
          .map(_.getColRef) //
          .flatMap(getTableIdentitiesFromColumn(_).asScala) //
          .toSet.asJava
        relatedTablesAlias.addAll(tablesFromMeasure)
      }
    })
    val joinSet: util.Set[String] = Sets.newHashSet()
    relatedTablesAlias.asScala.foreach(tableName => {
      val prSets = joinTableAliasMap.get(tableName)
      if (prSets != null) {
        joinSet.addAll(prSets)
      }
    })
    relatedTablesAlias.addAll(joinSet)
    relatedTablesAlias
  }

  private def isValidMeasure(id: Integer): Boolean = {
    !(model.getEffectiveMeasures == null ||
      model.getEffectiveMeasures.get(id) == null ||
      model.getEffectiveMeasures.get(id).getFunction == null ||
      model.getEffectiveMeasures.get(id).getFunction.getParameters == null)
  }

  private def getTableIdentitiesFromColumn(ref: TblColRef) = {
    val desc = ref.getColumnDesc
    if (desc.isComputedColumn) {
      Sets.newHashSet(ccTableNameAliasMap.get(ref.getName))
    } else {
      Sets.newHashSet(ref.getTableAlias)
    }
  }

  private def generateFullFlatTableDF(ss: SparkSession, model: NDataModel): Dataset[Row] = {
    val rootDF = generateDatasetOnTable(ss, model.getRootFactTable)
    // look up tables
    val joinTableDFMap = mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]()
    model.getJoinTables.asScala.map((joinTable: JoinTableDesc) => {
      joinTableDFMap.put(joinTable, generateDatasetOnTable(ss, joinTable.getTableRef))
    })
    val df = SegmentFlatTable.joinFactTableWithLookupTables(rootDF, joinTableDFMap, model, ss)
    if (StringUtils.isNotEmpty(model.getFilterCondition)) {
      df.where(NSparkCubingUtil.convertFromDot(model.getFilterCondition))
    }
    df
  }

  private def generateDatasetOnTable(ss: SparkSession, tableRef: TableRef): Dataset[Row] = {
    val tableCols = tableRef.getColumns.asScala.map(_.getColumnDesc).filter(!_.isComputedColumn).toArray
    val structType = SchemaProcessor.buildSchemaWithRawTable(tableCols)
    val alias = tableRef.getAlias
    val dataset = ss.createDataFrame(Lists.newArrayList[Row], structType).alias(alias)
    SegmentFlatTable.wrapAlias(dataset, alias)
  }

  private def initTableNames(): Unit = {
    val ccList = model.getComputedColumnDescs
    val originDf = generateFullFlatTableDF(SparderEnv.getSparkSession, model)
    val colFields = originDf.schema.fields
    val ds = originDf.selectExpr(ccList.asScala.map(_.getInnerExpression).map(NSparkCubingUtil.convertFromDot): _*)
    ccList.asScala.zip(ds.schema.fields).foreach(pair => {
      val ccFieldName = pair._2.name
      colFields.foreach(col => {
        if (ccFieldName.contains(col.name)) {
          val tableName = col.name.substring(0, col.name.indexOf(NSparkCubingUtil.SEPARATOR))
          val tableSet = ccTableNameAliasMap.getOrDefault(pair._1.getColumnName, Sets.newHashSet[String])
          tableSet.add(model.getTableNameMap.get(tableName).getAlias)
          ccTableNameAliasMap.put(pair._1.getColumnName, tableSet)
        }
      })
    })

    initFilterConditionTableNames(originDf, colFields)
    initPartitionColumnTableNames()
    initJoinTableName()
    allTablesAlias.add(model.getRootFactTable.getAlias)
  }


  def unwrapComputeColumn(ccInnerExpression: String): java.util.Set[TblColRef] = {
    val result: util.Set[TblColRef] = Sets.newHashSet()
    val originDf = generateFullFlatTableDF(SparderEnv.getSparkSession, model)
    val colFields = originDf.schema.fields
    val ccDs = originDf.selectExpr(NSparkCubingUtil.convertFromDot(ccInnerExpression))
    ccDs.schema.fields.foreach(fieldName => {
      colFields.foreach(col => {
        if (StringUtils.containsIgnoreCase(fieldName.name, col.name)) {
          val tableAndCol = col.name.split(NSparkCubingUtil.SEPARATOR)
          val ref = model.findColumn(tableAndCol(0), tableAndCol(1))
          if (ref != null) {
            result.add(ref)
          }
        }
      })
    })
    result
  }

  private def initFilterConditionTableNames(originDf: Dataset[Row], colFields: Array[StructField]): Unit =
    if (StringUtils.isNotEmpty(model.getFilterCondition)) {
      val whereDs = originDf.selectExpr(NSparkCubingUtil.convertFromDot(model.getFilterCondition))
      whereDs.schema.fields.foreach(whereField => {
        colFields.foreach(colField => {
          if (whereField.name.contains(colField.name)) {
            val tableName = colField.name.substring(0, colField.name.indexOf(NSparkCubingUtil.SEPARATOR))
            allTablesAlias.add(model.getTableNameMap.get(tableName).getAlias)
          }
        })
      })
    }

  private def initPartitionColumnTableNames(): Unit = {
    if (model.getPartitionDesc != null && model.getPartitionDesc.getPartitionDateColumnRef != null) {
      allTablesAlias.addAll(getTableIdentitiesFromColumn(model.getPartitionDesc.getPartitionDateColumnRef))
    }
  }

  private def initJoinTableName() {
    if (CollectionUtils.isEmpty(model.getJoinTables)) {
      return
    }
    val pkTableToFkTableAliasMap = Maps.newHashMap[String, String]
    model.getJoinTables.asScala.foreach(joinTable => {
      if (joinTable.getJoin.getPKSide != null && joinTable.getJoin.getFKSide != null) {
        val pkTableAlias = joinTable.getJoin.getPKSide.getAlias
        val fkTableAlias = joinTable.getJoin.getFKSide.getAlias
        pkTableToFkTableAliasMap.put(pkTableAlias, fkTableAlias)
      }
    })

    pkTableToFkTableAliasMap.asScala.foreach(tableAliasPair => {
      val pkTableAlias = tableAliasPair._1
      var fkTableAlias = tableAliasPair._2
      val dependencyTableSet = joinTableAliasMap.getOrDefault(pkTableAlias, Sets.newHashSet[String])
      while (fkTableAlias != null) {
        dependencyTableSet.add(fkTableAlias)
        fkTableAlias = pkTableToFkTableAliasMap.get(fkTableAlias)
      }
      joinTableAliasMap.putIfAbsent(pkTableAlias, dependencyTableSet)
    })
  }
}