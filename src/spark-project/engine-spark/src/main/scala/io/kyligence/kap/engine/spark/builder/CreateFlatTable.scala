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

import com.google.common.collect.Sets
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc
import io.kyligence.kap.metadata.model.{NDataModel, NDataModelFlatTableDesc}
import org.apache.commons.lang.StringUtils
import org.apache.kylin.metadata.model._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object CreateFlatTable extends Logging {
  def generateDataset(model: NDataModel,
                      ss: SparkSession): Dataset[Row] = {
    val rootFactDesc = model.getRootFactTable.getTableDesc
    var ds = ss.table(rootFactDesc).alias(model.getRootFactTable.getAlias)

    ds = changeSchemaToAliasDotName(ds, model.getRootFactTable.getAlias)
    for (lookupDesc <- model.getJoinTables.asScala) {
      val join = lookupDesc.getJoin
      if (join != null && !StringUtils.isEmpty(join.getType)) {
        val joinType = join.getType.toUpperCase
        val dimTable = lookupDesc.getTableRef
        var lookupTable = ss.table(dimTable.getTableDesc)
          .alias(dimTable.getAlias)
        lookupTable = changeSchemaToAliasDotName(lookupTable, dimTable.getAlias)
        val pk = join.getPrimaryKeyColumns
        val fk = join.getForeignKeyColumns
        if (pk.length != fk.length) {
          throw new RuntimeException(
            "Invalid join condition of lookup table:" + lookupDesc)
        }
        val condition = fk.zip(pk).map(joinKey =>
          col(NSparkCubingUtil.convertFromDot(joinKey._1.getIdentity))
            .equalTo(col(NSparkCubingUtil.convertFromDot(joinKey._2.getIdentity))))
          .reduce(_.and(_))
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
        ds = ds.join(lookupTable, condition, joinType)
      }
    }
    ds
  }

  def generateDataset(flatTable: IJoinedFlatTableDesc,
                      ss: SparkSession): Dataset[Row] = {
    val model = flatTable.getDataModel
    var ds = generateDataset(model, ss)

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      val afterConvertCondition = replaceDot(model.getFilterCondition, model)
      ds = ds.where(afterConvertCondition)
    }
    val partDesc = model.getPartitionDesc
    if (partDesc != null && partDesc.getPartitionDateColumn != null) {
      @SuppressWarnings(Array("rawtypes"))
      val segRange = flatTable.getSegRange
      if (segRange != null && !segRange.isInfinite) {
        val afterConvertPartition = replaceDot(
          partDesc.getPartitionConditionBuilder
            .buildDateRangeCondition(partDesc, null, segRange),
          model)
        logInfo(s"Partition filter $afterConvertPartition")
        ds = ds.where(afterConvertPartition) // TODO: mp not supported right now
      }
    }
    flatTable match {
      case joined: NCubeJoinedFlatTableDesc =>
        selectNCubeJoinedFlatTable(ds, joined)
      case model: NDataModelFlatTableDesc =>
        selectNModelJoinedFlatTable(ds, model)
    }
  }

  /*
     * Convert IJoinedFlatTableDesc to Dataset
     */
  def selectNModelJoinedFlatTable(ds: Dataset[Row], flatTable: NDataModelFlatTableDesc): Dataset[Row] = {
    val colRefs = flatTable.getAllColumns
    val exprs = new Array[String](colRefs.size)
    val names = new Array[String](exprs.length)
    for (i <- exprs.indices) {
      exprs(i) = NSparkCubingUtil.convertFromDot(colRefs.get(i).getExpressionInSourceDB)
      names(i) = NSparkCubingUtil.convertFromDot(colRefs.get(i).getIdentity)
    }
    ds.selectExpr(exprs: _*).toDF(names: _*)
  }

  def selectNCubeJoinedFlatTable(ds: Dataset[Row], flatTable: NCubeJoinedFlatTableDesc): Dataset[Row] = {
    val colRefs = flatTable.getAllColumns
    val colIndices = flatTable.getIndices
    val exprs = new Array[String](colRefs.size)
    val indices = new Array[String](exprs.length)
    for (i <- exprs.indices) {
      exprs(i) = NSparkCubingUtil.convertFromDot(colRefs.get(i).getExpressionInSourceDB)
      indices(i) = String.valueOf(colIndices.get(i))
    }
    ds.selectExpr(exprs: _*).toDF(indices: _*)
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

  def changeSchemaToAliasDotName(original: Dataset[Row],
                                 alias: String): Dataset[Row] = {
    val sf = original.schema.fields
    val newSchema = sf
      .map(field => NSparkCubingUtil.convertFromDot(alias + "." + field.name))
      .toSeq
    original.toDF(newSchema: _*)
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
    var i: Int = 0
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
              i += 1;
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
      && partDesc.getPartitionDateColumn != null && segRange != null && !(segRange.isInfinite)) {
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
    return col.getTableAlias + "_" + col.getName
  }
}
