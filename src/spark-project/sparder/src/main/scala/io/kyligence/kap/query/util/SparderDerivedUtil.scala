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

package io.kyligence.kap.query.util

import java.util

import com.google.common.collect.Maps
import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate
import io.kyligence.kap.metadata.cube.model.NDataSegment
import io.kyligence.kap.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.model.DeriveInfo.DeriveType
import org.apache.kylin.metadata.model.{DeriveInfo, TblColRef}
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.spark.sql.derived.DerivedInfo
import org.apache.spark.sql.execution.utils.{DeriveTableColumnInfo, SchemaProcessor}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._

// scalastyle:off
case class SparderDerivedUtil(gtInfoTableName: String,
                              dataSeg: NDataSegment,
                              gtColIdx: Array[Int],
                              tupleInfo: TupleInfo,
                              layoutCandidate: NLayoutCandidate) {
  val hostToDerivedInfo: util.Map[org.apache.kylin.common.util.Array[TblColRef],
                                  util.List[DeriveInfo]] =
    layoutCandidate.makeHostToDerivedMap

  var hasDerived = false
  var hostToDeriveds: List[DerivedInfo] = List.empty

  //  make hostToDerivedInfo with two keys prior to one keys
  //  so that joinDerived will choose LOOKUP prior to PK_FK derive
  val sortedHostToDerivedInfo = hostToDerivedInfo.asScala.toList
    .flatMap(pair => {
      pair._2.asScala.map(di => (pair._1, di))
    })
    .sortBy(p => p._1.data.length)
    .reverse
    .foreach(entry => findDerivedColumn(entry._1.data, entry._2))

  val derivedColumnNameMapping: util.HashMap[DerivedInfo, Array[String]] =
    Maps.newHashMap[DerivedInfo, Array[String]]()

  def findDerivedColumn(hostCols: Array[TblColRef],
                        deriveInfo: DeriveInfo): Unit = {

    //  for PK_FK derive on composite join keys, hostCols may be incomplete
    //  see CubeDesc.initDimensionColumns()
    var hostFkCols: Array[TblColRef] = null
    if (deriveInfo.`type` == DeriveType.PK_FK) {
      //  if(false)
      if (deriveInfo.join.getForeignKeyColumns.contains(hostCols.apply(0))) {
        // FK -> PK, most cases
        hostFkCols = deriveInfo.join.getForeignKeyColumns
      } else {
        hostFkCols = deriveInfo.join.getPrimaryKeyColumns
      }
    } else {
      hostFkCols = hostCols
    }

    val hostFkIdx = hostFkCols.map(hostCol => indexOnTheGTValues(hostCol))
    // fix for test src/kap-it/src/test/resources/query/sql_rawtable/query03.sql
    if (!hostFkIdx.exists(_ >= 0)) {
      return
    }

    val hostIndex = hostCols.map(hostCol => indexOnTheGTValues(hostCol))
    if (hostIndex.exists(_ < 0)) {
      return
    }

    val calciteIdx = deriveInfo.columns
      .map(column =>
        if (tupleInfo.hasColumn(column)) {
          tupleInfo.getColumnIndex(column)
        } else {
          -1
      })
      .filter(_ >= 0)

    if (calciteIdx.isEmpty) {
      return
    }

    val (path, tableName, aliasTableName, pkIndex) =
      getLookupTablePathAndPkIndex(deriveInfo)

    val lookupIdx = deriveInfo.columns.map(_.getColumnDesc.getZeroBasedIndex)
    hostToDeriveds ++= List(
      DerivedInfo(hostIndex,
                  hostFkIdx,
                  pkIndex,
                  calciteIdx,
                  lookupIdx,
                  path,
                  tableName,
                  aliasTableName,
                  deriveInfo.join,
                  deriveInfo.`type`))
    hasDerived = true
  }

  def indexOnTheGTValues(col: TblColRef): Int = {
    val cuboidDims = layoutCandidate.getCuboidLayout.getColumns
    val cuboidIdx = cuboidDims.indexOf(col)
    if (gtColIdx.contains(cuboidIdx)) {
      cuboidIdx
    } else {
      -1
    }
  }

  def joinDerived(dataFrame: DataFrame): DataFrame = {
    var joinedDf: DataFrame = dataFrame
    val joinedLookups = scala.collection.mutable.Set[String]()

    for (hostToDerived <- hostToDeriveds) {
      if (hostToDerived.deriveType != DeriveType.PK_FK) {
        //  PK_FK derive does not need joining
        if (!joinedLookups.contains(hostToDerived.aliasTableName)) {
          joinedDf = joinLookUpTable(dataFrame.schema.fieldNames,
                                     joinedDf,
                                     hostToDerived)
          joinedLookups.add(hostToDerived.aliasTableName)
        }
      }
    }

    joinedDf
  }

  def getLookupTablePathAndPkIndex(
      deriveInfo: DeriveInfo): (String, String, String, Array[Int]) = {
    val join = deriveInfo.join
    val metaMgr =
      NTableMetadataManager.getInstance(dataSeg.getConfig, dataSeg.getProject)
    val derivedTableName = join.getPKSide.getTableIdentity
    val pkCols = join.getPrimaryKey
    val tableDesc = metaMgr.getTableDesc(derivedTableName)
    val pkIndex =
      pkCols.map(pkCol => tableDesc.findColumnByName(pkCol).getZeroBasedIndex)
    val path = tableDesc.getLastSnapshotPath
    if (path == null && deriveInfo.`type` != DeriveType.PK_FK) {
      throw new IllegalStateException(
        "No snapshot for table '" + derivedTableName + "' found on cube segment"
          + dataSeg.getIndexPlan.getUuid + "/" + dataSeg)
    }
    (path,
     dataSeg.getProject + "@" + derivedTableName,
     s"${gtInfoTableName}_${join.getPKSide.getAlias}",
     pkIndex)
  }

  def joinLookUpTable(gTInfoNames: Array[String],
                      df: DataFrame,
                      derivedInfo: DerivedInfo): DataFrame = {
    val lookupTableAlias = derivedInfo.aliasTableName

    val lookupDf =
      SparderLookupManager.getOrCreate(derivedInfo.tableIdentity,
                                       derivedInfo.path,
                                       dataSeg.getConfig)

    val newNames = lookupDf.schema.fieldNames
      .map { name =>
        SchemaProcessor.parseDeriveTableSchemaName(name)

      }
      .sortBy(_.columnId)
      .map(
        deriveInfo =>
          DeriveTableColumnInfo(lookupTableAlias,
                                deriveInfo.columnId,
                                deriveInfo.columnName).toString)
      .array
    derivedColumnNameMapping.put(derivedInfo, newNames)
    val newNameLookupDf = lookupDf.toDF(newNames: _*)
    if (derivedInfo.fkIdx.length != derivedInfo.pkIdx.length) {
      throw new IllegalStateException(
        s"unequal host key num ${derivedInfo.fkIdx.length} " +
          s"vs derive pk num ${derivedInfo.pkIdx.length} ")
    }
    val zipIndex = derivedInfo.fkIdx.zip(derivedInfo.pkIdx)
    var joinCol: Column = null
    zipIndex.foreach {
      case (hostIndex, pkIndex) =>
        if (joinCol == null) {
          joinCol = col(gTInfoNames.apply(hostIndex))
            .equalTo(col(newNames(pkIndex)))
        } else {
          joinCol = joinCol.and(
            col(gTInfoNames.apply(hostIndex))
              .equalTo(col(newNames(pkIndex))))
        }
    }
    df.join(newNameLookupDf, joinCol, derivedInfo.join.getType)
  }
}
