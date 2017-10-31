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

import org.apache.kylin.cube.CubeSegment
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.cube.model.CubeDesc
import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.metadata.TableMetadataManager
import org.apache.kylin.metadata.model.DeriveInfo
import org.apache.kylin.metadata.model.DeriveInfo.DeriveType
import org.apache.kylin.metadata.model.{FunctionDesc, JoinDesc, TblColRef}
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.kylin.storage.gtrecord.CubeTupleConverter
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._

class SparderTupleConverter(gtInfoTableName: String,
                            cubeSeg: CubeSegment,
                            cuboid: Cuboid,
                            selectedDimensions: util.Set[TblColRef],
                            selectedMetrics: util.Set[FunctionDesc],
                            gtColIdx: Array[Int],
                            returnTupleInfo: TupleInfo)
    extends CubeTupleConverter(
      cubeSeg: CubeSegment,
      cuboid: Cuboid,
      selectedDimensions: util.Set[TblColRef],
      selectedMetrics: util.Set[FunctionDesc],
      gtColIdx: Array[Int],
      returnTupleInfo: TupleInfo
    ) {

  val hostToDerivedInfo: util.Map[org.apache.kylin.common.util.Array[TblColRef],
                                  util.List[DeriveInfo]] =
    cuboid.getCubeDesc.getHostToDerivedInfo(cuboid.getColumns, null)
  var hasDerived = false
  var hostToDeriveds: List[HostToDerived] = List.empty

  //  make hostToDerivedInfo with two keys prior to one keys
  //  so that joinDerived will choose LOOKUP prior to PK_FK derive
  val sortedHostToDerivedInfo = hostToDerivedInfo.asScala.toList
    .flatMap(pair => {
      pair._2.asScala.map(di => (pair._1, di))
    })
    .sortBy(p => p._1.data.length)
    .reverse
  for (entry <- sortedHostToDerivedInfo) {
    findDerivedColumn(entry._1.data, entry._2)
  }

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
        //  if (deriveInfo.join.getPrimaryKeyColumns.contains(hostCols.apply(0))) {
        // PK -> FK, rare
        throw new IllegalStateException(
          "PK -> FK derive is not supported in sparder")
      }

    } else {
      hostFkCols = hostCols
    }

    val hostFkIdx = hostFkCols.map(hostCol => indexOnTheGTValues(hostCol))
    if (hostFkIdx.exists(_ < 0)) {
      return
    }

    val hostIndex = hostCols.map(hostCol => indexOnTheGTValues(hostCol))
    if (hostIndex.exists(_ < 0)) {
      return
    }

    val calciteIdx = deriveInfo.columns
      .map(column =>
        if (returnTupleInfo.hasColumn(column)) {
          returnTupleInfo.getColumnIndex(column)
        } else { -1 }
      )
      .filter(_ >= 0)

    if (calciteIdx.isEmpty) {
      return
    }

    val (path, tableName, aliasTableName, pkIndex) =
      getLookupTablePathAndPkIndex(cubeSeg, deriveInfo)

    val lookupIdx = deriveInfo.columns.map(_.getColumnDesc.getZeroBasedIndex)
    hostToDeriveds ++= List(
      HostToDerived(hostIndex,
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

  def getLookupTablePathAndPkIndex(
      cubeSegment: CubeSegment,
      deriveInfo: DeriveInfo): (String, String, String, Array[Int]) = {
    val join = deriveInfo.join
    val metaMgr = TableMetadataManager.getInstance(cubeSeg.getCubeInstance.getConfig)
    val derivedTableName = join.getPKSide.getTableIdentity
    val pkCols = join.getPrimaryKey
    val tableDesc =
      metaMgr.getTableDesc(derivedTableName, cubeSegment.getProject)
    val pkIndex =
      pkCols.map(pkCol => tableDesc.findColumnByName(pkCol).getZeroBasedIndex)
    val path = cubeSegment.getSnapshotResPath(derivedTableName)
    if (path == null && deriveInfo.`type` != DeriveType.PK_FK) {
      throw new IllegalStateException(
        "No snapshot for table '" + derivedTableName + "' found on cube segment"
          + cubeSegment.getCubeInstance.getName + "/" + cubeSegment)
    }
    (path,
     cubeSegment.getProject + "@" + derivedTableName,
     s"${gtInfoTableName}_${join.getPKSide.getAlias}",
     pkIndex)
  }

  override def indexOnTheGTValues(col: TblColRef): Int = {
    val cuboidDims = cuboid.getColumns
    val cuboidIdx = cuboidDims.indexOf(col)
    if (gtColIdx.contains(cuboidIdx)) {
      cuboidIdx
    } else {
      -1
    }
  }

  def joinDerived(gTInfo: GTInfo, dataFrame: DataFrame): DataFrame = {
    val gTInfoNames = dataFrame.schema.fieldNames

    //        val joinKeyZips = hostToDeriveds.reduce()
    def joinLookUpTable(df: DataFrame,
                        hostToDerived: HostToDerived): DataFrame = {
      val lookupTableAlias = hostToDerived.aliasTableName

      val lookupDf = SparderLookupManager.getOrCreate(
        hostToDerived.tableIdentity,
        hostToDerived.path,
        cubeSeg.getConfig)
      val newNames = lookupDf.schema.fieldNames.map { name =>
        val gTInfoSchema = SchemaProcessor.parseDeriveTableSchemaName(name)
        SchemaProcessor.generateDeriveTableSchema(lookupTableAlias,
                                                  gTInfoSchema._3)
      }.array
      val newNameLookupDf = lookupDf.toDF(newNames: _*)
      if (hostToDerived.fkIdx.length != hostToDerived.pkIdx.length) {
        throw new IllegalStateException(
          s"unequal host key num ${hostToDerived.fkIdx.length} " +
            s"vs derive pk num ${hostToDerived.pkIdx.length} ")
      }
      val zipIndex = hostToDerived.fkIdx.zip(hostToDerived.pkIdx)
      var joinCol: Column = null
      zipIndex.foreach {
        case (hostIndex, pkIndex) =>
          val dataType = gTInfo.getColumnType(hostIndex)
          if (joinCol == null) {
            joinCol = col(gTInfoNames.apply(hostIndex))
              .equalTo(
                col(SchemaProcessor.generateDeriveTableSchema(lookupTableAlias,
                                                              pkIndex)))
          } else {
            joinCol = joinCol.and(
              col(gTInfoNames.apply(hostIndex))
                .equalTo(col(SchemaProcessor
                  .generateDeriveTableSchema(lookupTableAlias, pkIndex))))
          }
      }
      df.join(newNameLookupDf, joinCol, hostToDerived.join.getType)
    }

    var joinedDf: DataFrame = dataFrame
    val joinedLookups = scala.collection.mutable.Set[String]()

    for (hostToDerived <- hostToDeriveds) {
      if (hostToDerived.deriveType != DeriveType.PK_FK) {
        //  PK_FK derive does not need joining
        if (!joinedLookups.contains(hostToDerived.aliasTableName)) {
          joinedDf = joinLookUpTable(joinedDf, hostToDerived)
          joinedLookups.add(hostToDerived.aliasTableName)
        }
      }
    }

    joinedDf
  }

  def getGTIndex(): Array[Int] = {
    gtColIdx
  }

  override protected def newDerivedColumnFiller(hostCols: Array[TblColRef],
                                                deriveInfo: DeriveInfo)
    : CubeTupleConverter.IDerivedColumnFiller = {
    null
  }

  //  hostIdx and fkIdx may differ at composite key cases
  //  fkIdx respects to cuboid gt table, rather than origin fact table!
  //  hostFkIdx <-> pkIndex as join relationship
  //  calciteIdx <-> lookupIdx
  case class HostToDerived(hostIdx: Array[Int],
                           fkIdx: Array[Int],
                           pkIdx: Array[Int],
                           calciteIdx: Array[Int],
                           lookupIdx: Array[Int],
                           path: String,
                           tableIdentity: String,
                           aliasTableName: String,
                           join: JoinDesc,
                           deriveType: DeriveType) {
    require(fkIdx.length == pkIdx.length)
    require(calciteIdx.length == lookupIdx.length)
  }

}
