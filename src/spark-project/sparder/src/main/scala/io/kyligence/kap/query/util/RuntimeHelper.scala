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

package io.kyligence.kap.query.runtime

import io.kyligence.kap.query.util.SparderDerivedUtil
import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.DeriveInfo.DeriveType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.udf.UdfManager

import scala.collection.JavaConverters._
import scala.collection.mutable

// scalastyle:off
object RuntimeHelper {

  final val literalOne = lit("1")

  def registerSingleByColName(funcName: String, dataType: DataType): String = {
    val name = dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
    UdfManager.register(dataType, funcName)
    name
  }

  def gtSchemaToCalciteSchema(
                               primaryKey: ImmutableBitSet,
                               derivedUtil: SparderDerivedUtil,
                               factTableName: String,
                               allColumns: List[TblColRef],
                               sourceSchema: StructType,
                               gtColIdx: Array[Int],
                               tupleIdx: Array[Int]): Seq[Column] = {
    val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
    val calciteToGTinfo = tupleIdx.zipWithIndex.toMap
    var deriveMap: Map[Int, Column] = Map.empty
    // fixme aron adv
    //    val advanceMapping = cubeTupleConverter.advanceMapping
    if (derivedUtil.hasDerived) {
      deriveMap = derivedUtil.hostToDeriveds.flatMap { hostToDerived =>
        val deriveNames = derivedUtil.derivedColumnNameMapping.get(hostToDerived)
        val columns = mutable.ListBuffer.empty[(Int, Column)]
        val derivedTableName = hostToDerived.aliasTableName
        if (hostToDerived.deriveType.equals(DeriveType.PK_FK)) {
          // composite keys are split, so only copy [0] is enough,
          // see CubeDesc.initDimensionColumns()
          require(hostToDerived.calciteIdx.length == 1)
          require(hostToDerived.hostIdx.length == 1)
          val fkColumnRef = hostToDerived.join.getFKSide.getColumns.asScala.head
          columns.append(
            (
              hostToDerived.calciteIdx.apply(0),
              col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
                .alias(
                  SchemaProcessor
                    .generateDeriveTableSchemaName(
                      derivedTableName,
                      hostToDerived.derivedIndex.apply(0),
                      fkColumnRef.getName)
                    .toString)))
        } else {
          hostToDerived.calciteIdx.zip(hostToDerived.derivedIndex).foreach {
            case (calciteIdx, derivedIndex) =>
              columns.append((calciteIdx, col(deriveNames(derivedIndex))))
          }
        }
        columns
      }.toMap
    }

    allColumns.indices
      .zip(allColumns)
      .map {
        case (index, column) =>
          val columnName = "dummy_" + column.getTableRef.getAlias + "_" + column.getName
          //          if (advanceMapping.contains(index)) {
          //            advanceMapping.apply(index)
          //          } else
          if (calciteToGTinfo.contains(index)) {
            //  primarykey
            if (primaryKey.get(gtColIdx.apply(calciteToGTinfo.apply(index)))) {
              val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
              col(gTInfoNames.apply(gTInfoIndex))
            } else {
              //  measure
              val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
              col(gTInfoNames.apply(gTInfoIndex))
            }
          } else if (deriveMap.contains(index)) {
            deriveMap.apply(index)
          } else {
            literalOne.as(s"${factTableName}_${columnName}")
          }
      }
  }

  /*  def prepareCubeScanRequest(cubeSegment: CubeSegment, request: GTCubeStorageQueryRequest): Option[GTScanRequest] = {

      var filter = request.getFilter
      val serialize =
        TupleFilterSerializer.serialize(filter, StringCodeSystem.INSTANCE)
      filter = TupleFilterSerializer.deserialize(serialize, StringCodeSystem.INSTANCE)
      val translator: ITupleFilterTransformer = new BuiltInFunctionTransformer(
        cubeSegment.getDimensionEncodingMap)
      translator.transform(filter)
      var scanRangePlanner: CubeScanRangePlanner = null
      scanRangePlanner = new CubeScanRangePlanner(
        cubeSegment,
        request.getCuboid,
        filter,
        request.getDimensions,
        request.getGroups,
        request.getMetrics,
        request.getHavingFilter,
        request.getContext)

      val scanRequest = scanRangePlanner.planScanRequest()
      Some(scanRequest)
    }

    def gtSchemaToCalciteSchema(
                                     rel: OLAPRel,
                                     cubeTupleConverter: SparderTupleConverter,
                                     gtinfo: GTInfo,
                                     sourceSchema: StructType): Seq[Column] = {
      val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
      val factTableName = rel.getContext.firstTableScan.getBackupAlias
      val calciteToGTinfo = cubeTupleConverter.tupleIdx.zipWithIndex.toMap
      val gtColIdx = cubeTupleConverter.getGTIndex()
      val allColumns = rel.getColumnRowType.getAllColumns.asScala
      var deriveMap: Map[Int, Column] = Map.empty
      val advanceMapping = cubeTupleConverter.advanceMapping
      if (cubeTupleConverter.hasDerived) {
        deriveMap = cubeTupleConverter.hostToDeriveds.flatMap { hostToDerived =>
          val deriveNames = cubeTupleConverter.derivedColumnNameMapping.get(hostToDerived)
          val columns = mutable.ListBuffer.empty[(Int, Column)]
          val derivedTableName = hostToDerived.aliasTableName
          if (hostToDerived.deriveType.equals(DeriveType.PK_FK)) {
            // composite keys are split, so only copy [0] is enough,
            // see CubeDesc.initDimensionColumns()
            require(hostToDerived.calciteIdx.length == 1)
            require(hostToDerived.hostIdx.length == 1)
            val fkColumnRef = hostToDerived.join.getFKSide.getColumns.asScala.head
            columns.append(
              (
                hostToDerived.calciteIdx.apply(0),
                col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
                  .alias(
                    SchemaProcessor
                      .generateDeriveTableSchemaName(
                        derivedTableName,
                        hostToDerived.derivedIndex.apply(0),
                        fkColumnRef.getName)
                      .toString)))
          } else {
            hostToDerived.calciteIdx.zip(hostToDerived.derivedIndex).foreach {
              case (calciteIdx, derivedIndex) =>
                columns.append((calciteIdx, col(deriveNames(derivedIndex))))
            }
          }
          columns
        }.toMap
      }

      allColumns.indices
        .zip(allColumns)
        .map {
          case (index, column) =>
            val columnName = "dummy_" + column.getTableRef.getAlias + "_" + column.getName
            if (advanceMapping.contains(index)) {
              advanceMapping.apply(index)
            } else if (calciteToGTinfo.contains(index)) {
              //  primarykey
              if (gtinfo.getPrimaryKey.get(gtColIdx.apply(calciteToGTinfo.apply(index)))) {
                val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
                col(gTInfoNames.apply(gTInfoIndex))
              } else {
                //  measure
                val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
                col(gTInfoNames.apply(gTInfoIndex))
              }
            } else if (deriveMap.contains(index)) {
              deriveMap.apply(index)
            } else {
              literalOne.as(s"${factTableName}_${columnName}")
            }
        }
    }

    // only for raw table
    def gtInfoSchemaToCalciteSchema(
                                     rel: OLAPRel,
                                     tupleConverter: RawTableTupleConverter,
                                     gtinfo: GTInfo,
                                     sourceSchema: StructType): Seq[Column] = {
      val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
      val factTableName = rel.getContext.firstTableScan.getBackupAlias

      val columnMapping =
        tupleConverter.tupleIdx.zip(tupleConverter.gtColIdx).toMap
      val allColumns = rel.getColumnRowType.getAllColumns.asScala
      allColumns.indices
        .zip(allColumns)
        .map {
          case (index, column) =>
            if (columnMapping.contains(index)) {
              col(gTInfoNames.apply(columnMapping.apply(index)))
            } else {
              val columnName = "dummy_" + column.getTableRef.getAlias + "_" + column.getName
              literalOne.as(s"${factTableName}_${columnName}")
            }
        }
    }

    def buildDummyGTInfoSchema(
                                gTScanRequest: GTScanRequest,
                                columnMapping: Array[String],
                                dataFrame: DataFrame,
                                tableName: String): DataFrame = {
      val allColumns = gTScanRequest.getInfo.getAllColumns.asScala.toList

      val useColumns = gTScanRequest.getColumns.asScala.toList
      val selectedFactTable = allColumns.map { columnIndex =>
        if (useColumns.contains(columnIndex)) {
          col(SchemaProcessor.generateFactTableSchemaName(tableName, columnIndex, columnMapping(columnIndex)).toString)
        } else {
          literalOne.as(SchemaProcessor.generateFactTableSchemaName(tableName, columnIndex, columnMapping(columnIndex)))
        }
      }
      val selectedColumn = selectedFactTable ++ dataFrame.schema.fieldNames.filter(!_.startsWith("F")).map(col)
      dataFrame.select(selectedColumn: _*)
    }*/
}
