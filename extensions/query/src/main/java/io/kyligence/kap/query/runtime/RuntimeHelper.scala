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

package io.kyligence.kap.query.runtime

import com.google.common.collect.Sets
import io.kyligence.kap.query.relnode.KapRel
import io.kyligence.kap.query.util.SparderTupleConverter
import io.kyligence.kap.storage.parquet.cube.CubeStorageQuery
import org.apache.kylin.cube.CubeSegment
import org.apache.kylin.cube.model.CubeDesc.DeriveType
import org.apache.kylin.dict.BuiltInFunctionTransformer
import org.apache.kylin.gridtable.{GTInfo, GTScanRequest}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.filter.{
  ITupleFilterTransformer,
  StringCodeSystem,
  TupleFilter,
  TupleFilterSerializer
}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.query.relnode.OLAPContext
import org.apache.kylin.storage.gtrecord.{
  CubeScanRangePlanner,
  GTCubeStorageQueryRequest
}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.manager.{SparderDictionaryManager, UdfManager}
import org.apache.spark.sql.udf.AggraFuncArgs

import scala.collection.JavaConverters._
import scala.collection.mutable

object RuntimeHelper {

  def bindVariable(filter: TupleFilter, olapContext: OLAPContext): Unit = {
    if (filter == null) return

    for (childFilter <- filter.getChildren.asScala) {
      bindVariable(childFilter, olapContext)
    }

    //    filter match {
    //      case compFilter: CompareTupleFilter if olapContext != null =>
    //        for (entry <- compFilter.getVariables.entrySet().asScala) {
    //          val variable = entry.getKey
    //          Object value = optiqContext.get(variable)
    //          if (value != null) {
    //            String str = value.toString();
    //            if (compFilter.getColumn().getType().isDateTimeFamily())
    //              str = String.valueOf(DateFormat.stringToMillis(str));
    //
    //            compFilter.bindVariable(variable, str);
    //          }
    //
    //        }
    //      case _ =>
    //    }
  }

  def checkUdf(scanRequest: GTScanRequest,
               gTinfoStr: String): Map[Int, String] = {
    register(cleanRequest(scanRequest, gTinfoStr))
  }

  def cleanRequest(gTScanRequest: GTScanRequest,
                   gTinfoStr: String): Map[Int, AggraFuncArgs] = {

    gTScanRequest.getAggrMetrics
      .iterator()
      .asScala
      .zip(gTScanRequest.getAggrMetricsFuncs.iterator)
      .map {
        case (colId, funName) =>
          (colId.toInt,
           AggraFuncArgs(funName,
                         gTScanRequest.getInfo.getColumnType(colId),
                         gTinfoStr))
      }
      .toMap
  }

  // register by colId
  def register(aggrMap: Map[Int, AggraFuncArgs]): Map[Int, String] = {
    aggrMap.map {
      case (colId, aggr) =>
        val name = aggr.dataTp.toString
          .replace("(", "_")
          .replace(")", "_")
          .replace(",", "_") + aggr.funcName
        UdfManager.register(aggr.dataTp, aggr.funcName)
        (colId, name)
    }
  }

  // register by colName
  def registerByColName(
      aggrMap: Map[String, (String, DataType)]): Map[String, String] = {
    aggrMap.map {
      case (colName, (funcName, dataType)) =>
        val name = dataType.toString
          .replace("(", "_")
          .replace(")", "_")
          .replace(",", "_") + funcName
        UdfManager.register(dataType, funcName)
        (colName, name)
    }
  }

  def registerSingleByColName(colName: String,
                              funcName: String,
                              dataType: DataType): (String, String) = {
    val name = dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
    UdfManager.register(dataType, funcName)
    (colName, name)
  }

  def prepareScanRequest(
      olapContext: OLAPContext,
      cubeSegment: CubeSegment,
      cubeStorageQuery: CubeStorageQuery): (GTCubeStorageQueryRequest,
                                            Option[GTScanRequest],
                                            Option[Map[Integer, String]]) = {
    olapContext.resetSQLDigest()
    val sqlDigest = olapContext.getSQLDigest

    val request = cubeStorageQuery.getStorageQueryRequest(
      olapContext.storageContext,
      sqlDigest,
      olapContext.returnTupleInfo)
    var filter = request.getFilter
    val serialize =
      TupleFilterSerializer.serialize(filter, StringCodeSystem.INSTANCE)
    filter =
      TupleFilterSerializer.deserialize(serialize, StringCodeSystem.INSTANCE)
    val translator: ITupleFilterTransformer = new BuiltInFunctionTransformer(
      cubeSegment.getDimensionEncodingMap)
    translator.transform(filter)
    var scanRangePlanner: CubeScanRangePlanner = null
    scanRangePlanner = new CubeScanRangePlanner(cubeSegment,
                                                request.getCuboid,
                                                filter,
                                                request.getDimensions,
                                                request.getGroups,
                                                request.getMetrics,
                                                request.getHavingFilter,
                                                request.getContext)

    def buildDictionaryMap = {
      try {
        val mapping = request.getCuboid.getCuboidToGridTableMapping
        request.getDimensions.asScala
          .filter(
            dimension =>
              cubeSegment.getCubeDesc.getRowkey
                .getColDesc(dimension)
                .isUsingDictionary)
          .map(
            dimension =>
              (mapping
                 .makeGridTableColumns(Sets.newHashSet(dimension))
                 .asScala
                 .head,
               getDictPath(cubeSegment, dimension)))
          .filter(!_._2.equalsIgnoreCase("null"))
          .toMap
      } catch {
        case e: RuntimeException =>
          e.printStackTrace()
          null
        case e: Exception =>
          e.printStackTrace()
          null
      }
    }

    val dictMapping = buildDictionaryMap
    val scanRequest = scanRangePlanner.planScanRequest()
    //    scanRequest.getColumns
    (request, Some(scanRequest), Some(dictMapping))
  }

  def getDictPath(cubeSegment: CubeSegment, tblColRef: TblColRef): String = {
    val dictPath = cubeSegment.getDictResPath(tblColRef)
    if (dictPath == null) {
      return "null"
    }
    SparderDictionaryManager.add(dictPath)
    //    val store = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv).getStore
    //    val raw = store.getResource(dictPath)
  }

  def gtInfoSchemaToCalciteSchema(rel: KapRel,
                                  cubeTupleConverter: SparderTupleConverter,
                                  gtinfo: GTInfo,
                                  dataFrame: DataFrame): Seq[Column] = {
    val gTInfoNames = SchemaProcessor.buildFactTableSortNames(dataFrame)
    val factTableName = rel.getContext.firstTableScan.getBackupAlias
    val calciteToGTinfo = cubeTupleConverter.tupleIdx.zipWithIndex.toMap
    val gtColIdx = cubeTupleConverter.getGTIndex()
    val allColumns = rel.getColumnRowType.getAllColumns.asScala
    var deriveMap: Map[Int, Column] = Map.empty
    if (cubeTupleConverter.hasDerived) {
      deriveMap = cubeTupleConverter.hostToDeriveds.flatMap { hostToDerived =>
        val columns = mutable.ListBuffer.empty[(Int, Column)]
        val derivedTableName = hostToDerived.aliasTableName
        if (hostToDerived.deriveType.equals(DeriveType.PK_FK)) {
          require(hostToDerived.calciteIdx.length == 1)
          require(hostToDerived.hostIdx.length == 1)

          columns.append(
            (hostToDerived.calciteIdx.apply(0),
             col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
               .alias(
                 SchemaProcessor
                   .generateDeriveTableSchema(derivedTableName,
                                              hostToDerived.lookupIdx.apply(0))
                   .toString)))
        } else {
          hostToDerived.calciteIdx.zip(hostToDerived.lookupIdx).foreach {
            case (calciteIdx, lookupIdx) =>
              columns.append(
                (calciteIdx,
                 col(SchemaProcessor.generateDeriveTableSchema(derivedTableName,
                                                               lookupIdx))))
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
          if (calciteToGTinfo.contains(index)) {
            //  primarykey
            if (gtinfo.getPrimaryKey.get(
                  gtColIdx.apply(calciteToGTinfo.apply(index)))) {
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
            lit(1).as(s"${factTableName}_${columnName}")
          }
      }
  }
}
