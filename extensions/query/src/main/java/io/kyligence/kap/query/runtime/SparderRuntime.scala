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

import java.nio.ByteBuffer
import java.util

import io.kyligence.kap.query.exception.UnsupportedQueryException
import io.kyligence.kap.query.relnode._
import io.kyligence.kap.query.util.SparderTupleConverter
import io.kyligence.kap.storage.parquet.cube.CubeStorageQuery
import io.kyligence.kap.storage.parquet.format.filter.{
  BinaryFilterConverter,
  BinaryFilterSerializer
}
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexLiteral, _}
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.cube.{CubeInstance, CubeSegment}
import org.apache.kylin.gridtable.{GTInfo, GTScanRequest, GTUtil}
import org.apache.kylin.metadata.model.SegmentStatusEnum
import org.apache.kylin.query.relnode.{KylinAggregateCall, OLAPAggregateRel}
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest
import org.apache.kylin.storage.hybrid.HybridInstance
import org.apache.spark.sql.execution.utils.{
  CubePathCache,
  HexUtils,
  SchemaProcessor
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame, SparderFunc}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class SparderRuntime {}

object SparderRuntime {
  private val logger = LoggerFactory.getLogger(classOf[SparderRuntime])

  def createOlapTable(rel: KapRel, dataContext: DataContext): DataFrame = {
    if (SparderFunc.spark.sparkContext.isStopped) {
      SparderFunc.initSpark()
    }

    val olapContext = rel.getContext
    olapContext.bindVariable(dataContext)

    val realization = olapContext.realization
    val cubeInstance = realization match {
      case instance: HybridInstance =>
        if (instance.getRealizations.length > 1 && instance.getRealizations
              .exists(!_.isInstanceOf[CubeInstance])) {
          throw new UnsupportedQueryException("unsupported raw table")
        }
        instance.getRealizations.map(_.asInstanceOf[CubeInstance]).head
      case instance: CubeInstance =>
        instance.asInstanceOf[CubeInstance]
      case _ =>
        throw new UnsupportedQueryException("unsupported instance")
    }
    val query = new CubeStorageQuery(cubeInstance)
    val cubeSegmentList =
      cubeInstance.getSegments(SegmentStatusEnum.READY).asScala

    val dfs = cubeSegmentList
      .map { cubeSegment =>
        (cubeSegment,
         RuntimeHelper.prepareScanRequest(olapContext, cubeSegment, query))
      }
      .filter(_._2._2.get != null) //  过滤掉没有gtscanrequest的segment
      .filter {
        //  过滤掉没有文件的segment
        case (cubeSegment: CubeSegment,
              (request: GTCubeStorageQueryRequest,
               optionScanRequest: Option[GTScanRequest],
               dictMapping: Option[Map[Integer, String]])) =>
          CubePathCache
            .cuboidFileSeq(request.getCuboid.getId,
                           cubeInstance.getId,
                           cubeSegment.getUuid)
            .nonEmpty
      }
      .map {
        case (cubeSegment: CubeSegment,
              (request: GTCubeStorageQueryRequest,
               optionScanRequest: Option[GTScanRequest],
               dictMapping: Option[Map[Integer, String]])) =>
          val scanRequest = optionScanRequest.get
          //    val instance = olapContext.
          // realization.asInstanceOf[HybridInstance]
          // .getRealizations.apply(0).asInstanceOf[CubeInstance]
          //  prepare gtinfo
          val gtinfo = scanRequest.getInfo
          var gTScanRequest = HexUtils.toHexString(scanRequest.toByteArray)
          val buffer = ByteBuffer.allocate(10240)
          GTInfo.serializer.serialize(scanRequest.getInfo, buffer)
          val gtStr = HexUtils.toHexString(buffer.array())
          val tableName = olapContext.firstTableScan.getBackupAlias
          val mapping = request.getCuboid.getCuboidToGridTableMapping

          // build convert
          val gtDimsIdx = mapping.getDimIndexes(request.getDimensions)
          val gtMetricsIdx = mapping.getMetricsIndexes(request.getMetrics)
          val gtColIdx = gtDimsIdx ++ gtMetricsIdx
          val converter = new SparderTupleConverter(tableName,
                                                    cubeSegment,
                                                    request.getCuboid,
                                                    request.getDimensions,
                                                    request.getMetrics,
                                                    gtColIdx,
                                                    olapContext.returnTupleInfo)

          //  prepare filter
          var dictmap = dictMapping.get
          if(dictmap == null){
              dictmap = Map.empty[Integer, String]
          }

          val jsonMap = JsonUtil.writeValueAsString(dictmap.asJava)
          val pushdownHex = HexUtils.toHexString(
            GTUtil.serializeGTFilter(scanRequest.getFilterPushDown, gtinfo))
          var binaryFilterHex: String = ""
          if (scanRequest != null && scanRequest.getFilterPushDown != null && !BinaryFilterConverter
                .containsSpecialFilter(scanRequest.getFilterPushDown)) {
            val binaryFilter = new BinaryFilterConverter(scanRequest.getInfo)
              .toBinaryFilter(scanRequest.getFilterPushDown)
            binaryFilterHex = HexUtils.toHexString(
              BinaryFilterSerializer.serialize(binaryFilter))
          }
          var df = SparderFunc.createDictPushDownTable(
            CreateDictPushdownTableArgc(
              gtStr,
              tableName,
              request.getCuboid.getId.toString,
              SchemaProcessor.buildSchemaV2(gtinfo, tableName),
              pushdownHex,
              binaryFilterHex,
              jsonMap,
              CubePathCache.cuboidFileSeq(request.getCuboid.getId,
                                          cubeInstance.getId,
                                          cubeSegment.getUuid),
              KapConfig.getInstanceFromEnv.sparderFileFormat()
            ))
          val converteredColumns =
            RuntimeHelper.gtInfoSchemaToCalciteSchema(rel,
                                                      converter,
                                                      gtinfo,
                                                      df)

          // todo   initspark
          if (converter.hasDerived) {
            df = converter.joinDerived(gtinfo, df)
          }

          df.select(converteredColumns: _*)
      }
    if (dfs.isEmpty) {
      SparderFunc.createEmptyDataFrame(
        StructType(
          rel.getColumnRowType.getAllColumns.asScala
            .map(column => StructField(column.getName, StringType))))
    } else {
      dfs.reduce(_.union(_))
    }
  }

  def createLookupTable(rel: KapRel): DataFrame = {
    if (SparderFunc.spark.sparkContext.isStopped) {
      SparderFunc.initSpark()
    }
    val olapContext = rel.getContext
    var cube: CubeInstance = null
    olapContext.realization match {
      case cube1: CubeInstance => cube = cube1
      case hybridInstance: HybridInstance =>
        val latestRealization = hybridInstance.getLatestRealization
        // scalastyle:off
        latestRealization match {
          case cube1: CubeInstance => cube = cube1
          case _                   => throw new IllegalStateException
        }
      case _ =>
    }

    val lookupTableName = olapContext.firstTableScan.getTableName
    val dim = cube.getDescriptor.findDimensionByTable(lookupTableName)
    if (dim == null){
      throw new IllegalStateException(
        "No dimension with derived columns found for lookup table "
          + lookupTableName + ", cube desc " + cube.getDescriptor)
    }
    val tableName = dim.getJoin.getPKSide.getTableIdentity
    val snapshotResPath =
      cube.getLatestReadySegment.getSnapshotResPath(tableName)
    val config = cube.getConfig
    val dataFrameTableName = cube.getProject + "@" + lookupTableName
    //    this.allRows = table.getAllRows
    //
    val lookupDf = SparderLookupManager.getOrCreate(dataFrameTableName,
                                                    snapshotResPath,
                                                    config)
    val olapTable = olapContext.firstTableScan.getOlapTable
    val alisTableName = olapContext.firstTableScan.getBackupAlias
    val newNames = lookupDf.schema.fieldNames.map { name =>
      val gTInfoSchema = SchemaProcessor.parseDeriveTableSchemaName(name)
      SchemaProcessor.generateDeriveTableSchema(alisTableName, gTInfoSchema._3)
    }.array
    val newNameLookupDf = lookupDf.toDF(newNames: _*)
    val colIndex = olapTable.getSourceColumns.asScala.map(
      column =>
        col(
          SchemaProcessor
            .generateDeriveTableSchema(alisTableName, column.getZeroBasedIndex)
            .toString))
    newNameLookupDf.select(colIndex: _*)
  }

  def select(inputs: util.List[DataFrame], rel: KapProjectRel): DataFrame = {

    val dataFrame = inputs.get(0)
    val selectedColumns = rel.rewriteProjects.asScala
      .map(rex => {
        val visitor = new SparderRexVisitor(dataFrame,
                                            rel.getInput.getRowType,
                                            null,
                                            rel.getContext.afterAggregate)
        rex.accept(visitor)
      })
      .zipWithIndex
      .map(c => {
        //  add p0,p1 suffix for window queries will generate
        // indicator columns like false,false,false
        lit(c._1).as(s"cxt${rel.getContext.id}_prj${c._2}")
      })

    dataFrame.select(selectedColumns: _*)
  }

  def agg(inputs: util.List[DataFrame], rel: KapAggregateRel): DataFrame = {
    //    val schema = statefulDF.indexSchema
    val dataFrame = inputs.get(0)
    val schemaNames = dataFrame.schema.fieldNames
    val groupList = rel.getGroupSet.asScala
      .map(groupId => col(schemaNames.apply(groupId)))
      .toList
    val aggList = buildAgg(schemaNames, rel)
    SparderFunc.agg(AggArgc(dataFrame, groupList, aggList))
  }

  def buildAgg(schemaNames: Array[String],
               rel: KapAggregateRel): List[Column] = {
    val contextId = rel.getContext.id
    rel.getRewriteAggCalls.asScala.zipWithIndex.map {
      case (call: KylinAggregateCall, index: Int) =>
        val dataType = call.getFunc.getReturnDataType
        val funcName = OLAPAggregateRel.getAggrFuncName(call)
        val columnName =
          call.getArgList.asScala.map(id => schemaNames.apply(id)).head
        val (schema, registeredFuncName) =
          RuntimeHelper.registerSingleByColName(columnName, funcName, dataType)
        callUDF(registeredFuncName, col(columnName)).alias(
          s"cxt${contextId}_agg($index)")
      case (call: Any, index: Int) =>
        val funcName = OLAPAggregateRel.getAggrFuncName(call)
        val colArgs = call.getArgList.asScala.map(id => schemaNames.apply(id))
        // scalastyle:off
        funcName match {
          case "SUM"   => sum(colArgs.head).alias(s"cxt${contextId}_agg($index)")
          case "COUNT" => count(lit(1)).alias(s"cxt${contextId}_agg($index)")
          case "MAX"   => max(colArgs.head).alias(s"cxt${contextId}_agg($index)")
          case "MIN"   => min(colArgs.head).alias(s"cxt${contextId}_agg($index)")
          case "COUNT_DISTINCT" =>
            countDistinct(colArgs.head, colArgs.drop(1): _*)
              .alias(s"cxt${contextId}_agg($index})")
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported function name $funcName""")
        }
    }.toList
  }

  def join(inputs: util.List[DataFrame], rel: KapJoinRel): DataFrame = {

    // val schema = statefulDF.indexSchema
    val lDataFrame = inputs.get(0)
    val lSchemaNames = lDataFrame.schema.fieldNames
    val rDataFrame = inputs.get(1)
    val rSchemaNames = rDataFrame.schema.fieldNames
    var joinCol: Column = null

    //todo   utils
    rel.getLeftKeys.asScala
      .zip(rel.getRightKeys.asScala)
      .foreach(tuple => {
        if (joinCol == null) {
          joinCol = col(lSchemaNames.apply(tuple._1))
            .equalTo(col(rSchemaNames.apply(tuple._2)))
        } else {
          joinCol = joinCol.and(
            col(lSchemaNames.apply(tuple._1))
              .equalTo(col(rSchemaNames.apply(tuple._2))))
        }
      })
    if (joinCol == null) {
      lDataFrame.crossJoin(rDataFrame)
    } else {
      lDataFrame.join(rDataFrame, joinCol, rel.getJoinType.lowerName)
    }
  }

  def filter(inputs: util.List[DataFrame],
             condition: RexNode,
             rel: KapFilterRel,
             dataContext: DataContext): DataFrame = {
    val df = inputs.get(0)
    val visitor = new SparderRexVisitor(df,
                                        rel.getInput.getRowType,
                                        dataContext,
                                        rel.getContext.afterAggregate)
    val filterColumn = condition.accept(visitor).asInstanceOf[Column]
    df.filter(filterColumn)
  }

  def union(inputs: util.List[DataFrame], rel: KapUnionRel): DataFrame = {
    var df = inputs.get(0)
    val drop = inputs.asScala.drop(1)
    for (other <- drop) {
      df = df.union(other)
    }
    df
  }

  def limit(inputs: util.List[DataFrame], rel: KapLimitRel): DataFrame = {
    //    val schema = statefulDF.indexSchema
    inputs
      .get(0)
      .limit(BigDecimal(
        rel.localFetch.asInstanceOf[RexLiteral].getValue.toString).toInt)

  }

  def sort(inputs: util.List[DataFrame], rel: KapSortRel): DataFrame = {

    val dataFrame = inputs.get(0)

    val columns = rel.getChildExps.asScala
      .map(rex => {
        val visitor = new SparderRexVisitor(dataFrame,
                                            rel.getInput.getRowType,
                                            null,
                                            rel.getContext.afterAggregate)
        rex.accept(visitor)
      })
      .map(c => lit(c))
      .zipWithIndex //.map//map{case (x,y) => {null}}
      .map(pair => {
        val collation = rel.collation.getFieldCollations.get(pair._2)

        /** From Calcite: org.apache.calcite.rel.RelFieldCollation.Direction#defaultNullDirection
          * Returns the null direction if not specified. Consistent with Oracle,
          * NULLS are sorted as if they were positive infinity. */
        (collation.direction, collation.nullDirection) match {
          case (
              org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
              org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED) =>
            pair._1.asc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            pair._1.asc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            pair._1.asc_nulls_first
          case (
              org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
              org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED) =>
            pair._1.desc_nulls_first
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST) =>
            pair._1.desc_nulls_last
          case (org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING,
                org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST) =>
            pair._1.desc_nulls_first
          case _ => throw new IllegalArgumentException
        }
      })

    inputs.get(0).sort(columns: _*)
  }

  def collectEnumerable(df: DataFrame,
                        rowType: RelDataType): Enumerable[Array[Any]] = {
    val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
    Linq4j.asEnumerable(rowsItr.array)
  }

  def collectScalarEnumerable(df: DataFrame,
                              rowType: RelDataType): Enumerable[Any] = {
    val rowsItr: Array[Array[Any]] = collectInternal(df, rowType)
    val x = rowsItr.toIterable.map(a => a.apply(0)).asJava
    Linq4j.asEnumerable(x)
  }

  private def collectInternal(df: DataFrame, rowType: RelDataType) = {
    val resultTypes = rowType.getFieldList.asScala
    val data = df.collect()

    data.map { row =>
      var rowIndex = 0
      row.toSeq.map { cell =>
        {
          val rType = resultTypes.apply(rowIndex).getType
          val value =
            SparderTypeUtil.convertStringToValue(cell, rType, toCalcite = true)
          rowIndex = rowIndex + 1
          value
        }
      }.toArray
    }
  }
}
