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
package io.kyligence.kap.query.runtime.plan

import java.util

import com.google.common.collect.Sets
import io.kyligence.kap.cube.cuboid.NLayoutCandidate
import io.kyligence.kap.cube.gridtable.NCuboidToGridTableMapping
import io.kyligence.kap.cube.kv.NCubeDimEncMap
import io.kyligence.kap.cube.model.{NDataSegment, NDataflow}
import io.kyligence.kap.query.exception.UnsupportedQueryException
import io.kyligence.kap.query.relnode.KapRel
import io.kyligence.kap.query.runtime.RuntimeHelper
import io.kyligence.kap.query.util.SparderDerivedUtil
import org.apache.calcite.DataContext
import org.apache.kylin.common.KapConfig
import org.apache.kylin.cube.gridtable.GridTables
import org.apache.kylin.metadata.model.{
  FunctionDesc,
  SegmentStatusEnum,
  TblColRef
}
import org.apache.kylin.metadata.realization.IRealization
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasource.CubeRelation
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.{DataFrame, _}

import scala.collection.JavaConverters._

// scalastyle:off
object TableScanPlan extends Logging {
  private var session: SparkSession = SparderEnv.getSparkSession
  private val kylinConfig: KapConfig = KapConfig.getInstanceFromEnv

  def listSegmentsForQuery(cube: NDataflow): util.List[NDataSegment] = {
    val r: util.List[NDataSegment] = new util.ArrayList[NDataSegment]
    import scala.collection.JavaConversions._
    for (seg <- cube.getSegments(SegmentStatusEnum.READY)) {
      r.add(seg)
    }
    r
  }

  def createOLAPTable(rel: KapRel, dataContext: DataContext): DataFrame = {
    val start = System.currentTimeMillis()
    if (session.sparkContext.isStopped) {
      SparderEnv.initSpark()
    }

    val olapContext = rel.getContext
    rel.getContext.bindVariable(dataContext)
    val realization = olapContext.realization
    val cubeInstances: List[IRealization] = realization match {

      case instance: NDataflow => List(instance)

      //          case instance: RawTableInstance => List(instance)

      case _ =>
        throw new UnsupportedQueryException("unsupported instance")
    }

    val cuboidDF = cubeInstances
      .map {
        case dataflow: NDataflow =>
          olapContext.resetSQLDigest()

          val context = olapContext.storageContext
          val cuboidLayout = context.getCandidate.getCuboidLayout
          val tableName = olapContext.firstTableScan.getBackupAlias
          val mapping = new NCuboidToGridTableMapping(cuboidLayout)
          val info =
            GridTables.newGTInfo(mapping,
                                 new NCubeDimEncMap(dataflow.getFirstSegment))
          val relation =
            CubeRelation(tableName, dataflow, info, cuboidLayout)(session)
          /////////////////////////////////////////////
          val kapConfig = KapConfig.wrap(dataflow.getConfig)
          val basePath = kapConfig.getReadParquetStoragePath
          val segments = listSegmentsForQuery(dataflow)
          val fileList = segments.asScala.map(
            seg =>
              s"$basePath${dataflow.getUuid}/${seg.getId}/${relation.cuboid.getId}"
          )

          var df = session.read
            .parquet(fileList: _*)
            .toDF(relation.schema.fieldNames: _*)
          /////////////////////////////////////////////
          val groups: util.Collection[TblColRef] =
            olapContext.getSQLDigest.groupbyColumns
          val otherDims = Sets.newHashSet(context.getDimensions)
          otherDims.removeAll(groups)
          // expand derived (xxxD means contains host columns only, derived columns were translated)
          val groupsD = expandDerived(context.getCandidate, groups)
          val otherDimsD: util.Set[TblColRef] =
            expandDerived(context.getCandidate, otherDims)
          otherDimsD.removeAll(groupsD)

          // identify cuboid
          val dimensionsD = new util.LinkedHashSet[TblColRef]
          dimensionsD.addAll(groupsD)
          dimensionsD.addAll(otherDimsD)
          //          var df = session.baseRelationToDataFrame(relation)
          val gtColIdx = mapping.getDimIndices(dimensionsD) ++ mapping
            .getMetricsIndices(context.getMetrics)

          val derived = SparderDerivedUtil(tableName,
                                           dataflow.getFirstSegment,
                                           gtColIdx,
                                           olapContext.returnTupleInfo,
                                           context.getCandidate)
          if (derived.hasDerived) {
            df = derived.joinDerived(df)
          }

          val tupleIdx = getTupleIdx(dimensionsD,
                                     context.getMetrics,
                                     olapContext.returnTupleInfo)
          val schema = RuntimeHelper.gtSchemaToCalciteSchema(
            mapping.getPrimaryKey,
            derived,
            tableName,
            rel.getColumnRowType.getAllColumns.asScala.toList,
            relation.schema,
            gtColIdx,
            tupleIdx)
          df.select(schema: _*)
      }
      .reduce(_.union(_))
    logInfo(s"Gen table scan cost Time :${System.currentTimeMillis() - start} ")
    cuboidDF
  }

  // copy from NCubeTupleConverter
  def getTupleIdx(selectedDimensions: util.Set[TblColRef],
                  selectedMetrics: util.Set[FunctionDesc],
                  tupleInfo: TupleInfo): Array[Int] = {
    var tupleIdx: Array[Int] =
      new Array[Int](selectedDimensions.size + selectedMetrics.size)

    var i = 0
    // pre-calculate dimension index mapping to tuple
    selectedDimensions.asScala.foreach(
      dim => {
        tupleIdx(i) =
          if (tupleInfo.hasColumn(dim)) tupleInfo.getColumnIndex(dim) else -1
        i += 1
      }
    )

    selectedMetrics.asScala.foreach(
      metric => {
        if (metric.needRewrite) {
          val rewriteFieldName = metric.getRewriteFieldName
          tupleIdx(i) =
            if (tupleInfo.hasField(rewriteFieldName))
              tupleInfo.getFieldIndex(rewriteFieldName)
            else -1
        } else { // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
          val col = metric.getParameter.getColRefs.get(0)
          tupleIdx(i) =
            if (tupleInfo.hasColumn(col)) tupleInfo.getColumnIndex(col) else -1
        }
        i += 1
      }
    )
    tupleIdx
  }

  def createLookupTable(rel: KapRel, dataContext: DataContext): DataFrame = {
    if (session.sparkContext.isStopped) {
      SparderEnv.initSpark()
      session = SparderEnv.getSparkSession
    }
    val olapContext = rel.getContext
    var instance: NDataflow = olapContext.realization.asInstanceOf[NDataflow]

    val lookupTableName = olapContext.firstTableScan.getTableName
    val snapshotResPath =
      instance.getLastSegment.getSnapshots.get(lookupTableName)
    val config = instance.getConfig
    val dataFrameTableName = instance.getProject + "@" + lookupTableName
    val lookupDf = SparderLookupManager.getOrCreate(dataFrameTableName,
                                                    snapshotResPath,
                                                    config)
    val olapTable = olapContext.firstTableScan.getOlapTable
    val alisTableName = olapContext.firstTableScan.getBackupAlias
    val newNames = lookupDf.schema.fieldNames.map { name =>
      val gTInfoSchema = SchemaProcessor.parseDeriveTableSchemaName(name)
      SchemaProcessor.generateDeriveTableSchemaName(alisTableName,
                                                    gTInfoSchema.columnId,
                                                    gTInfoSchema.columnName)
    }.array
    val newNameLookupDf = lookupDf.toDF(newNames: _*)
    val colIndex = olapTable.getSourceColumns.asScala.map(
      column =>
        col(
          SchemaProcessor
            .generateDeriveTableSchemaName(alisTableName,
                                           column.getZeroBasedIndex,
                                           column.getName)
            .toString))
    newNameLookupDf.select(colIndex: _*)
  }

  private def expandDerived(
      layoutCandidate: NLayoutCandidate,
      cols: util.Collection[TblColRef]): util.Set[TblColRef] = {
    val expanded = new util.HashSet[TblColRef]
    cols.asScala.foreach(
      col => {
        val hostInfo = layoutCandidate.getDerivedToHostMap.get(col)
        if (hostInfo != null) {
          for (hostCol <- hostInfo.columns) {
            expanded.add(hostCol)
          }
        } else {
          expanded.add(col)
        }
      }
    )
    expanded
  }

}
