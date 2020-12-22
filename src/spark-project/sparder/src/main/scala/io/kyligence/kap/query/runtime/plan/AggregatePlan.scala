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

import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.query.relnode.KapAggregateRel
import io.kyligence.kap.query.runtime.RuntimeHelper
import org.apache.calcite.DataContext
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlKind
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.kylin.query.relnode.{KylinAggregateCall, OLAPAggregateRel}
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{CreateArray, In}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.udaf.SingleValueAgg
import org.apache.spark.sql.util.SparderTypeUtil

import java.util.Locale
import scala.collection.JavaConverters._

// scalastyle:off
object AggregatePlan extends LogEx {
  val binaryMeasureType =
    List("PERCENTILE", "PERCENTILE_APPROX", "INTERSECT_COUNT", "COUNT_DISTINCT", "BITMAP_UUID")

  def agg(inputs: java.util.List[DataFrame],
          rel: KapAggregateRel,
          dataContext: DataContext): DataFrame = logTime("aggregate", info = true) {

      var dataFrame = inputs.get(0)
      val schemaNames = dataFrame.schema.fieldNames
      val groupList = rel.getRewriteGroupKeys.asScala
        .map(groupId => col(schemaNames.apply(groupId)))
        .toList

      if (rel.getContext != null && rel.getContext.isExactlyAggregate) {
        // exactly match, skip agg, direct project.
        val aggCols = rel.getRewriteAggCalls.asScala
          .map(call => col(schemaNames.apply(call.getArgList.get(0)))).toList
        val prjList = groupList ++ aggCols
        logInfo(s"Query exactly match index, skip agg, project $prjList.")
        dataFrame.select(prjList: _*)
      } else {
        dataFrame = genFiltersWhenIntersectCount(rel, dataFrame)
        val aggList = buildAgg(dataFrame.schema, rel)
        val groupSets = rel.getRewriteGroupSets.asScala
          .map(groupSet => groupSet.asScala.map(groupId => col(schemaNames.apply(groupId))).toList).toList
        SparkOperation.agg(AggArgc(dataFrame, groupList, aggList, groupSets, rel.isSimpleGroupType))
      }
  }

  private def genFiltersWhenIntersectCount(rel: KapAggregateRel, dataFrame: DataFrame): DataFrame = {
    try {
      val intersects = rel.getRewriteAggCalls.asScala.filter(_.isInstanceOf[KylinAggregateCall])
        .filter(!_.asInstanceOf[KylinAggregateCall].getFunc.isCount)
        .map(_.asInstanceOf[KylinAggregateCall])
        .filter(call => !call.getFunc.isCount && OLAPAggregateRel.getAggrFuncName(call).equals(FunctionDesc.FUNC_INTERSECT_COUNT))
      val names = dataFrame.schema.names
      val children = dataFrame.queryExecution.logical
      if (intersects.nonEmpty && intersects.size == rel.getRewriteAggCalls.size() && children.isInstanceOf[Project]) {
        // only exists intersect count function in agg
        val list = children.asInstanceOf[Project].projectList
        val supportGenFilter = intersects.forall { call =>
          val listIndex = call.getArgList.get(2)
          call.getArgList.size() == 3 && list.apply(listIndex).eval().asInstanceOf[ArrayData].array.map(_.toString)
            .forall(!_.contains(KylinConfig.getInstanceFromEnv.getIntersectFilterOrSeparator))
        }
        if (supportGenFilter) {
          val filters = intersects.map { call =>
            val filterColumnIndex = call.getArgList.get(1)
            val litIndex = call.getArgList.get(2)
            new Column(In(col(names(filterColumnIndex)).expr, list.apply(litIndex).children.head.asInstanceOf[CreateArray].children))
          }
          val column = filters.reduceLeft(_.or(_))
          dataFrame.filter(column)
        } else {
          dataFrame
        }
      } else {
        dataFrame
      }
    } catch {
      case e: Throwable => logWarning("Error occurred when generate filters", e)
        dataFrame
    }
  }

  def buildAgg(schema: StructType,
               rel: KapAggregateRel): List[Column] = {
    val hash = System.identityHashCode(rel).toString

    rel.getRewriteAggCalls.asScala.zipWithIndex.map {
      case (call: KylinAggregateCall, index: Int)
        if binaryMeasureType.contains(OLAPAggregateRel.getAggrFuncName(call)) =>
        val dataType = call.getFunc.getReturnDataType
        val isCount = call.getFunc.isCount
        val funcName =
          if (isCount) FunctionDesc.FUNC_COUNT else OLAPAggregateRel.getAggrFuncName(call)
        val argNames = call.getArgList.asScala.map(schema.names.apply(_))
        val columnName = argNames.map(col)
        val registeredFuncName = RuntimeHelper.registerSingleByColName(funcName, dataType)
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, funcName, hash, argNames: _*)
        if (funcName == "COUNT_DISTINCT") {
          if (dataType.getName == "hllc") {
            org.apache.spark.sql.KapFunctions
              .approx_count_distinct(columnName.head, dataType.getPrecision)
              .alias(aggName)
          } else {
              KapFunctions.precise_count_distinct(columnName.head).alias(aggName)
          }
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_BITMAP_UUID)) {
          KapFunctions.precise_bitmap_uuid(columnName.head).alias(aggName)
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_INTERSECT_COUNT)) {
          require(columnName.size >= 3, s"Input columns size ${columnName.size} don't greater than or equal to 3.")
          val columns = columnName.slice(0, 3).zipWithIndex.map {
            case (column: Column, 2) => column.cast(ArrayType.apply(schema.fields.apply(call.getArgList.get(1)).dataType))
            case (column: Column, _) => column
          }
          val separator = s"\\${KylinConfig.getInstanceFromEnv.getIntersectFilterOrSeparator}"
          val upperBound = KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound
          call.name.toUpperCase(Locale.ROOT) match {
            case FunctionDesc.FUNC_INTERSECT_COUNT => KapFunctions.intersect_count(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_VALUE => KapFunctions.intersect_value(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID => KapFunctions.intersect_bitmap(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_COUNT_V2 => KapFunctions.intersect_count_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_VALUE_V2 => KapFunctions.intersect_value_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_V2 => KapFunctions.intersect_bitmap_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case func => throw new UnsupportedOperationException(s"Unsupported intersect count function: $func, please check the sql.")
          }
        } else {
          callUDF(registeredFuncName, columnName.toList: _*).alias(aggName)
        }
      case (call: Any, index: Int) =>
        val funcName = OLAPAggregateRel.getAggrFuncName(call)
        val schemaNames = schema.names
        val argNames = call.getArgList.asScala.map(id => schemaNames.apply(id))
        val inputType = call.getType
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index,
          funcName,
          hash,
          argNames: _*)
        funcName match {
          case FunctionDesc.FUNC_SUM =>
            if (isSum0(call)) {
              sum0(
                col(argNames.head).cast(
                  SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
                .alias(aggName)
            } else {
              sum(
                col(argNames.head).cast(
                  SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
                .alias(aggName)
            }
          case FunctionDesc.FUNC_COUNT =>
            count(if (argNames.isEmpty) k_lit(1) else col(argNames.head))
              .alias(aggName)
          case FunctionDesc.FUNC_MAX =>
            max(
              col(argNames.head).cast(
                SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_MIN =>
            min(
              col(argNames.head).cast(
                SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_COUNT_DISTINCT =>
            countDistinct(argNames.head, argNames.drop(1): _*)
              .alias(aggName)
          // Issue 4337: Supported select (select '2012-01-02') as data, xxx from table group by xxx
          case SqlKind.SINGLE_VALUE.sql =>
            SingleValueAgg(schema.head).apply(col(argNames.head)).alias(aggName)
          case FunctionDesc.FUNC_GROUPING =>
            if (!rel.isSimpleGroupType) {
              grouping(argNames.head).alias(aggName)
            } else {
              if (rel.getRewriteGroupKeys.contains(call.getArgList.get(0))) {
                k_lit(0).alias(aggName)
              } else {
                k_lit(1).alias(aggName)
              }
            }
          case FunctionDesc.FUNC_COLLECT_SET =>
            array_distinct(flatten(collect_set(col(argNames.head))))
              .alias(aggName)
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported function name $funcName""")
        }
    }.toList
  }

  private def isSum0(call: AggregateCall) = {
    call.isInstanceOf[KylinAggregateCall] && call
      .asInstanceOf[KylinAggregateCall]
      .isSum0
  }
}
