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

import io.kyligence.kap.query.relnode.KapAggregateRel
import io.kyligence.kap.query.runtime.RuntimeHelper
import org.apache.calcite.DataContext
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlKind
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.kylin.query.relnode.{KylinAggregateCall, OLAPAggregateRel}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{AggArgc, Column, DataFrame, KapFunctions, SparkOperation}

import scala.collection.JavaConverters._

// scalastyle:off
object AggregatePlan extends Logging {
  val binaryMeasureType =
    List("COUNT_DISTINCT", "PERCENTILE", "PERCENTILE_APPROX", "INTERSECT_COUNT")

  def agg(inputs: java.util.List[DataFrame],
          rel: KapAggregateRel,
          dataContext: DataContext): DataFrame = {
    val start = System.currentTimeMillis()
    val dataFrame = inputs.get(0)
    val schemaNames = dataFrame.schema.fieldNames
    val groupList = rel.getGroupSet.asScala
      .map(groupId => col(schemaNames.apply(groupId)))
      .toList
    val aggList = buildAgg(schemaNames, rel)
    val df = SparkOperation.agg(AggArgc(dataFrame, groupList, aggList))
    logInfo(s"Gen aggregate cost Time :${System.currentTimeMillis() - start} ")
    df
  }

  def buildAgg(schemaNames: Array[String],
               rel: KapAggregateRel): List[Column] = {
    val hash = System.identityHashCode(rel).toString

    rel.getRewriteAggCalls.asScala.zipWithIndex.map {
      case (call: KylinAggregateCall, index: Int)
          if binaryMeasureType.contains(OLAPAggregateRel.getAggrFuncName(call)) =>
        val dataType = call.getFunc.getReturnDataType
        val isCount = call.getFunc.isCount
        val funcName =
          if (isCount) FunctionDesc.FUNC_COUNT else OLAPAggregateRel.getAggrFuncName(call)
        val argNames = call.getArgList.asScala.map(schemaNames.apply(_))
        val columnName = argNames.map(col)
        var registeredFuncName =
          RuntimeHelper.registerSingleByColName(funcName, dataType)
        val aggName =
          SchemaProcessor.replaceToAggravateSchemaName(index, funcName, hash, argNames: _*)
        if (funcName == "COUNT_DISTINCT") {
          if (dataType.getName == "hllc") {
            org.apache.spark.sql.KapFunctions
              .approx_count_distinct(columnName.head, dataType.getPrecision)
              .alias(aggName)
          } else {
            KapFunctions.precise_count_distinct(columnName.head).alias(aggName)
          }
        } else {
          callUDF(registeredFuncName, columnName.toList: _*).alias(aggName)
        }
      case (call: Any, index: Int) =>
        val funcName = OLAPAggregateRel.getAggrFuncName(call)
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
            first(argNames.head).alias(aggName)
          case FunctionDesc.FUNC_GROUPING =>
            if (rel.getGroupSet.get(call.getArgList.get(0))) {
              k_lit(0).alias(aggName)
            } else {
              k_lit(1).alias(aggName)
            }
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
