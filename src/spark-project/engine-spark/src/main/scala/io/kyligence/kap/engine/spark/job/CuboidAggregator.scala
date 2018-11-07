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

package io.kyligence.kap.engine.spark.job

import java.util

import io.kyligence.kap.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._

object CuboidAggregator {
  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, NDataModel.Measure],
          seg: NDataSegment): DataFrame = {
    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(seg.getCubePlan, seg.getSegRange)
    agg(ss, dataSet, dimensions, measures, flatTableDesc, false)
  }

  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, NDataModel.Measure],
          flatTableDesc: NCubeJoinedFlatTableDesc,
          isSparkSql: Boolean): DataFrame = {
    if (measures.isEmpty) {
      return dataSet
        .select(NSparkCubingUtil.getColumns(dimensions): _*)
        .dropDuplicates()
    }

    val afterAgg = dataSet.schema.fieldNames
      .contains(measures.keySet().asScala.head.toString)
    val coulmnIndex =
      dataSet.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap

    val agg = measures.asScala.map { measureEntry =>
      val measure = measureEntry._2
      val function = measure.getFunction
      val parameter = function.getParameter.getPlainParameters.asScala.head
      if (parameter.isColumnType) {
        var column: Column = null
        try {
          if (afterAgg) {
            column = col(measureEntry._1.toString)
          } else {
            column = col(
              coulmnIndex.apply(
                flatTableDesc.getColumnIndex(parameter.getColRef)))
          }
        } catch {
          case e: Exception =>
            throw e
        }
        function.getExpression.toUpperCase match {
          case "MAX" =>
            max(column).as(measureEntry._1.toString)
          case "MIN" =>
            min(column).as(measureEntry._1.toString)
          case "SUM" =>
            sum(column).as(measureEntry._1.toString)
          case "COUNT_DISTINCT" =>
            if (isSparkSql) {
              countDistinct(column).as(measureEntry._1.toString)
            } else {
              val udfName = UdfManager.register(function.getReturnDataType,
                                                function.getExpression,
                                                !afterAgg)
              if (!afterAgg) {
                callUDF(udfName, column.cast(StringType))
                  .as(measureEntry._1.toString)
              } else {
                callUDF(udfName, column).as(measureEntry._1.toString)
              }
            }
        }
      } else if (function.isCount) {
        if (afterAgg) {
          sum(col(measureEntry._1.toString)).as(measureEntry._1.toString)
        } else {
          count(lit(1)).as(measureEntry._1.toString)
        }
      } else {
        lit("")
      }
    }.toSeq
    dataSet
      .groupBy(NSparkCubingUtil.getColumns(dimensions): _*)
      .agg(agg.head, agg.drop(1): _*)
  }
}
