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
import io.kyligence.kap.engine.spark.builder.DFFlatTableEncoder
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.measure.hllc.HLLCMeasureType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.util.SparderTypeUtil.toSparkType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

object CuboidAggregator {
  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, NDataModel.Measure],
          seg: NDataSegment): DataFrame = {
    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange)
    agg(ss, dataSet, dimensions, measures, flatTableDesc, isSparkSql = false)
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
      val parameters = function.getParameter.getPlainParameters.asScala.toList
      val columns = new mutable.ListBuffer[Column]
      if (parameters.head.isColumnType) {
        try {
          if (afterAgg) {
            columns.append(col(measureEntry._1.toString))
          } else {
            columns.appendAll(parameters.map(p => col(coulmnIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)))))
          }
        } catch {
          case e: Exception =>
            throw e
        }
      } else {
        if (afterAgg) {
          columns.append(col(measureEntry._1.toString))
        } else {
          if (function.getExpression.equalsIgnoreCase("SUM")) {
            val parameteter = parameters.head.getValue
            if (StringUtils.isNumeric(parameteter))  {
                if (parameteter.contains(".")) {
                  columns.append(lit(parameteter.toDouble))
                } else {
                  columns.append(lit(parameteter.toLong))
                }
            }
          } else {
            columns.append(lit(parameters.head.getValue))
          }
        }
      }

      function.getExpression.toUpperCase match {
        case "MAX" =>
          max(columns.head).as(measureEntry._1.toString)
        case "MIN" =>
          min(columns.head).as(measureEntry._1.toString)
        case "SUM" =>
          sum(columns.head).as(measureEntry._1.toString)
        case "COUNT" =>
          if (afterAgg) {
            sum(columns.head).as(measureEntry._1.toString)
          } else {
            count(columns.head).as(measureEntry._1.toString)
          }
        case "COUNT_DISTINCT" =>
          if (isSparkSql) {
            countDistinct(columns.head).as(measureEntry._1.toString)
          } else {
            val udfName = UdfManager.register(function.getReturnDataType,
              function.getExpression,
              null,
              !afterAgg)
            if (!afterAgg) {
              var col = columns.head
              if (function.getReturnDataType.getName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
                col = wrapEncodeColumn(parameters.head.getColRef, columns.head)
              } else if (columns.length > 1 && function.getReturnDataType.getName.startsWith(HLLCMeasureType.DATATYPE_HLLC)) {
                col = wrapMutilHllcColumn(columns: _*)
              }
              callUDF(udfName, col.cast(StringType))
                .as(measureEntry._1.toString)
            } else {
              callUDF(udfName, columns.head).as(measureEntry._1.toString)
            }
          }
        case "TOP_N" =>

          val measure = function.getParameter.getColRef.getColumnDesc

          val schema = StructType(parameters.map(_.getColRef.getColumnDesc).map { col =>
            val dateType = toSparkType(col.getType)
            if (col == measure) {
              StructField(s"MEASURE_${col.getName}", dateType)
            } else {
              StructField(s"DIMENSION_${col.getName}", dateType)
            }
          })

          val udfName = UdfManager.register(function.getReturnDataType,
            function.getExpression, schema, !afterAgg)
          callUDF(udfName, columns: _*).as(measureEntry._1.toString)
        case "PERCENTILE_APPROX" =>
          val udfName = UdfManager.register(function.getReturnDataType, function.getExpression, null, !afterAgg)
          if (!afterAgg) {
            callUDF(udfName, columns.head.cast(StringType)).as(measureEntry._1.toString)
          } else {
            callUDF(udfName, columns.head).as(measureEntry._1.toString)
          }
      }
    }.toSeq

    if (!dimensions.isEmpty) {
      dataSet
        .groupBy(NSparkCubingUtil.getColumns(dimensions): _*)
        .agg(agg.head, agg.drop(1): _*)
    } else {
      dataSet.agg(agg.head, agg.drop(1): _*)
    }
  }

  def wrapEncodeColumn(ref: TblColRef, column: Column): Column = {
    new Column(column.toString() + DFFlatTableEncoder.ENCODE_SUFFIX)
  }

  def wrapMutilHllcColumn(columns: Column*): Column = {
    var col: Column = when(isnull(columns.head), null)
    for (inputCol <- columns.drop(1)) {
      col = col.when(isnull(inputCol), null)
    }

    col = col.otherwise(hash(columns: _*))
    col
  }
}
