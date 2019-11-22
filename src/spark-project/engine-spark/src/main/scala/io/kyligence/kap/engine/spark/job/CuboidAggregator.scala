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

import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel.Measure
import org.apache.kylin.common.KapConfig
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.measure.hllc.HLLCMeasureType
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.udaf._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.util.SparderTypeUtil.toSparkType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

object CuboidAggregator {

  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, Measure],
          seg: NDataSegment,
          spanningTree: NSpanningTree): DataFrame = {

    val needJoin = spanningTree match {
      // when merge cuboid st is null
      case null => true
      case _ => DFChooser.needJoinLookupTables(seg.getModel, spanningTree)
    }

    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, needJoin)

    aggInternal(ss, dataSet, dimensions, measures, flatTableDesc, isSparkSql = false)
  }

  def aggInternal(ss: SparkSession,
                  dataSet: DataFrame,
                  dimensions: util.Set[Integer],
                  measures: util.Map[Integer, Measure],
                  flatTableDesc: NCubeJoinedFlatTableDesc,
                  isSparkSql: Boolean): DataFrame = {
    if (measures.isEmpty) {
      return dataSet
        .select(NSparkCubingUtil.getColumns(dimensions): _*)
        .dropDuplicates()
    }

    val reuseLayout = dataSet.schema.fieldNames
      .contains(measures.keySet().asScala.head.toString)
    val columnIndex =
      dataSet.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap

    var taggedColIndex: Int = -1

    val agg = measures.asScala.map { measureEntry =>
      val measure = measureEntry._2
      val function = measure.getFunction
      val parameters = function.getParameters.asScala.toList
      val columns = new mutable.ListBuffer[Column]
      val returnType = function.getReturnDataType
      if (parameters.head.isColumnType) {
        if (reuseLayout) {
          columns.append(col(measureEntry._1.toString))
        } else {
          columns.appendAll(parameters.map(p => col(columnIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)))))
        }
      } else {
        if (reuseLayout) {
          columns.append(col(measureEntry._1.toString))
        } else {
          val par = parameters.head.getValue
          if (function.getExpression.equalsIgnoreCase("SUM")) {
            columns.append(lit(par).cast(SparderTypeUtil.toSparkType(returnType)))
          } else {
            columns.append(lit(par))
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
          if (reuseLayout) {
            sum(columns.head).as(measureEntry._1.toString)
          } else {
            count(columns.head).as(measureEntry._1.toString)
          }
        case "COUNT_DISTINCT" =>
          // add for test
          if (isSparkSql) {
            countDistinct(columns.head).as(measureEntry._1.toString)
          } else {
            var cdCol = columns.head
            val isBitmap = returnType.getName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)
            val isHllc = returnType.getName.startsWith(HLLCMeasureType.DATATYPE_HLLC)

            if (isBitmap && parameters.size == 2) {
              require(measures.size() == 1, "Opt intersect count can only has one measure.")
              taggedColIndex = columnIndex.apply(flatTableDesc.getColumnIndex(parameters.last.getColRef)).toInt
              val tagCol = col(taggedColIndex.toString)
              if (!reuseLayout) {
                val separator = KapConfig.getInstanceFromEnv.getIntersectCountSeparator
                cdCol = wrapEncodeColumn(columns.head)
                new Column(OptIntersectCount(cdCol.expr, split(tagCol, s"\\$separator").expr).toAggregateExpression())
                  .as(s"map_${measureEntry._1.toString}")
              } else {
                new Column(ReusePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                  .as(measureEntry._1.toString)
              }
            } else {
              if (!reuseLayout) {
                if (isBitmap) {
                  cdCol = wrapEncodeColumn(columns.head)
                  new Column(EncodePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else if (columns.length > 1 && isHllc) {
                  cdCol = wrapMutilHllcColumn(columns: _*)
                  new Column(EncodeApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else {
                  new Column(EncodeApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                }
              } else {
                if (isBitmap) {
                  new Column(ReusePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else if (columns.length > 1 && isHllc) {
                  cdCol = wrapMutilHllcColumn(columns: _*)
                  new Column(ReuseApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else {
                  new Column(ReuseApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                }
              }
            }
          }
        case "TOP_N" =>

          val measure = function.getParameters.get(0).getColRef.getColumnDesc

          val schema = StructType(parameters.map(_.getColRef.getColumnDesc).map { col =>
            val dateType = toSparkType(col.getType)
            if (col == measure) {
              StructField(s"MEASURE_${col.getName}", dateType)
            } else {
              StructField(s"DIMENSION_${col.getName}", dateType)
            }
          })

          val udfName = UdfManager.register(returnType,
            function.getExpression, schema, !reuseLayout)
          callUDF(udfName, columns: _*).as(measureEntry._1.toString)

        case "PERCENTILE_APPROX" =>
          val udfName = UdfManager.register(returnType, function.getExpression, null, !reuseLayout)
          if (!reuseLayout) {
            callUDF(udfName, columns.head.cast(StringType)).as(measureEntry._1.toString)
          } else {
            callUDF(udfName, columns.head).as(measureEntry._1.toString)
          }
      }
    }.toSeq

    val dim = if (taggedColIndex != -1 && !reuseLayout) {
      val d = new util.HashSet[Integer](dimensions)
      d.remove(taggedColIndex)
      d
    } else {
      dimensions
    }

    val df: DataFrame = if (!dim.isEmpty) {
      dataSet
        .groupBy(NSparkCubingUtil.getColumns(dim): _*)
        .agg(agg.head, agg.drop(1): _*)
    } else {
      dataSet
        .agg(agg.head, agg.drop(1): _*)
    }

    // Avoid sum(decimal) add more precision
    // For example: sum(decimal(19,4)) -> decimal(29,4)  sum(sum(decimal(19,4))) -> decimal(38,4)
    if (reuseLayout) {
      val columns = NSparkCubingUtil.getColumns(dimensions) ++ measureColumns(dataSet.schema, measures)
      df.select(columns: _*)
    } else {
      if (taggedColIndex != -1) {
        val icCol = df.schema.fieldNames.filter(_.contains("map")).head
        val fieldsWithoutIc = df.schema.fieldNames.filter(!_.contains(icCol))

        val cdMeasureName = icCol.split("_").last
        val newSchema = fieldsWithoutIc.:+(taggedColIndex.toString).:+(cdMeasureName)

        val exploded = fieldsWithoutIc.map(col).:+(explode(col(icCol)))
        df.select(exploded: _*).toDF(newSchema: _*)
      } else {
        df
      }
    }
  }

  private def measureColumns(schema: StructType, measures: util.Map[Integer, Measure]): mutable.Iterable[Column] = {
    measures.asScala.map { mea =>
      val measureId = mea._1.toString
      mea._2.getFunction.getExpression.toUpperCase match {
        case "SUM" =>
          val dataType = schema.find(_.name.equals(measureId)).get.dataType
          col(measureId).cast(dataType).as(measureId)
        case _ =>
          col(measureId)
      }
    }
  }

  def wrapEncodeColumn(column: Column): Column = {
    new Column(column.toString() + ENCODE_SUFFIX)
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
