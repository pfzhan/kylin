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
package io.kyligence.kap.query.runtime.plan

import io.kyligence.kap.query.relnode.KapWindowRel
import io.kyligence.kap.query.runtime.SparderRexVisitor
import org.apache.calcite.DataContext
import org.apache.calcite.rel.RelCollationImpl
import org.apache.calcite.rex.RexInputRef
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._

object WindowPlan {
  // the function must have sort
  val sortSpecified =
    List("CUME_DIST", "LEAD", "RANK", "DENSE_RANK", "ROW_NUMBER", "NTILE")

  // the function must row range
  val rowSpecified =
    List("RANK", "PERCENT_RANK", "DENSE_RANK", "NTILE", "ROW_NUMBER")

  // the function no range
  val nonRangeSpecified = List(
    "LAG",
    "LEAD"
  )

  def window(input: java.util.List[DataFrame],
             rel: KapWindowRel, datacontex: DataContext): DataFrame = {
    var windowCount = 0
    rel.groups.asScala.head.upperBound
    val df = input.get(0)
    val columnSize = df.schema.length

    val columns = df.schema.fieldNames.map(col)
    val constantMap = rel.getConstants.asScala
      .map(_.getValue)
      .zipWithIndex
      .filter(_._1.isInstanceOf[Number])
      .map { entry =>
        (entry._2 + columnSize, entry._1.asInstanceOf[Number])
      }
      .toMap
    val visitor = new SparderRexVisitor(df,
      rel.getInput.getRowType,
      datacontex)
    val constants = rel.getConstants.asScala
      .map { constant =>
        lit(Literal.apply(constant.accept(visitor)))
      }
    val columnsAndConstants = columns ++ constants
    val windows = rel.groups.asScala
      .flatMap { group =>
        var isDateTimeFamilyType = false
        val fieldsNameToType = rel.getInput.getRowType.getFieldList.asScala.zipWithIndex
          .map {
            case (field, index) => index -> field.getType.getSqlTypeName.toString
          }.toMap

        fieldsNameToType.foreach(map =>
          if (SparderTypeUtil.isDateTimeFamilyType(map._2)) {
            isDateTimeFamilyType = true
          })

        var orderByColumns = group.orderKeys
          .asInstanceOf[RelCollationImpl]
          .getFieldCollations
          .asScala
          .map { fieldIndex =>
            var column = columns.apply(fieldIndex.getFieldIndex)
            if (!group.isRows && !fieldsNameToType(fieldIndex.getFieldIndex).equalsIgnoreCase("date")) {
              column = column.cast(LongType)
            }
            column
          }
          .toList
        val partitionColumns = group.keys.asScala
          .map(fieldIndex => columns.apply(fieldIndex))
          .toSeq
        group.aggCalls.asScala.map { agg =>
          var windowDesc: WindowSpec = null
          val opName = agg.op.getName.toUpperCase
          var (lowerBound: Long, upperBound: Long) = buildRange(group, constantMap, isDateTimeFamilyType, group.isRows)
          if (orderByColumns.nonEmpty) {

            windowDesc = Window.orderBy(orderByColumns: _*)
            if (!nonRangeSpecified.contains(opName)) {
              if (group.isRows || rowSpecified.contains(opName)) {
                windowDesc = windowDesc.rowsBetween(lowerBound, upperBound)
              } else {
                windowDesc = windowDesc.rangeBetween(lowerBound, upperBound)
              }
            }
          } else {
            if (sortSpecified.contains(opName)) {
              windowDesc = Window.orderBy(lit(1))
              if (!nonRangeSpecified.contains(opName)) {
                if (group.isRows || rowSpecified.contains(opName)) {
                  windowDesc = windowDesc.rowsBetween(lowerBound, upperBound)
                } else {
                  windowDesc = windowDesc.rangeBetween(lowerBound, upperBound)
                }
              }
            }
          }
          if (partitionColumns.nonEmpty) {
            windowDesc =
              if (windowDesc == null) Window.partitionBy(partitionColumns: _*)
              else windowDesc.partitionBy(partitionColumns: _*)
          }

          val func = opName match {
            case "ROW_NUMBER" =>
              row_number()
            case "RANK" =>
              rank()
            case "DENSE_RANK" =>
              dense_rank()
            case "FIRST_VALUE" =>
              first(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "LAST_VALUE" =>
              last(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "LEAD" =>
              val args =
                agg.operands.asScala.map(_.asInstanceOf[RexInputRef].getIndex)
              args.size match {
                // offset default value is 1 in spark
                case 1 => lead(columnsAndConstants.apply(args.head), 1)
                case 2 => lead(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).intValue())
                case 3 => lead(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).intValue(),
                  constantMap.apply(args(2)))
              }

            case "LAG" =>
              val args =
                agg.operands.asScala.map(_.asInstanceOf[RexInputRef].getIndex)
              args.size match {
                // offset default value is 1 in spark
                case 1 => lag(columnsAndConstants.apply(args.head), 1)
                case 2 => lag(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).intValue())
                case 3 => lag(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).intValue(),
                  constantMap.apply(args(2)))
              }
            case "NTILE" =>
              ntile(constantMap
                .apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex)
                .intValue())
            case "COUNT" =>
              count(
                if (agg.operands.isEmpty) {
                  lit(1)
                } else {
                  columnsAndConstants.apply(
                    agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex)
                }
              )
            case "MAX" =>
              max(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case x if opName.contains("SUM") =>
              sum(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "MIN" =>
              min(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "AVG" =>
              avg(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
          }
          windowCount = windowCount + 1
          val alias = s"${System.identityHashCode(rel)}_window_" + windowCount
          if (windowDesc == null) {
            func.over().alias(alias)
          } else {
            func.over(windowDesc).alias(alias)
          }
        }

      }
    val selectColumn = columns ++ windows
    df.select(selectColumn: _*)
  }

  def buildRange(group: org.apache.calcite.rel.core.Window.Group,
                 constantMap: Map[Int, Number],
                 isDateType: Boolean,
                 isRows: Boolean): (Long, Long) = {
    var lowerBound = Window.currentRow
    if (group.lowerBound.isPreceding) {
      if (group.lowerBound.isUnbounded) {
        lowerBound = Window.unboundedPreceding
      } else {
        lowerBound = -constantMap
          .apply(group.lowerBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          lowerBound = lowerBound / 1000
        }

      }
    } else if (group.lowerBound.isFollowing) {
      if (group.lowerBound.isUnbounded) {
        lowerBound = Window.unboundedFollowing
      } else {
        lowerBound = constantMap
          .apply(group.lowerBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          lowerBound = lowerBound / 1000
        }

      }
    }

    var upperBound = Window.currentRow
    if (group.upperBound.isPreceding) {
      if (group.upperBound.isUnbounded) {
        upperBound = Window.unboundedPreceding
      } else {
        upperBound = -constantMap
          .apply(group.upperBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          upperBound = upperBound / 1000
        }

      }
    } else if (group.upperBound.isFollowing) {
      if (group.upperBound.isUnbounded) {
        upperBound = Window.unboundedFollowing
      } else {
        upperBound = constantMap
          .apply(group.upperBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          upperBound = upperBound / 1000
        }
      }
    }

    (lowerBound, upperBound)
  }
}
