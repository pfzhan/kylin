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

import java.lang.{Boolean, Byte, Double, Float, Long, Short}
import java.math.BigDecimal
import java.util.{GregorianCalendar, TimeZone}

import org.apache.calcite.DataContext
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.IntervalSqlType
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator
import org.apache.calcite.util.NlsString
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Convert RexNode to a nested Column
  *
  * @param df             dataframe
  * @param rowType        rowtyple
  * @param dataContext    context
  * @param afterAggregate agg
  */
class SparderRexVisitor(val df: DataFrame,
                        val rowType: RelDataType,
                        val dataContext: DataContext,
                        val afterAggregate: Boolean)
    extends RexVisitorImpl[Any](true) {
  val fieldNames: Array[String] = df.schema.fieldNames

  // scalastyle:off
  override def visitCall(call: RexCall): Any = {
    val children = new ListBuffer[Any]()
    for (operand <- call.operands.asScala) {
      val childFilter = operand.accept(this)
      children += childFilter
    }
    val op = call.getOperator
    op.getKind match {
      case AND =>
        children.foreach(filter => assert(filter.isInstanceOf[Column]))
        children.map(_.asInstanceOf[Column]).reduce {
          _.and(_)
        }
      case OR =>
        children.foreach(filter => assert(filter.isInstanceOf[Column]))
        children.map(_.asInstanceOf[Column]).reduce {
          _.or(_)
        }

      case NOT =>
        assert(children.size == 1)
        children.foreach(filter => assert(filter.isInstanceOf[Column]))
        not(children.head.asInstanceOf[Column])
      case EQUALS =>
        assert(children.size == 2)
        lit(children.head) === lit(children.last)
      case GREATER_THAN =>
        assert(children.size == 2)
        lit(children.head) > lit(children.last)
      case LESS_THAN =>
        assert(children.size == 2)
        lit(children.head) < lit(children.last)
      case GREATER_THAN_OR_EQUAL =>
        assert(children.size == 2)
        lit(children.head) >= lit(children.last)
      case LESS_THAN_OR_EQUAL =>
        assert(children.size == 2)
        lit(children.head) <= lit(children.last)
      case NOT_EQUALS =>
        assert(children.size == 2)
        lit(children.head) =!= lit(children.last)
      case IS_NULL =>
        assert(children.size == 1)
        lit(children.head).isNull
      case IS_NOT_NULL =>
        assert(children.size == 1)
        lit(children.head).isNotNull
      case LIKE =>
        assert(children.size == 2)
        lit(children.head).like(children.last.asInstanceOf[String])
      case PLUS =>
        assert(children.size == 2)
        if (op.getName.equals("DATETIME_PLUS")) {
          // scalastyle:off
          children.last match {
            case num: MonthNum => {
              val ts = to_utc_timestamp(
                from_unixtime(lit(children.head).cast("long").divide(1000)),
                TimeZone.getDefault.getID)
              val r = from_utc_timestamp(add_months(lit(ts), num.num),
                                         TimeZone.getDefault.getID)
              return lit(
                unix_timestamp(r).multiply(1000).cast("long").cast("string"))
            }
            case _ =>
              return lit(children.head)
                .plus(lit(children.last))
                .cast("long")
                .cast("string")

          }
        }

        return lit(children.head).plus(lit(children.last))
      case MINUS =>
        assert(children.size == 2)
        if (op.isInstanceOf[SqlDatetimeSubtractionOperator]) {

          if ("DAY".equalsIgnoreCase(
                call.`type`
                  .asInstanceOf[IntervalSqlType]
                  .getIntervalQualifier
                  .timeUnitRange
                  .name)) {
            datediff(lit(children.last), lit(children.head))
          } else {
            throw new IllegalStateException(
              "Only timestampdiff with date is supported")
          }
        }
        lit(children.head).minus(lit(children.last))
      case TIMES =>
        assert(children.size == 2)
        lit(children.head).multiply(lit(children.last))
      case DIVIDE =>
        assert(children.size == 2)
        lit(children.head).divide(lit(children.last))
      case CASE =>
        val evens =
          children.zipWithIndex.filter(p => p._2 % 2 == 0).map(p => lit(p._1))
        val odds =
          children.zipWithIndex.filter(p => p._2 % 2 == 1).map(p => lit(p._1))
        assert(evens.length == odds.length + 1)
        val zip = evens zip odds
        var column: Column = null
        if (zip.nonEmpty) {
          column = when(zip.head._1, zip.head._2)

          zip
            .drop(1)
            .foreach(p => {
              column = column.when(p._1, p._2)
            })
        }
        column.otherwise(evens.last)
      case EXTRACT => {
        val timeUnit = children.head.asInstanceOf[String]
        //TODO: here assuming all input ts is in long format, however some functions may produce ts directly?
        val inputAsTS = to_utc_timestamp(
          from_unixtime(lit(children.apply(1)).cast("long").divide(1000)),
          TimeZone.getDefault.getID)
        timeUnit match {
          case "YEAR"    => year(lit(inputAsTS))
          case "QUARTER" => quarter(lit(inputAsTS))
          case "MONTH"   => month(lit(inputAsTS))
          case "WEEK"    => weekofyear(lit(inputAsTS))
          case "DOY"     => dayofyear(lit(inputAsTS))
          case "DAY"     => dayofmonth(lit(inputAsTS))
          case "DOW" =>
            throw new UnsupportedOperationException(
              "DOW is not supported under sparder")
          case "HOUR"   => hour(lit(inputAsTS))
          case "MINUTE" => minute(lit(inputAsTS))
          case "SECOND" => second(lit(inputAsTS))
        }
      }
      case REINTERPRET =>
        lit(children.head)
      case CAST =>
        lit(children.head).cast(
          SparderTypeUtil.convertSqlTypeNameToSparkType(
            call.getType.getSqlTypeName))
      case OTHER =>
        val funcName = call.getOperator.getName.toLowerCase
        funcName match {
          case "||" => concat(lit(children.head), lit(children.apply(1)))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case OTHER_FUNCTION =>
        val funcName = call.getOperator.getName.toLowerCase
        funcName match {
          //math_funcs
          case "abs" =>
            abs(
              lit(children.head).cast(SparderTypeUtil
                .convertSqlTypeNameToSparkType(call.getType.getSqlTypeName)))
          case "round" =>
            round(
              lit(children.head),
              children.apply(1).asInstanceOf[java.math.BigDecimal].intValue())

          //string_funcs
          case "lower"            => lower(lit(children.head))
          case "upper"            => upper(lit(children.head))
          case "char_length"      => length(lit(children.head))
          case "character_length" => length(lit(children.head))
          case "replace" =>
            regexp_replace(lit(children.head),
                           children.apply(1).asInstanceOf[String],
                           children.apply(2).asInstanceOf[String])
          case "substring" => {
            if (children.length == 3) {
              substring(
                lit(children.head),
                children.apply(1).asInstanceOf[java.math.BigDecimal].intValue(),
                children.apply(2).asInstanceOf[java.math.BigDecimal].intValue())
            } else {
              throw new UnsupportedOperationException(
                s"substring must provide three parameters under sparder")
            }
          }
          case "concat" =>
            concat(lit(children.head), lit(children.apply(1)))
          // time_funcs
          case "current_date" =>
            lit(
              DateTimeUtils
                .millisToDays(System.currentTimeMillis())
                .toLong * 24 * 3600 * 1000)
          case "current_timestamp" =>
            lit(System.currentTimeMillis() * 1000)
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        ceil(lit(children.head))
      case FLOOR =>
        floor(lit(children.head))
      case ARRAY_VALUE_CONSTRUCTOR =>
        array(lit(children.head))
      case _ =>
        throw new UnsupportedOperationException()
    }
  }

  override def visitLocalRef(localRef: RexLocalRef) =
    throw new UnsupportedOperationException("local ref:" + localRef)

  override def visitInputRef(inputRef: RexInputRef): Column =
    col(fieldNames(inputRef.getIndex))

  override def visitLiteral(literal: RexLiteral): Any = {
    val v = afterAggregate match {
      case Boolean.TRUE => convertFilterValueAfterAggr(literal)
      case Boolean.FALSE =>
        convertFilterValueAfterAggr(literal) //use same for now.
    }
    v match {
      case Some(toReturn) => toReturn
      case None           => null
    }
  }

  case class MonthNum(num: Int)

  // as underlying schema types for cuboid table are all "string",
  // we rely spark to convert the cuboid col data from string to real type to finish comparing
  private def convertFilterValueAfterAggr(literal: RexLiteral): Any = {
    if (literal == null || literal.getValue == null) {
      return None
    }

    literal.getType match {
      case t: IntervalSqlType => {
        if (Seq("MONTH", "YEAR").contains(
              t.getIntervalQualifier.timeUnitRange.name)) {
          return Some(
            MonthNum(literal.getValue.asInstanceOf[BigDecimal].intValue))
        }
      }
      case _ =>
    }

    literal.getValue match {
      case s: NlsString =>
        Some(s.getValue)
      case g: GregorianCalendar =>
        Some(g.getTimeInMillis)
      case range: TimeUnitRange =>
        // Extract(x from y) in where clause
        Some(range.name)
      case b: Boolean =>
        Some(b)
      case b: BigDecimal =>
        Some(b)
      case b: Float =>
        Some(b)
      case b: Double =>
        Some(b)
      case b: Integer =>
        Some(b)
      case b: Byte =>
        Some(b)
      case b: Short =>
        Some(b)
      case b: Long =>
        Some(b)
      case _ =>
        Some(literal.getValue.toString)
    }
  }

  //
  //  private def convertFilterValueBeforeAggr(literalValue: Any): Any = {
  //    if (literalValue == null) {
  //      return None
  //    }
  //    literalValue match {
  //      case s: NlsString =>
  //        s.getValue
  //      case g: GregorianCalendar =>
  //        g.getTimeInMillis.toString
  //      case range: TimeUnitRange =>
  //        // Extract(x from y) in where clause
  //        range.name
  //      case _ =>
  //        literalValue.toString
  //    }
  //  }

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Any = {
    val name = dynamicParam.getName
    val s = dataContext.get(name)
    val rType = dynamicParam.getType

    SparderTypeUtil.convertStringToValue(s, rType, toCalcite = false)
    //    rType.getSqlTypeName match {
    //      case n if SqlTypeName.CHAR_TYPES.contains(n) => s.toString
    //      case n if SqlTypeName.APPROX_TYPES.contains(n) || SqlTypeName.EXACT_TYPES.contains(n) => new BigDecimal(s.toString)
    //      case n if SqlTypeName.DATETIME_TYPES.contains(n) => DateFormat.stringToMillis(s.toString)
    //      case _ => s.toString
    //    }
  }
}
