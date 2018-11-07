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
import java.sql.Timestamp
import java.util.GregorianCalendar

import io.kyligence.kap.query.util.UnsupportedSparkFunctionException
import org.apache.calcite.DataContext
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.{
  IntervalSqlType,
  SqlTypeFamily,
  SqlTypeName
}
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator
import org.apache.calcite.util.NlsString
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Convert RexNode to a nested Column
  *
  * @param df          dataframe
  * @param rowType     rowtyple
  * @param dataContext context
  */
class SparderRexVisitor(val df: DataFrame,
                        val rowType: RelDataType,
                        val dataContext: DataContext)
    extends RexVisitorImpl[Any](true) {
  val fieldNames: Array[String] = df.schema.fieldNames

  // scalastyle:off
  override def visitCall(call: RexCall): Any = {

    val children = new ListBuffer[Any]()
    var isDateType = false
    var isTimeType = false

    for (operand <- call.operands.asScala) {
      if (operand.getType.getSqlTypeName.name().equals("DATE")) {
        isDateType = true
      }
      if (operand.getType.getSqlTypeName.name().equals("TIMESTAMP")) {
        isTimeType = true
      }

      val childFilter = operand.accept(this)
      children += childFilter
    }

    def getOperands: (Column, Column) = {
      var left = lit(children.head)
      var right = lit(children.last)

      // get the lit pos.
      // ($1, "2010-01-01 15:43:38") pos:1
      // ("2010-01-01 15:43:38", $1) pos:0
      val litPos = call.getOperands.asScala.zipWithIndex
        .filter(!_._1.isInstanceOf[RexInputRef])
        .map(_._2)

      if (isDateType) {
        litPos
          .foreach {
            case 0 => left = left.cast(TimestampType).cast(DateType)
            case 1 => right = right.cast(TimestampType).cast(DateType)
          }
      }
      if (isTimeType) {
        litPos
          .foreach {
            case 0 => left = left.cast(TimestampType)
            case 1 => right = right.cast(TimestampType)
          }
      }

      (left, right)
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
        val (left: Column, right: Column) = getOperands
        left === right
      case GREATER_THAN =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left > right
      case LESS_THAN =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left < right
      case GREATER_THAN_OR_EQUAL =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left >= right
      case LESS_THAN_OR_EQUAL =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left <= right
      case NOT_EQUALS =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left =!= right
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
              // both add_month and add_year case
              val ts = lit(children.head).cast(TimestampType).cast(LongType)
              return lit(kap_add_months(lit(ts), num.num))
            }
            case _ =>
          }
        }

        call.getType.getSqlTypeName match {
          case SqlTypeName.DATE =>
            lit(children.head)
              .cast(TimestampType)
              .cast(LongType)
              .plus(lit(children.last))
              .cast(TimestampType)
              .cast(DateType)
          case SqlTypeName.TIMESTAMP =>
            lit(children.head)
              .cast(LongType)
              .plus(lit(children.last))
              .cast(TimestampType)
          case _ =>
            lit(children.head)
              .plus(lit(children.last))
              .cast(LongType)
        }
      case MINUS =>
        assert(children.size == 2)
        if (op.isInstanceOf[SqlDatetimeSubtractionOperator]) {

          val timeUnitName = call.`type`
            .asInstanceOf[IntervalSqlType]
            .getIntervalQualifier
            .timeUnitRange
            .name
          if ("DAY".equalsIgnoreCase(timeUnitName) || "SECOND".equalsIgnoreCase(
                timeUnitName)) {
            // for ADD_DAY case
            // the calcite plan looks like: /INT(Reinterpret(-($0, 2012-01-01)), 86400000)
            // and the timeUnitName is DAY

            // for ADD_WEEK case
            // the calcite plan looks like: /INT(CAST(/INT(Reinterpret(-($0, 2000-01-01)), 1000)):INTEGER, 604800)
            // and the timeUnitName is SECOND

            // expecting ts instead of seconds
            // so we need to multiply 1000 here

            val ts1 = lit(children.head).cast(TimestampType).cast(LongType) //col
            val ts2 = lit(children.last).cast(LongType) //lit
            ts1.minus(ts2).multiply(1000)

          } else if ("MONTH".equalsIgnoreCase(timeUnitName) || "YEAR"
                       .equalsIgnoreCase(timeUnitName)) {

            // for ADD_YEAR case,
            // the calcite plan looks like: CAST(/INT(Reinterpret(-($0, 2000-03-01)), 12)):INTEGER
            // and the timeUnitName is YEAR

            // for ADD_QUARTER case
            // the calcite plan looks like: /INT(CAST(Reinterpret(-($0, 2000-01-01))):INTEGER, 3)
            // and the timeUnitName is MONTH

            // for ADD_MONTH case

            val ts1 = lit(children.head).cast(TimestampType)
            val ts2 = lit(children.last).cast(TimestampType)
            kap_subtract_months(ts1, ts2)

          } else {
            throw new IllegalStateException(
              "Unsupported SqlInterval: " + timeUnitName)
          }
        } else {
          lit(children.head).minus(lit(children.last))
        }
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
        val inputAsTS = children.apply(1)
        call.operands.get(1).getType.getSqlTypeName.name() match {
          case "DATE" =>
          case _ =>
            throw new UnsupportedSparkFunctionException(
              s"Unsupported function $timeUnit")
        }

        timeUnit match {
          case "YEAR"    => year(lit(inputAsTS))
          case "QUARTER" => quarter(lit(inputAsTS))
          case "MONTH"   => month(lit(inputAsTS))
          case "WEEK"    => weekofyear(lit(inputAsTS))
          case "DOY"     => dayofyear(lit(inputAsTS))
          case "DAY"     => dayofmonth(lit(inputAsTS))
          case "DOW"     => kap_day_of_week(lit(inputAsTS))
          case _ =>
            throw new UnsupportedSparkFunctionException(
              s"Unsupported function $timeUnit")
        }
      }
      case REINTERPRET =>
        lit(children.head)
      case CAST =>
        // all date type is long,skip is
        val goalType = SparderTypeUtil.convertSqlTypeNameToSparkType(
          call.getType.getSqlTypeName)
        lit(children.head).cast(goalType)

      case TRIM =>
        if (children.length == 3) {
          children.head match {
            case "TRAILING" =>
              rtrim(lit(children.last))
            case "LEADING" =>
              ltrim(lit(children.last))
            case "BOTH" =>
              trim(lit(children.last))
          }
        } else {
          trim(lit(children.head))
        }

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
          case "cot" =>
            lit(1).divide(tan(lit(children.head)))

          //string_funcs
          case "lower"            => lower(lit(children.head))
          case "upper"            => upper(lit(children.head))
          case "char_length"      => length(lit(children.head))
          case "character_length" => length(lit(children.head))
          case "replace" =>
            regexp_replace(lit(children.head),
                           children.apply(1).asInstanceOf[String],
                           children.apply(2).asInstanceOf[String])
          case "substring" =>
            if (children.length == 3) {
              lit(children.head)
                .substr(lit(children.apply(1)), lit(children.apply(2)))
            } else {
              throw new UnsupportedOperationException(
                s"substring must provide three parameters under sparder")
            }
          case "concat" =>
            concat(lit(children.head), lit(children.apply(1)))
          // time_funcs
          case "current_date" =>
            lit(
              DateTimeUtils.dateToString(
                DateTimeUtils.millisToDays(System.currentTimeMillis())))
          case "current_timestamp" =>
            lit(SparderTypeUtil.toSparkTimestamp(System.currentTimeMillis()))
          case "power" =>
            pow(lit(children.head), lit(children.apply(1)))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        ceil(lit(children.head))
      case FLOOR =>
        floor(lit(children.head))
      case ARRAY_VALUE_CONSTRUCTOR =>
        array(children.map(child => lit(child.toString)): _*)
      case _ =>
        throw new UnsupportedOperationException()
    }
  }

  override def visitLocalRef(localRef: RexLocalRef) =
    throw new UnsupportedOperationException("local ref:" + localRef)

  override def visitInputRef(inputRef: RexInputRef): Column =
    col(fieldNames(inputRef.getIndex))

  override def visitLiteral(literal: RexLiteral): Any = {
    val v = convertFilterValueAfterAggr(literal)
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
        if (Seq("MONTH", "YEAR", "QUARTER").contains(
              t.getIntervalQualifier.timeUnitRange.name)) {
          return Some(
            MonthNum(literal.getValue.asInstanceOf[BigDecimal].intValue))
        }
        if (literal.getType.getFamily
              .asInstanceOf[SqlTypeFamily] == SqlTypeFamily.INTERVAL_DAY_TIME) {
          return Some(
            SparderTypeUtil.toSparkTimestamp(
              new java.math.BigDecimal(literal.getValue.toString).longValue()))
        }
      }
      case _ =>
    }

    literal.getValue match {
      case s: NlsString =>
        Some(s.getValue)
      case g: GregorianCalendar =>
        Some(SparderTypeUtil.toSparkTimestamp(g.getTimeInMillis))
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
    var s = dataContext.get(name)
    val rType = dynamicParam.getType

    if (rType.getSqlTypeName.getName.equals("TIMESTAMP")) {
      // s is a string like "2010-01-01 15:43:38", need to be java.sql.Timestamp
      s = new Timestamp(DateFormat.stringToMillis(s.toString))
    }
    SparderTypeUtil.convertStringToValue(s, rType, toCalcite = false)
  }
}
