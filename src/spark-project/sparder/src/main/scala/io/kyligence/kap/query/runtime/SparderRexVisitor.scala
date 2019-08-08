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

import java.lang
import java.math.BigDecimal
import java.sql.Timestamp

import io.kyligence.kap.query.util.UnsupportedSparkFunctionException
import org.apache.calcite.DataContext
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.{BasicSqlType, IntervalSqlType, SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.catalyst.expressions.{Expression, IfNull, StringLocate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

 /**
  * Convert RexNode to a nested Column
  *
  * @param dfs         dataframes
  * @param rowType     rowtyple
  * @param dataContext context
  */
class SparderRexVisitor(val dfs: Array[DataFrame],
                        val rowType: RelDataType,
                        val dataContext: DataContext)
    extends RexVisitorImpl[Any](true) {
  val fieldNames: Array[String] = dfs.flatMap(df => df.schema.fieldNames)

  def this(df: DataFrame,
           rowType: RelDataType,
           dataContext: DataContext) = this(Array(df), rowType, dataContext)

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
      var left = k_lit(children.head)
      var right = k_lit(children.last)

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
        k_lit(children.head).isNull
      case IS_NOT_NULL =>
        assert(children.size == 1)
        k_lit(children.head).isNotNull
      case LIKE =>
        assert(children.size == 2)
        k_like(k_lit(children.head), k_lit(children.last))
      case MINUS_PREFIX =>
        assert(children.size == 1)
        negate(k_lit(children.head))
      case IN => val values = children.drop(1).map(c => k_lit(c).expr)
        in(k_lit(children.head).expr, values)
      case NOT_IN =>
        val values = children.drop(1).map(c => k_lit(c).expr)
        not(in(k_lit(children.head).expr, values))
      case PLUS =>
        assert(children.size == 2)
        if (op.getName.equals("DATETIME_PLUS")) {
          // scalastyle:off
          children.last match {
            case num: MonthNum => {
              // both add_month and add_year case
              val ts = k_lit(children.head).cast(TimestampType)
              return k_lit(kap_add_months(k_lit(ts), num.num))
            }
            case _ =>
          }
        }

        call.getType.getSqlTypeName match {
          case SqlTypeName.DATE =>
            k_lit(children.head)
              .cast(TimestampType)
              .cast(LongType)
              .plus(k_lit(children.last))
              .cast(TimestampType)
              .cast(DateType)
          case SqlTypeName.TIMESTAMP =>
            k_lit(children.head)
              .cast(TimestampType)
              .cast(LongType)
              .plus(k_lit(children.last))
              .cast(TimestampType)
          case _ =>
            k_lit(children.head)
              .plus(k_lit(children.last))
        }
      case MINUS =>
        assert(children.size == 2)
        if (op.isInstanceOf[SqlDatetimeSubtractionOperator]) {
          call.getType.getSqlTypeName match {
            case SqlTypeName.DATE =>
              return k_lit(children.head).cast(TimestampType).cast(LongType).minus(lit(children.last)).cast(TimestampType).cast(DateType)
            case SqlTypeName.TIMESTAMP =>
              return k_lit(children.head)
                .cast(LongType)
                .minus(k_lit(children.last))
                .cast(TimestampType)
            case _ =>
          }
          val timeUnitName = call.`type`
            .asInstanceOf[IntervalSqlType]
            .getIntervalQualifier
            .timeUnitRange
            .name
          if ("DAY".equalsIgnoreCase(timeUnitName)
              || "SECOND".equalsIgnoreCase(timeUnitName)
              || "HOUR".equalsIgnoreCase(timeUnitName)
              || "MINUTE".equalsIgnoreCase(timeUnitName)) {
            // for ADD_DAY case
            // the calcite plan looks like: /INT(Reinterpret(-($0, 2012-01-01)), 86400000)
            // and the timeUnitName is DAY

            // for ADD_WEEK case
            // the calcite plan looks like: /INT(CAST(/INT(Reinterpret(-($0, 2000-01-01)), 1000)):INTEGER, 604800)
            // and the timeUnitName is SECOND

            // for MINUTE case
            // the Calcite plan looks like: CAST(/INT(Reinterpret(-($1, CAST($0):TIMESTAMP(0))), 60000)):INTEGER

            // for HOUR case
            // the Calcite plan looks like: CAST(/INT(Reinterpret(-($1, CAST($0):TIMESTAMP(0))), 3600000)):INTEGER

            // expecting ts instead of seconds
            // so we need to multiply 1000 here

            val ts1 = k_lit(children.head).cast(TimestampType).cast(LongType) //col
            val ts2 = k_lit(children.last).cast(TimestampType).cast(LongType) //lit
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

            val ts1 = k_lit(children.head).cast(TimestampType)
            val ts2 = k_lit(children.last).cast(TimestampType)
            kap_subtract_months(ts1, ts2)

          } else {
            throw new IllegalStateException(
              "Unsupported SqlInterval: " + timeUnitName)
          }
        } else {
          k_lit(children.head).minus(k_lit(children.last))
        }
      case TIMES =>
        assert(children.size == 2)
        children.head match {
          case num: MonthNum => {
            val ts = k_lit(children.apply(1)).cast(TimestampType).cast(LongType)
            MonthNum(k_lit(ts).multiply(k_lit(num.num)))
          }
          case _ =>
            k_lit(children.head).multiply(k_lit(children.last))
        }
      case DIVIDE =>
        assert(children.size == 2)
        k_lit(children.head).divide(k_lit(children.last))
      case CASE =>
        val evens =
          children.zipWithIndex.filter(p => p._2 % 2 == 0).map(p => k_lit(p._1))
        val odds =
          children.zipWithIndex.filter(p => p._2 % 2 == 1).map(p => k_lit(p._1))
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

        timeUnit match {
          case "YEAR"    => year(k_lit(inputAsTS))
          case "QUARTER" => quarter(k_lit(inputAsTS))
          case "MONTH"   => month(k_lit(inputAsTS))
          case "WEEK"    => weekofyear(k_lit(inputAsTS))
          case "DOY"     => dayofyear(k_lit(inputAsTS))
          case "DAY"     => dayofmonth(k_lit(inputAsTS))
          case "DOW"     => kap_day_of_week(k_lit(inputAsTS))
          case "HOUR"    => hour(k_lit(inputAsTS))
          case "MINUTE"  => minute(k_lit(inputAsTS))
          case "SECOND"  => second(k_lit(inputAsTS))
          case _ =>
            throw new UnsupportedSparkFunctionException(
              s"Unsupported function $timeUnit")
        }
      }
      case REINTERPRET =>
        k_lit(children.head)
      case CAST =>
        // all date type is long,skip is
        val goalType = SparderTypeUtil.convertSqlTypeToSparkType(
          call.getType)
        k_lit(children.head).cast(goalType)

      case TRIM =>
        if (children.length == 3) {
          children.head match {
            case "TRAILING" =>
              rtrim(k_lit(children.last))
            case "LEADING" =>
              ltrim(k_lit(children.last))
            case "BOTH" =>
              trim(k_lit(children.last))
          }
        } else {
          trim(k_lit(children.head))
        }
      case MOD =>
        assert(children.size == 2)
        val (left: Column, right: Any) = getOperands
        left mod right

      case OTHER =>
        val funcName = call.getOperator.getName.toLowerCase
        funcName match {
          case "||" => concat(k_lit(children.head), k_lit(children.apply(1)))
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
              k_lit(children.head).cast(SparderTypeUtil
                .convertSqlTypeToSparkType(call.getType)))
          case "round" =>
            round(
              k_lit(children.head),
              children.apply(1).asInstanceOf[Int])
          case "truncate" =>
            kap_truncate(k_lit(children.head), children.apply(1).asInstanceOf[Int])
          case "cot" =>
            k_lit(1).divide(tan(k_lit(children.head)))
          //null handling funcs
          case "isnull" =>
            isnull(k_lit(children.head))
          case "ifnull" =>
              new Column(new IfNull(k_lit(children.head).expr, k_lit(children.apply(1)).expr))
          //string_funcs
          case "lower"            => lower(k_lit(children.head))
          case "upper"            => upper(k_lit(children.head))
          case "char_length"      => length(k_lit(children.head))
          case "character_length" => length(k_lit(children.head))
          case "replace" =>
            regexp_replace(k_lit(children.head),
                           children.apply(1).asInstanceOf[String],
                           children.apply(2).asInstanceOf[String])
          case "substring" | "substr" =>
            if (children.length == 3) { //substr(str1,startPos,length)
              k_lit(children.head)
                .substr(k_lit(children.apply(1)), k_lit(children.apply(2)))
            } else if (children.length == 2) { //substr(str1,startPos)
              k_lit(children.head).
                substr(k_lit(children.apply(1)), k_lit(Int.MaxValue))
            } else {
              throw new UnsupportedOperationException(
                s"substring must provide three or two parameters under sparder")
            }
          case "initcapb" =>
            initcap(k_lit(children.head))
          case "instr" =>
            val instr =
              if (children.length == 2) 1
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.apply(1)).expr, k_lit(children.head).expr, lit(instr).expr)) //instr(str,substr,start)
          case "length" =>
            length(k_lit(children.head))
          case "strpos" =>
            val pos =
              if (children.length == 2) 1
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.apply(1)).expr, k_lit(children.head).expr, lit(pos).expr)) //strpos(str,substr,start)
          case "position" =>
            val pos =
              if (children.length == 2) 0
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.head).expr, k_lit(children.apply(1)).expr, lit(pos).expr)) //position(substr,str,start)
          case "concat" =>
            concat(k_lit(children.head), k_lit(children.apply(1)))
          // time_funcs
          case "current_date" =>
            k_lit(
              DateTimeUtils.dateToString(
                DateTimeUtils.millisToDays(System.currentTimeMillis())))
          case "current_timestamp" =>
            current_timestamp()
          case "power" =>
            pow(k_lit(children.head), k_lit(children.apply(1)))
          case "log10" =>
            log10(k_lit(children.head))
          case "ln" =>
            log(Math.E, k_lit(children.head))
          case "exp" =>
            exp(k_lit(children.head))
          case "acos" =>
            acos(k_lit(children.head))
          case "asin" =>
            asin(k_lit(children.head))
          case "atan" =>
            atan(k_lit(children.head))
          case "atan2" =>
            assert(children.size == 2)
            atan2(k_lit(children.head), k_lit(children.last))
          case "cos" =>
            cos(k_lit(children.head))
          case "degrees" =>
            degrees(k_lit(children.head))
          case "radians" =>
            radians(k_lit(children.head))
          case "sign" =>
            signum(k_lit(children.head))
          case "tan" =>
            tan(k_lit(children.head))
          case "sin" =>
            sin(k_lit(children.head))
          case "initcap" =>
            initcap(k_lit(children.head))
          case "pi" =>
            k_lit(Math.PI)
          case "regexp_like"=>
            k_like(k_lit(children.head),k_lit(children.apply(1)))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        ceil(k_lit(children.head))
      case FLOOR =>
        floor(k_lit(children.head))
      case ARRAY_VALUE_CONSTRUCTOR =>
        array(children.map(child => k_lit(child.toString)): _*)
      case unsupportedFunc =>
        throw new UnsupportedOperationException(unsupportedFunc.toString)
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

   case class MonthNum(num: Column)

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
            MonthNum(k_lit(literal.getValue.asInstanceOf[BigDecimal].intValue)))
        }
        if (literal.getType.getFamily
              .asInstanceOf[SqlTypeFamily] == SqlTypeFamily.INTERVAL_DAY_TIME) {
          return Some(
            SparderTypeUtil.toSparkTimestamp(
              new java.math.BigDecimal(literal.getValue.toString).longValue()))
        }
      }

      case literalSql: BasicSqlType => {
        literalSql.getSqlTypeName match {
          case SqlTypeName.DATE =>
            return Some(DateTimeUtils.stringToTime(literal.toString))
          case SqlTypeName.TIMESTAMP =>
            return Some(DateTimeUtils.toJavaTimestamp(DateTimeUtils.stringToTimestamp(UTF8String.fromString(literal.toString)).head))
          case _ =>
        }
      }

      case _ =>
    }
    val ret = SparderTypeUtil.getValueFromRexLit(literal)
    Some(ret)
  }

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
