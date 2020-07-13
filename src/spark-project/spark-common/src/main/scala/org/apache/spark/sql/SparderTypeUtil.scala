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

package org.apache.spark.sql.util

import java.lang.{Boolean, Byte, Double, Float, Long, Short}
import java.math.BigDecimal
import java.sql.{Date, Timestamp, Types}
import java.util.regex.Pattern
import java.util.{GregorianCalendar, TimeZone}

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.NlsString
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

object SparderTypeUtil extends Logging {
  val DATETIME_FAMILY = List("time", "date", "timestamp", "datetime")

  def isDateTimeFamilyType(dataType: String): Boolean = {
    DATETIME_FAMILY.contains(dataType.toLowerCase())
  }

  def isDateType(dataType: String): Boolean = {
    "date".equalsIgnoreCase(dataType)
  }

  def isDateTime(sqlTypeName: SqlTypeName): Boolean = {
    SqlTypeName.DATETIME_TYPES.contains(sqlTypeName)
  }

  // scalastyle:off
  def kylinTypeToSparkResultType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case tp if tp.startsWith("hllc") => LongType
      case tp if tp.startsWith("percentile") => DoubleType
      case tp if tp.startsWith("bitmap") => LongType
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => IntegerType
      case "time" => LongType
      case "timestamp" => LongType
      case "datetime" => LongType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case _ => throw new IllegalArgumentException
    }
  }

  // scalastyle:off
  def toSparkType(dataTp: DataType, isSum: Boolean = false): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      // org.apache.spark.sql.catalyst.expressions.aggregate.Sum#resultType
      case "decimal" =>
        if (isSum) {
          val i = dataTp.getPrecision + 10
          DecimalType(Math.min(DecimalType.MAX_PRECISION, i), dataTp.getScale)
        }
        else DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => if (isSum) LongType else ByteType
      case "smallint" => if (isSum) LongType else ShortType
      case "integer" => if (isSum) LongType else IntegerType
      case "int4" => if (isSum) LongType else IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => if (isSum) DoubleType else FloatType
      case "double" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case tp if tp.startsWith("hllc") => BinaryType
      case tp if tp.startsWith("bitmap") => BinaryType
      case tp if tp.startsWith("extendedcolumn") => BinaryType
      case tp if tp.startsWith("percentile") => BinaryType
      case tp if tp.startsWith("raw") => BinaryType
      case _ => throw new IllegalArgumentException(dataTp.toString)
    }
  }

  def convertSqlTypeToSparkType(dt: RelDataType): org.apache.spark.sql.types.DataType = {
    dt.getSqlTypeName match {
      case SqlTypeName.DECIMAL => DecimalType(dt.getPrecision, dt.getScale)
      case SqlTypeName.CHAR => StringType
      case SqlTypeName.VARCHAR => StringType
      case SqlTypeName.INTEGER => IntegerType
      case SqlTypeName.TINYINT => ByteType
      case SqlTypeName.SMALLINT => ShortType
      case SqlTypeName.BIGINT => LongType
      case SqlTypeName.FLOAT => FloatType
      case SqlTypeName.DOUBLE => DoubleType
      case SqlTypeName.DATE => DateType
      case SqlTypeName.TIMESTAMP => TimestampType
      case SqlTypeName.BOOLEAN => BooleanType
      case SqlTypeName.ANY => StringType
      case _ =>
        throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
    }
  }

  def convertSparkTypeToSqlType(dt: org.apache.spark.sql.types.DataType): String = {
    dt match {
      case StringType => SqlTypeName.VARCHAR.getName
      case IntegerType => SqlTypeName.INTEGER.getName
      case ByteType => SqlTypeName.TINYINT.getName
      case ShortType => SqlTypeName.SMALLINT.getName
      case LongType => SqlTypeName.BIGINT.getName
      case FloatType => SqlTypeName.FLOAT.getName
      case DoubleType => SqlTypeName.DOUBLE.getName
      case DateType => SqlTypeName.DATE.getName
      case TimestampType => SqlTypeName.TIMESTAMP.getName
      case BooleanType => SqlTypeName.BOOLEAN.getName
      case _ => {
        if (dt.isInstanceOf[DecimalType]) {
          val decimalType = dt.asInstanceOf[DecimalType]
          SqlTypeName.DECIMAL.getName + "(" + decimalType.precision + "," + decimalType.scale + ")"
        } else {
          throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
        }
      }
    }
  }

  def getValueFromNlsString(s: NlsString): String = {
    if (!KylinConfig.getInstanceFromEnv.isQueryEscapedLiteral) {
      val ret = new StringBuilder
      ret.append("'")
      ret.append(s.getValue.replace("'", "\\\'"))
      ret.append("'")
      val res = ret.toString
      ParserUtils.unescapeSQLString(res)
    } else {
      s.getValue
    }
  }

  def getValueFromRexLit(literal: RexLiteral) = {
    val ret = literal.getValue match {
      case s: NlsString =>
        getValueFromNlsString(s)
      case g: GregorianCalendar =>
        if (literal.getTypeName.getName.equals("DATE")) {
          new Date(DateTimeUtils.stringToTimestamp(UTF8String.fromString(literal.toString)).get / 1000)
        } else {
          new Timestamp(DateTimeUtils.stringToTimestamp(UTF8String.fromString(literal.toString)).get / 1000)
        }
      case range: TimeUnitRange =>
        // Extract(x from y) in where clause
        range.name
      case b: Boolean =>
        b
      case b: BigDecimal =>
        literal.getType.getSqlTypeName match {
          case SqlTypeName.BIGINT =>
            b.longValue()
          case SqlTypeName.INTEGER =>
            b.intValue()
          case SqlTypeName.DOUBLE =>
            b.doubleValue()
          case SqlTypeName.FLOAT =>
            b.floatValue()
          case SqlTypeName.SMALLINT =>
            b.shortValue()
          case _ =>
            b
        }
      case b: Float =>
        b
      case b: Double =>
        b
      case b: Integer =>
        b
      case b: Byte =>
        b
      case b: Short =>
        b
      case b: Long =>
        b
      case _ =>
        literal.getValue.toString
    }
    ret
  }

  def convertToStringWithCalciteType(rawValue: Any, relType: RelDataType): String = (rawValue, relType.getSqlTypeName) match {
    case (null, _) => null
    // types that matched
    case (value: BigDecimal, SqlTypeName.DECIMAL) => value.toString
    case (value: Integer, SqlTypeName.INTEGER) => value.toString
    case (value: Byte, SqlTypeName.TINYINT) => value.toString
    case (value: Short, SqlTypeName.SMALLINT) => value.toString
    case (value: Long, SqlTypeName.BIGINT) => value.toString
    case (value: Float, SqlTypeName.FLOAT | SqlTypeName.REAL) => value.toString
    case (value: Double, SqlTypeName.DOUBLE) => value.toString
    case (value: java.sql.Date, SqlTypeName.DATE) => value.toString
    case (value: java.sql.Timestamp, SqlTypeName.TIMESTAMP) => DateFormat.castTimestampToString(value.getTime)
    case (value: java.sql.Time, SqlTypeName.TIME) => value.toString
    case (value: Boolean, SqlTypeName.BOOLEAN) => value.toString
    // handle cast to char/varchar
    case (value: java.sql.Timestamp, SqlTypeName.CHAR | SqlTypeName.VARCHAR) => DateFormat.castTimestampToString(value.getTime)
    case (value: java.sql.Date, SqlTypeName.CHAR | SqlTypeName.VARCHAR) => DateFormat.formatToDateStr(value.getTime)
    case (value, SqlTypeName.CHAR | SqlTypeName.VARCHAR) => value.toString
    // cast type to align with relType
    case (value: Any, SqlTypeName.DECIMAL) =>
      new java.math.BigDecimal(value.toString)
        .setScale(relType.getScale, BigDecimal.ROUND_HALF_EVEN)
        .toString
    case (value: Any, SqlTypeName.INTEGER | SqlTypeName.TINYINT | SqlTypeName.SMALLINT | SqlTypeName.BIGINT) =>
      value match {
        case number: Double => number.longValue().toString
        case number: Float => number.longValue().toString
        case number: BigDecimal => number.longValue().toString
        case other => new BigDecimal(other.toString).setScale(0, BigDecimal.ROUND_HALF_EVEN).longValue().toString
      }
    case (value: Any, SqlTypeName.FLOAT | SqlTypeName.REAL) => java.lang.Float.parseFloat(value.toString).toString
    case (value: Any, SqlTypeName.DOUBLE) => java.lang.Double.parseDouble(value.toString).toString
    case (value: Any, SqlTypeName.TIME) => value.toString
    case (value: Any, SqlTypeName.DATE | SqlTypeName.TIMESTAMP) =>
      val millis = DateFormat.stringToMillis(value.toString)
      if (relType.getSqlTypeName == SqlTypeName.TIMESTAMP) {
        DateFormat.castTimestampToString(millis)
      } else {
        DateFormat.formatToDateStr(new Date(millis).getTime)
      }
    // in case the type is not set
    case (ts: java.sql.Timestamp, _) => DateFormat.castTimestampToString(ts.getTime)
    case (dt: java.sql.Date, _) => DateFormat.formatToDateStr(dt.getTime)
    case (value: mutable.WrappedArray.ofRef[Any], _) =>
      s"[${value.array.mkString(",")}]"
    case (other, _) => other.toString
  }

  // scalastyle:off
  def convertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    val sqlTypeName = rowType.getSqlTypeName
    if (s == null) {
      null
    } else if (s.toString.isEmpty) {
      sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(0)
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => 0
        case SqlTypeName.TINYINT => 0.toByte
        case SqlTypeName.SMALLINT => 0.toShort
        case SqlTypeName.BIGINT => 0L
        case SqlTypeName.FLOAT => 0f
        case SqlTypeName.REAL => 0f
        case SqlTypeName.DOUBLE => 0d
        case SqlTypeName.DATE => 0
        case SqlTypeName.TIMESTAMP => 0L
        case SqlTypeName.TIME => 0L
        case SqlTypeName.BOOLEAN => null;
        case null => null
        case _ => null
      }
    } else {
      try {
        val a: Any = sqlTypeName match {
          case SqlTypeName.DECIMAL =>
            if (s.isInstanceOf[java.lang.Double] || s
              .isInstanceOf[java.lang.Float] || s.toString.contains(".")) {
              new java.math.BigDecimal(s.toString)
                .setScale(rowType.getScale, BigDecimal.ROUND_HALF_EVEN)
            } else {
              new java.math.BigDecimal(s.toString)
            }
          case SqlTypeName.CHAR => s.toString
          case SqlTypeName.VARCHAR => s.toString
          case SqlTypeName.INTEGER => s.toString.toInt
          case SqlTypeName.TINYINT => s.toString.toByte
          case SqlTypeName.SMALLINT => s.toString.toShort
          case SqlTypeName.BIGINT => s.toString.toLong
          case SqlTypeName.FLOAT => java.lang.Double.parseDouble(s.toString)
          case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
          case SqlTypeName.DATE => {
            // time over here is with timezone.
            val string = s.toString
            if (string.contains("-")) {
              val time = DateFormat.stringToDate(string).getTime
              if (toCalcite) {
                //current date is local timezone, org.apache.calcite.avatica.util.AbstractCursor.DateFromNumberAccessor need to utc
                DateTimeUtils.stringToDate(UTF8String.fromString(string)).get
              } else {
                // ms to s
                time / 1000
              }
            } else {
              // should not come to here?
              if (toCalcite) {
                (toCalciteTimestamp(DateFormat.stringToMillis(string)) / (3600 * 24 * 1000)).toInt
              } else {
                DateFormat.stringToMillis(string) / 1000
              }
            }
          }
          case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => {
            var ts = s.asInstanceOf[Timestamp].toString
            if (toCalcite) {
              // current ts is local timezone ,org.apache.calcite.avatica.util.AbstractCursor.TimeFromNumberAccessor need to utc
              DateTimeUtils.stringToTimestamp(UTF8String.fromString(ts), TimeZone.getTimeZone("UTC")).get / 1000
            } else {
              // ms to s
              s.asInstanceOf[Timestamp].getTime / 1000
            }
          }
          case SqlTypeName.BOOLEAN => s;
          case _ => s.toString
        }
        a
      } catch {
        case th: Throwable =>
          logWarning(s"""convertStringToValue failed: {"v": "${s}", "cls": "${s.getClass}", "type": "$sqlTypeName"}""")
          // fixme aron never come to here, for coverage ignore.
          safetyConvertStringToValue(s, rowType, toCalcite)
      }
    }
  }

  def kylinRawTableSQLTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("char") => StringType
      case tp if tp.startsWith("varchar") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  def safetyConvertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    try {
      rowType.getSqlTypeName match {
        case SqlTypeName.DECIMAL =>
          if (s.isInstanceOf[java.lang.Double] || s
            .isInstanceOf[java.lang.Float] || s.toString.contains(".")) {
            new java.math.BigDecimal(s.toString)
              .setScale(rowType.getScale, BigDecimal.ROUND_HALF_EVEN)
          } else {
            new java.math.BigDecimal(s.toString)
          }
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => s.toString.toDouble.toInt
        case SqlTypeName.TINYINT => s.toString.toDouble.toByte
        case SqlTypeName.SMALLINT => s.toString.toDouble.toShort
        case SqlTypeName.BIGINT => s.toString.toDouble.toLong
        case SqlTypeName.FLOAT => java.lang.Float.parseFloat(s.toString)
        case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
        case SqlTypeName.DATE => {
          // time over here is with timezone.
          val string = s.toString
          if (string.contains("-")) {
            val time = DateFormat.stringToDate(string).getTime
            if (toCalcite) {
              (time / (3600 * 24 * 1000)).toInt
            } else {
              // ms to s
              time / 1000
            }
          } else {
            // should not come to here?
            if (toCalcite) {
              (toCalciteTimestamp(DateFormat.stringToMillis(string)) / (3600 * 24 * 1000)).toInt
            } else {
              DateFormat.stringToMillis(string)
            }
          }
        }
        case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => {
          var ts = s.asInstanceOf[Timestamp].getTime
          if (toCalcite) {
            ts
          } else {
            // ms to s
            ts / 1000
          }
        }
        case SqlTypeName.BOOLEAN => s;
        case _ => s.toString
      }
    } catch {
      case th: Throwable =>
        throw new RuntimeException(s"Error for convert value : $s , class: ${s.getClass}", th)
    }
  }


  // ms to second
  def toSparkTimestamp(calciteTimestamp: Long): java.lang.Long = {
    calciteTimestamp / 1000
  }

  // ms to microsecond, spark need micro sec.
  //  def toSparkMicrosecond(calciteTimestamp: Long): java.lang.Long = {
  //    calciteTimestamp * 1000
  //  }

  // ms to day
  //  def toSparkDate(calciteTimestamp: Long): java.lang.Integer = {
  //    (calciteTimestamp / 1000 / 3600 / 24).toInt
  //  }

  def toCalciteTimestamp(sparkTimestamp: Long): Long = {
    sparkTimestamp * 1000
  }

  def alignDataType(origin: StructType, goal: StructType): Array[Column] = {
    val columns = origin.zip(goal).map {
      case (sparkField, goalField) =>
        val sparkDataType = sparkField.dataType
        val goalDataType = goalField.dataType
        if (!sparkDataType.sameType(goalDataType)) {
          if (Cast.canCast(sparkDataType, goalDataType)) {
            col(sparkField.name).cast(goalDataType)
          } else {
            logError(s"Error for cast datatype from  $sparkDataType to $goalDataType with column name is : ${sparkField.name}")
            col(sparkField.name)
          }
        } else {
          col(sparkField.name)
        }
    }.toArray
    logInfo(s"Align data type is ${columns.mkString(",")}")
    columns
  }

  def convertSparkFieldToJavaField(field: org.apache.spark.sql.types.StructField): io.kyligence.kap.metadata.query.StructField = {
    val builder = new io.kyligence.kap.metadata.query.StructField.StructFieldBuilder
    builder.setName(field.name)

    field.dataType match {
      case decimal: DecimalType =>
        builder.setPrecision(decimal.precision)
        builder.setScale(decimal.scale)
        builder.setDataType(Types.DECIMAL)
        builder.setDataTypeName(s"DECIMAL(${decimal.precision},${decimal.scale})")
      case BinaryType =>
        builder.setDataType(Types.BINARY)
        builder.setDataTypeName("BINARY")
      case BooleanType =>
        builder.setDataType(Types.BOOLEAN)
        builder.setDataTypeName("BOOLEAN")
      case DoubleType =>
        builder.setDataType(Types.DOUBLE)
        builder.setDataTypeName("DOUBLE")
      case FloatType =>
        builder.setDataType(Types.FLOAT)
        builder.setDataTypeName("FLOAT")
      case IntegerType =>
        builder.setDataType(Types.INTEGER)
        builder.setDataTypeName("INTEGER")
      case LongType =>
        builder.setDataType(Types.BIGINT)
        builder.setDataTypeName("BIGINT")
      case ShortType =>
        builder.setDataType(Types.SMALLINT)
        builder.setDataTypeName("SMALLINT")
      case ByteType =>
        builder.setDataType(Types.TINYINT)
        builder.setDataTypeName("TINYINT")
      case DateType =>
        builder.setDataType(Types.DATE)
        builder.setDataTypeName("DATE")
      case TimestampType =>
        builder.setDataType(Types.TIMESTAMP)
        builder.setDataTypeName("TIMESTAMP")
      case StringType =>
        builder.setDataType(Types.VARCHAR)
        builder.setDataTypeName("VARCHAR")
      case _ =>
        builder.setDataType(Types.OTHER)
        builder.setDataTypeName(field.dataType.sql)
    }

    builder.setNullable(field.nullable)
    builder.createStructField()
  }

  private def getDecimalPrecisionAndScale(javaType: String): (Int, Int) = {
    val DECIMAL_PATTERN = Pattern.compile("DECIMAL\\(([0-9]+),([0-9]+)\\)", Pattern.CASE_INSENSITIVE)
    val decimalMatcher = DECIMAL_PATTERN.matcher(javaType)
    if (decimalMatcher.find) {
      (Integer.valueOf(decimalMatcher.group(1)), Integer.valueOf(decimalMatcher.group(2)))
    }
    else null
  }
}
