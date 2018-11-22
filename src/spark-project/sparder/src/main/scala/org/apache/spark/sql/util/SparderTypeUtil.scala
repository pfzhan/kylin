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

import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

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
      case tp if tp.startsWith("top") => ArrayType(ArrayType(ByteType))
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
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case _ => throw new IllegalArgumentException
    }
  }

  // scalastyle:off
  def convertSqlTypeNameToSparkType(sqlTypeName: SqlTypeName): String = {
    sqlTypeName match {
      case SqlTypeName.DECIMAL => "decimal"
      case SqlTypeName.CHAR => "string"
      case SqlTypeName.VARCHAR => "string"
      case SqlTypeName.INTEGER => "int"
      case SqlTypeName.TINYINT => "byte"
      case SqlTypeName.SMALLINT => "short"
      case SqlTypeName.BIGINT => "long"
      case SqlTypeName.FLOAT => "float"
      case SqlTypeName.DOUBLE => "double"
      case SqlTypeName.DATE => "date"
      case SqlTypeName.TIMESTAMP => "timestamp"
      case SqlTypeName.BOOLEAN => "boolean"
      case _ =>
        throw new IllegalArgumentException(s"unsupported SqlTypeName $sqlTypeName")
    }
  }

  // scalastyle:off
  def kylinCubeDataTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case "decimal" => DecimalType(29, 4)
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
      case tp if tp.startsWith("varchar") => StringType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case tp if tp.startsWith("hllc") => BinaryType
      case tp if tp.startsWith("topn") => BinaryType
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
      case _ =>
        throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
    }
  }

  def convertSparkTypeToSqlType(dt: org.apache.spark.sql.types.DataType): SqlTypeName = {
    dt match {
      case StringType => SqlTypeName.VARCHAR
      case IntegerType => SqlTypeName.INTEGER
      case ByteType => SqlTypeName.TINYINT
      case ShortType => SqlTypeName.SMALLINT
      case LongType => SqlTypeName.BIGINT
      case FloatType => SqlTypeName.FLOAT
      case DoubleType => SqlTypeName.DOUBLE
      case DateType => SqlTypeName.DATE
      case TimestampType => SqlTypeName.TIMESTAMP
      case BooleanType => SqlTypeName.BOOLEAN
      case _ => {
        if (dt.isInstanceOf[DecimalType]) {
          SqlTypeName.DECIMAL
        } else {
          throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
        }
      }
    }
  }

  // scalastyle:off
  def convertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    val sqlTypeName = rowType.getSqlTypeName
    if (s == null) {
      null
    } else if (s.toString.isEmpty) {
      val a: Any = sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(0)
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => 0
        case SqlTypeName.TINYINT => 0.toByte
        case SqlTypeName.SMALLINT => 0.toShort
        case SqlTypeName.BIGINT => 0L
        case SqlTypeName.FLOAT => 0f
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
        a
      } catch {
        case th: Throwable =>
          logError(s"Error for convert value : $s , class: ${s.getClass}", th)
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

}
