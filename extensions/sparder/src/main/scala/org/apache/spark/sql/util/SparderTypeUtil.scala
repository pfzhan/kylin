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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.types._

object SparderTypeUtil {

  def isDateTime(sqlTypeName: SqlTypeName): Boolean = {
    SqlTypeName.DATETIME_TYPES.contains(sqlTypeName)
  }

  def kylinTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case x if x.startsWith("hllc") => LongType
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
      case "varchar" => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case _ => throw new IllegalArgumentException
    }
  }

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
      case _ => throw new IllegalArgumentException(s"unsupported SqlTypeName $sqlTypeName")
    }
  }

  def convertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    val sqlTypeName = rowType.getSqlTypeName
    if (s == null) {
      val a: Any = sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(0)
        case SqlTypeName.CHAR => null
        case SqlTypeName.VARCHAR => null
        case SqlTypeName.INTEGER => 0
        case SqlTypeName.TINYINT => 0.toByte
        case SqlTypeName.SMALLINT => 0.toShort
        case SqlTypeName.BIGINT => 0L
        case SqlTypeName.FLOAT => 0f
        case SqlTypeName.DOUBLE => 0d
        case SqlTypeName.DATE => 0
        case SqlTypeName.TIMESTAMP => 0L
        case SqlTypeName.TIME => 0L
        case null => null
        case _ => null
      }
      a
    } else {
      val a: Any = sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(s.toString)
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => s.toString.toInt
        case SqlTypeName.TINYINT => s.toString.toByte
        case SqlTypeName.SMALLINT => s.toString.toShort
        case SqlTypeName.BIGINT => s.toString.toLong
        case SqlTypeName.FLOAT => java.lang.Float.parseFloat(s.toString)
        case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
        case SqlTypeName.DATE => {
          if (toCalcite)
            (DateFormat.stringToMillis(s.toString) / (1000 * 3600 * 24)).toInt
          else
            DateFormat.stringToMillis(s.toString)
        }
        case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => DateFormat.stringToMillis(s.toString)
        case _ => s.toString
      }
      a
    }
  }

}
