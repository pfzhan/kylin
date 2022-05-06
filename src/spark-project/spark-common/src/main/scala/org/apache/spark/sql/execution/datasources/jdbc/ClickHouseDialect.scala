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
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, Timestamp, Types}
import java.util.Locale
import java.text.SimpleDateFormat

import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter
import scala.util.matching.Regex

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}

// TODO: move to clickhouse module
object ClickHouseDialect extends JdbcDialect with Logging {

  private[jdbc] val arrayTypePattern: Regex = "^Array\\((.*)\\)$".r
  private[jdbc] val dateTypePattern: Regex = "^[dD][aA][tT][eE]$".r
  private[jdbc] val dateTimeTypePattern: Regex = "^[dD][aA][tT][eE][tT][iI][mM][eE](64)?(\\((.*)\\))?$".r
  private[jdbc] val decimalTypePattern: Regex = "^[dD][eE][cC][iI][mM][aA][lL]\\((\\d+),\\s*(\\d+)\\)$".r
  private[jdbc] val decimalTypePattern2: Regex = "^[dD][eE][cC][iI][mM][aA][lL](32|64|128|256)\\((\\d+)\\)$".r
  private[jdbc] val enumTypePattern: Regex = "^Enum(8|16)$".r
  private[jdbc] val fixedStringTypePattern: Regex = "^FixedString\\((\\d+)\\)$".r
  private[jdbc] val nullableTypePattern: Regex = "^Nullable\\((.*)\\)".r

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:clickhouse")

  /**
   * Inferred schema always nullable.
   * see [[JDBCRDD.resolveTable(JDBCOptions)]]
   */
  override def getCatalystType(sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    val scale = md.build.getLong("scale").toInt
    logDebug(s"sqlType: $sqlType, typeName: $typeName, precision: $size, scale: $scale")
    sqlType match {
      case Types.ARRAY =>
        unwrapNullable(typeName) match {
          case (_, arrayTypePattern(nestType)) =>
            toCatalystType(nestType).map { case (nullable, dataType) => ArrayType(dataType, nullable) }
          case _ => None
        }
      case _ => toCatalystType(typeName).map(_._2)
    }
  }

  // Spark use a widening conversion both ways.
  // see https://github.com/apache/spark/pull/26301#discussion_r347725332
  private[jdbc] def toCatalystType(typeName: String): Option[(Boolean, DataType)] = {
    val (nullable, _typeName) = unwrapNullable(typeName)
    val dataType = _typeName match {
      case "String" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) => Some(StringType)
      case "Int8" => Some(ByteType)
      case "UInt8" | "Int16" => Some(ShortType)
      case "UInt16" | "Int32" => Some(IntegerType)
      case "UInt32" | "Int64" | "UInt64" | "IPv4" => Some(LongType) // UInt64 is not fully support
      case "Int128" | "Int256" | "UInt256" => None // not support
      case "Float32" => Some(FloatType)
      case "Float64" => Some(DoubleType)
      case dateTypePattern() => Some(DateType)
      case dateTimeTypePattern() => Some(TimestampType)
      case decimalTypePattern(precision, scale) => Some(DecimalType(precision.toInt, scale.toInt))
      case decimalTypePattern2(w, scale) => w match {
        case "32" => Some(DecimalType(9, scale.toInt))
        case "64" => Some(DecimalType(18, scale.toInt))
        case "128" => Some(DecimalType(38, scale.toInt))
        case "256" => Some(DecimalType(76, scale.toInt)) // throw exception, spark support precision up to 38
      }
      case _ => None
    }
    dataType.map((nullable, _))
  }

  private[jdbc] def unwrapNullable(maybeNullableTypeName: String): (Boolean, String) = maybeNullableTypeName match {
    case nullableTypePattern(typeName) => (true, typeName)
    case _ => (false, maybeNullableTypeName)
  }

  // NOT recommend auto create ClickHouse table by Spark JDBC, the reason is it's hard to handle nullable because
  // ClickHouse use `T` to represent ANSI SQL `T NOT NULL` and  `Nullable(T)` to represent ANSI SQL `T NULL`,
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("String", Types.VARCHAR))
    // ClickHouse doesn't have the concept of encodings. Strings can contain an arbitrary set of bytes,
    // which are stored and output as-is.
    // See detail at https://clickhouse.tech/docs/en/sql-reference/data-types/string/
    case BinaryType => Some(JdbcType("String", Types.BINARY))
    case BooleanType => Some(JdbcType("UInt8", Types.BOOLEAN))
    case ByteType => Some(JdbcType("Int8", Types.TINYINT))
    case ShortType => Some(JdbcType("Int16", Types.SMALLINT))
    case IntegerType => Some(JdbcType("Int32", Types.INTEGER))
    case LongType => Some(JdbcType("Int64", Types.BIGINT))
    case FloatType => Some(JdbcType("Float32", Types.FLOAT))
    case DoubleType => Some(JdbcType("Float64", Types.DOUBLE))
    case t: DecimalType => Some(JdbcType(s"Decimal(${t.precision},${t.scale})", Types.DECIMAL))
    case DateType => Some(JdbcType("Date", Types.DATE))
    case TimestampType => Some(JdbcType("DateTime", Types.TIMESTAMP))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] =>
      getJDBCType(et)
        .orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jdbcType => JdbcType(s"Array(${jdbcType.databaseTypeDefinition})", Types.ARRAY))
    case _ => None
  }

  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    val shards = ShardOptions.create(options)
    val driverClass: String = options.driverClass
    (partitionId: Int) => {
      require(partitionId < shards.shards.length)
      val url = if (partitionId == -1) {
        shards.shards.apply(0)
      } else {
        shards.shards.apply(partitionId)
      }
      logInfo(s"Create connection for shard: $url")
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case wrapper: DriverWrapper if wrapper.wrapped.getClass.getCanonicalName == driverClass =>
          wrapper
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(url, options.asConnectionProperties)
    }
  }

  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def isCascadingTruncateTable: Option[Boolean] = Some(false)

  override def compileValue(value: Any): Any = value match {
    case ts: Timestamp =>
      "'" + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.getDefault(Locale.Category.FORMAT)).format(ts) + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("[", ",", "]")
    case _ => super.compileValue(value)
  }

  override def getSchemaQuery(table: String): String = {
    s"SELECT * FROM $table limit 0"
  }
}