/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.streaming

import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.{ObjectUtils, StringUtils}
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.exceptions.DataIncompatibleException
import org.apache.kylin.guava30.shaded.common.base.{Preconditions, Throwables}
import org.apache.kylin.parser.AbstractDataParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.Objects
import scala.collection.JavaConverters._

class PartitionRowIterator(iter: Iterator[Row],
                           parsedSchema: StructType,
                           partitionColumn: String,
                           dataParser: AbstractDataParser[ByteBuffer, java.util.Map[String, AnyRef]],
                           interruptible: Boolean = false,
                           keyNormalizer: String => String = identity) extends Iterator[Row] {
  private val logger = LoggerFactory.getLogger(classOf[PartitionRowIterator])

  private val EMPTY_ROW = Row()

  private val DATE_PATTERN = Array[String](DateFormat.COMPACT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS)

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Row = {
    val input = iter.next.get(0)
    if (Objects.isNull(input) || StringUtils.isEmpty(input.toString)) {
      logger.error(s"input data is null or length is 0, returning empty row. line is '$input'")
      return EMPTY_ROW
    }

    try {
      parseToRow(input.toString)
    } catch {
      case e: Exception =>
        logger.error(s"parse data failed, line is: '$input'", Throwables.getRootCause(e))
        if (interruptible) {
          throw e
        } else {
          EMPTY_ROW
        }
    }
  }

  def parseToRow(input: String): Row = {
    val jsonMapOpt: Option[Map[String, AnyRef]] =
      Option(dataParser.process(StandardCharsets.UTF_8.encode(input)).orElse(java.util.Collections.emptyMap()))
        .map(_.asScala)
        .map(_.map(pair => (keyNormalizer.apply(pair._1), pair._2)).toMap)

    Row(parsedSchema.fields.map {field => {
      jsonMapOpt.map(jsonMap => parseValue(jsonMap.getOrElse(field.name, null), field.name, field)).orNull
    }}: _*)
  }

  def parseValue(value: AnyRef, formattedFieldName: String, field: StructField): Any = {
    if (shouldReturnNull(value, field.dataType, field.nullable)) {
      return null
    }

    val strValue = String.valueOf(value)
    try {
      field.dataType match {
        case StringType =>
          if (value == null && !field.nullable) {
            throw new RuntimeException(s"Field $formattedFieldName is not nullable, but met null value!")
          }
          strValue
        case ShortType => lang.Short.parseShort(strValue)
        case IntegerType => Integer.parseInt(strValue)
        case LongType => lang.Long.parseLong(strValue)
        case DoubleType => lang.Double.parseDouble(strValue)
        case FloatType => lang.Float.parseFloat(strValue)
        case BooleanType =>
          if (value == null) throw new RuntimeException(s"Field $field can't be a null value!")
          lang.Boolean.parseBoolean(strValue)
        case TimestampType => processTimestamp(formattedFieldName, strValue)
        case DateType => new Date(DateUtils.parseDate(strValue, DATE_PATTERN).getTime)
        case DecimalType() => BigDecimal(strValue)
        case _ => value
      }
    } catch {
      case e: Exception =>
        throw new DataIncompatibleException(s"The input value [$strValue] can't be parsed as ${field.toString()}!", e)
    }
  }

  private def shouldReturnNull(value: AnyRef, dataType: DataType, nullable: Boolean): Boolean = {
    value match {
      case cs: CharSequence =>
        if (dataType != StringType && nullable && (cs.length() == 0 || StringUtils.equalsIgnoreCase(cs, "null"))) {
          true
        } else {
          false
        }
      case _ =>
        nullable && ObjectUtils.isEmpty(value)
    }
  }

  def processTimestamp(colName: String, value: String): Timestamp = {
    val timestamp = DateUtils.parseDate(value, DATE_PATTERN).getTime
    if (colName.equalsIgnoreCase(partitionColumn)) {
      Preconditions.checkArgument(timestamp >= 0, "invalid value %s", value)
    }
    new Timestamp(timestamp)
  }
}
