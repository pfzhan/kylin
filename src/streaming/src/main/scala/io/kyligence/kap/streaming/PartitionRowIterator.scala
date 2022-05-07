/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */
package io.kyligence.kap.streaming

import io.kyligence.kap.parser.AbstractDataParser
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.{ObjectUtils, StringUtils}
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionRowIterator(iter: Iterator[Row],
                           parsedSchema: StructType,
                           dateParser: AbstractDataParser[ByteBuffer]) extends Iterator[Row] {
  val logger = LoggerFactory.getLogger(classOf[PartitionRowIterator])

  val EMPTY_ROW = Row()

  val DATE_PATTERN = Array[String](DateFormat.COMPACT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS)

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Row = {
    val csvString = iter.next.get(0)
    if (csvString == null || StringUtils.isBlank(csvString.toString)) {
      EMPTY_ROW
    } else {
      try {
        convertJson2Row(csvString.toString)
      } catch {
        case e: Exception =>
          logger.error(s"custom parse data fail ${e.toString}\nStackTrace is: ${e.getStackTrace.toString}\nline is: $csvString")
          EMPTY_ROW
      }
    }
  }

  def convertJson2Row(input: String): Row = {
    val jsonMap: mutable.Map[String, AnyRef] = dateParser.process(StandardCharsets.UTF_8.encode(input)).asScala
      .map(pair => (pair._1.toLowerCase(Locale.ROOT), pair._2))

    Row(parsedSchema.fields.indices.map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      val value = jsonMap.getOrElse(colName, null)
      val dataType = parsedSchema.fields(index).dataType
      if (dataType == StringType) {
        value
      } else if (ObjectUtils.isEmpty(value)) {
        // key not exist -> null
        // value not exist ("", null, new int[]{}) -> null
        null
      } else {
        dataType match {
          case ShortType => lang.Short.parseShort(value.toString)
          case IntegerType => Integer.parseInt(value.toString)
          case LongType => lang.Long.parseLong(value.toString)
          case DoubleType => lang.Double.parseDouble(value.toString)
          case FloatType => lang.Float.parseFloat(value.toString)
          case BooleanType => lang.Boolean.parseBoolean(value.toString)
          case TimestampType => new Timestamp(DateUtils.parseDate(value.toString, DATE_PATTERN).getTime)
          case DateType => new Date(DateUtils.parseDate(value.toString, DATE_PATTERN).getTime)
          case DecimalType() => BigDecimal(value.toString)
          case _ => value
        }
      }
    }: _*)
  }
}
