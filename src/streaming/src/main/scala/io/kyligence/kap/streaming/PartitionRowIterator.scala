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

import com.google.gson.JsonParser
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionRowIterator(iter: Iterator[Row], parsedSchema: StructType) extends Iterator[Row] {
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
    val parser = new JsonParser()
    if (csvString == null || StringUtils.isEmpty(csvString.toString)) {
      EMPTY_ROW
    } else {
      try {
        convertJson2Row(csvString.toString(), parser)
      } catch {
        case e: Exception =>
          logger.error(s"parse json text fail ${e.toString}  stackTrace is: " +
            s"${e.getStackTrace.toString} line is: ${csvString}")
          EMPTY_ROW
      }
    }
  }

  def convertJson2Row(jsonStr: String, parser: JsonParser): Row = {
    val jsonMap = new mutable.HashMap[String, String]()
    val jsonObj = parser.parse(jsonStr).getAsJsonObject
    val entries = jsonObj.entrySet().asScala
    entries.foreach { entry =>
      jsonMap.put(entry.getKey.toLowerCase(Locale.ROOT), entry.getValue.getAsString)
    }
    Row((0 to parsedSchema.fields.length - 1).map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      if (!jsonMap.contains(colName)) { // key not exist
        null
      } else {
        val value = jsonMap.get(colName).getOrElse(null) // value not exist
        parsedSchema.fields(index).dataType match {
          case ShortType => if (value == null || value.equals("")) null else value.toShort
          case IntegerType => if (value == null || value.equals("")) null else value.toInt
          case LongType => if (value == null || value.equals("")) null else value.toLong
          case DoubleType => if (value == null || value.equals("")) null else value.toDouble
          case FloatType => if (value == null || value.equals("")) null else value.toFloat
          case BooleanType => if (value == null || value.equals("")) null else value.toBoolean
          case TimestampType => if (value == null || value.equals("")) null
          else new Timestamp(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DateType => if (value == null || value.equals("")) null
          else new Date(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DecimalType() => if (StringUtils.isEmpty(value)) null else BigDecimal(value)
          case _ => value
        }
      }
    }: _*)
  }

}