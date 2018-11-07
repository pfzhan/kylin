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

package org.apache.spark.sql.catalyst.util

import org.apache.calcite.avatica.util.TimeUnitRange

object KapDateTimeUtils {
  val MILLIS_PER_DAY = 86400000L

  def addMonths(timestamp: Long, m: Int): Long = {
    var ms = timestamp * 1000
    val millis = floorMod(ms, MILLIS_PER_DAY)
    ms -= millis
    val x = dateAddMonths((ms / MILLIS_PER_DAY).toInt, m)
    (x * MILLIS_PER_DAY + millis) * 1000
  }

  /** Finds the number of months between two dates, each represented as the
    * number of days since the epoch. */
  def dateSubtractMonths(date0: Int, date1: Int): Int = {
    if (date0 < date1) return -dateSubtractMonths(date1, date0)
    // Start with an estimate.
    // Since no month has more than 31 days, the estimate is <= the true value.
    var m = (date0 - date1) / 31

    while (true) {
      val date2 = dateAddMonths(date1, m)
      if (date2 >= date0) return m
      val date3 = dateAddMonths(date1, m + 1)
      if (date3 > date0) return m

      m += 1
    }

    // will never reach here
    -1
  }

  def dayOfWeek(date: Int): Int = {
    (date + 4) % 7 + 1
  }


  /**
    * Add date and year-month interval.
    * Returns a date value, expressed in days since 1.1.1970.
    */
  def dateAddMonths(date: Int, m: Int): Int = {
    var y0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.YEAR, date)
      .toInt
    var m0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.MONTH, date)
      .toInt
    var d0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.DAY, date)
      .toInt

    val y = (m + m0) / 12
    y0 += y
    m0 = m + m0 - y * 12
    if (m0 <= 0) {
      m0 += 12
      assert(m0 > 0)
      y0 -= 1
    }
    val last = lastDay(y0, m0)
    if (d0 > last) d0 = last

    org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate(y0, m0, d0)
  }

  def floorDiv(x: Long, y: Long): Long = {
    var r = x / y
    if ((x ^ y) < 0L && r * y != x) r -= 1
    r
  }

  def floorMod(x: Long, y: Long): Long = x - floorDiv(x, y) * y

  private def lastDay(y: Int, m: Int) = m match {
    case 2 =>
      if (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) 29
      else 28
    case 4 => 30
    case 6 => 30
    case 9 => 30
    case 11 => 30
    case _ => 31
  }
}
