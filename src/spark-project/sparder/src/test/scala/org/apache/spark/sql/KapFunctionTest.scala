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
package org.apache.spark.sql

import java.util.Calendar

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, KapDateTimeUtils}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

class KapFunctionsTest extends SparderBaseFunSuite with SharedSparkSession {

  test("kapDayOfWeekTest") {
    val cl = Calendar.getInstance
    cl.set(2013, 1, 9)
    val expected = cl.get(Calendar.DAY_OF_WEEK)
    val startTime = cl.getTimeInMillis
    cl.set(1970, 0, 1)
    val endTime = cl.getTimeInMillis
    Assert.assertEquals(expected,
      KapDateTimeUtils.dayOfWeek(((startTime - endTime) / (3600 * 1000 * 24)).toInt))
  }

  test("kapAddMonths") {
    Assert.assertEquals(KapDateTimeUtils.subtractMonths( 1585411200000L, 1582819200000L), 1)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths( 1585497600000L, 1582905600000L), 0)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths( 1585584000000L, 1582905600000L), 1)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths(  1593446400000L, 1585584000000L), 3)
    var time = 1420048800000L
    // 4 year,include leap year
    for (i <- 0 to 35040) {
      for ( j <- -12 to 12) {
        val now = DateTimeUtils.millisToDays(time)
        val d1 = org.apache.spark.sql.catalyst.util.DateTimeUtils.dateAddMonths(now, j)
        val d2 = KapDateTimeUtils.dateAddMonths(now, j)

        Assert.assertEquals(d1, d2)
      }
      // add hour
      time += 3600*1000L
    }
  }

}
