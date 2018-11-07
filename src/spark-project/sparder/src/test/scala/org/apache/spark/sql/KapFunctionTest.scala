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

import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
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
}
