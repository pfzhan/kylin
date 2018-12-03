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

package io.kyligence.kap.common

import io.netty.util.internal.ThrowableUtil
import org.apache.spark.sql.common.SparderQueryTest
import org.scalatest.Suite

trait CompareSupport extends QuerySupport {
  self: Suite =>

  def runAndCompare(querySql: String,
                    sparkSql: String,
                    project: String,
                    errorMsg: String): String = {
    try {
      val maybeString = SparderQueryTest.checkAnswer(
        sql(sparkSql),
        singleQuery(querySql, project))
      if (maybeString != null) {
        val queryErrorMsg = errorMsg + maybeString
        if ("true".equals(System.getProperty("Failfast"))) {
          throw new RuntimeException(queryErrorMsg)
        }
        queryErrorMsg
      } else {
        null
      }
    } catch {
      case exception: Exception =>
        if ("true".equals(System.getProperty("Failfast"))) {
          throw exception
        } else {
          errorMsg + "\n" + ThrowableUtil.stackTraceToString(exception)
        }
    }
  }
}
