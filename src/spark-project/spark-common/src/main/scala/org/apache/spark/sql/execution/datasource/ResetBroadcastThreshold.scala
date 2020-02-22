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
package org.apache.spark.sql.execution.datasource

import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait ResetBroadcastThreshold extends Logging {
  def setBroadcastThreshold(bytes: Long, sparkSession: SparkSession): Unit = {
    val cubeBroadcastThreshold = KylinConfig.getInstanceFromEnv.getCubeBroadcastThreshold
    if (bytes > cubeBroadcastThreshold) {
      logInfo(s"enable cube broadcast reason is cube size : ${bytes} is over threshold ${cubeBroadcastThreshold} ")
      sparkSession.sessionState.conf.setLocalProperty("spark.sql.autoBroadcastJoinThreshold", String.valueOf(11L * 1024 * 1024))
    }
  }
}
