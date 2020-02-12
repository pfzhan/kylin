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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

trait StreamingJobSupport  extends Eventually {
    def waitQueryReady(): StreamExecution = {
      eventually(timeout(200.second), interval(200.milliseconds)) {
        assert(SparkSession.getActiveSession.isDefined)
      }

      eventually(timeout(200.second), interval(200.milliseconds)) {
        val buildSession = SparkSession.getActiveSession.get
        val query = buildSession.sessionState.streamingQueryManager
        assert(query.active.nonEmpty)
      }

      eventually(timeout(200.second), interval(200.milliseconds)) {
        val buildSession = SparkSession.getActiveSession.get
        val activeQuery = buildSession.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
        assert(activeQuery.lastProgress.sources.nonEmpty)
      }

      val buildSession = SparkSession.getActiveSession.get
      val activeQuery = buildSession.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
      activeQuery
    }
    def waitQueryStop(stream: StreamExecution): Unit = {
        stream.stop()
    }
}
