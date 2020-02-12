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
package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{Offset, StreamExecution, StreamingQueryWrapper}

import scala.collection.JavaConverters._

object OffsetRangeManager extends Logging {

  def getKafkaSourceOffset(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case (t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }

  def awaitOffset(currentStream: StreamExecution, sourceIndex: Int, offset: Offset, timeoutMs: Long): Unit = {
    logInfo("compare offset")
    currentStream.awaitOffset(sourceIndex, offset, timeoutMs)
  }

  def currentOffsetRange(ss: SparkSession): (String, String) = {
    assert(ss.sessionState.streamingQueryManager.active.length == 1, "support only one active streaming query")
    val activeQuery = ss.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
    val committedOffsets =
      if (activeQuery.committedOffsets.isEmpty) {
        "{}"
      } else {
        activeQuery.committedOffsets.values.head.json()
      }
    val availableOffsets = activeQuery.availableOffsets.values.head.json()
    return (committedOffsets, availableOffsets)
  }

  def partitionOffsets(str: String): java.util.Map[Integer, java.lang.Long] = {
    JsonUtils.partitionOffsets(str).map { kv =>
      (Int.box(kv._1.partition()), Long.box(kv._2))
    }.asJava
  }
}
