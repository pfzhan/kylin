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

object StreamingTestConstant {
  val KAP_SSB_STREAMING_TABLE = "../examples/test_metadata/data/SSB.P_LINEORDER.csv"
  val KAFKA_QPS = 500

  val High_KAFKA_QPS = 10000

  val CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/ssb"
  val INTERVAL = 1000
  val PROJECT = "streaming_test"
  val DATAFLOW_ID = "511a9163-7888-4a60-aa24-ae735937cc87"
  val SQL_FOLDER = "../spark-project/spark-it/src/test/resources/ssb"

  val COUNTDISTINCT_DATAFLOWID = "511a9163-7888-4a60-aa24-ae735937cc88"
  val COUNTDISTINCT_SQL_FOLDER = "../spark-project/spark-it/src/test/resources/count_distinct"
  val COUNTDISTINCT_CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/count_distinct"

  val MERGE_DATAFLOWID = "511a9163-7888-4a60-aa24-ae735937cc89"
  val MERGE_CHECKPOINT_LOCATION = "hdfs://localhost:8020/spark/checkpoint/merge"

  val BATCH_ROUNDS = 5
}
