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
package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions.{REPLICA_SPLIT_CHAR, SPLIT_CHAR}

case class ShardOptions(sharding: String) {
  val replicaShards: Array[Array[String]] = sharding
    .split(REPLICA_SPLIT_CHAR)
    .map(_.split("\\" + SPLIT_CHAR))

  //  val shards: Array[String] = sharding.split(ShardOptions.SPLIT_CHAR)
  val shards: Array[String] = replicaShards(0)

}

object ShardOptions {
  val SHARD_URLS = "shard_urls"
  val PUSHDOWN_AGGREGATE = "pushDownAggregate"
  val PUSHDOWN_LIMIT = "pushDownLimit"
  val PUSHDOWN_OFFSET = "pushDownOffset"
  val PUSHDOWN_NUM_PARTITIONS = "numPartitions"
  val SPLIT_CHAR = "<url_split>"
  val REPLICA_SPLIT_CHAR = "<replica_split>"

  def create(options: JDBCOptions): ShardOptions = {
    ShardOptions(options.parameters.get(SHARD_URLS).getOrElse(options.url))
  }

  def buildSharding(urls: String*): String = {
    urls.mkString(SPLIT_CHAR.toString)
  }

  def buildReplicaSharding(urls: Array[Array[String]]): String = {
    urls.map(_.mkString(SPLIT_CHAR.toString)).mkString(REPLICA_SPLIT_CHAR)
  }
}