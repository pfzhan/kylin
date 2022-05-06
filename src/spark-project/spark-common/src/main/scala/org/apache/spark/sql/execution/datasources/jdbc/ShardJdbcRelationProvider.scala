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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation

class ShardJdbcRelationProvider extends JdbcRelationProvider {
  override def shortName(): String = "shard_jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val relation = super.createRelation(sqlContext, parameters)
    assert(relation.isInstanceOf[JDBCRelation])
    val jdbcRelation = relation.asInstanceOf[JDBCRelation]
    val shards = ShardOptions.create(jdbcRelation.jdbcOptions)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val parts = if (shards.shards.length == 1) {
      JDBCRelation.columnPartition(jdbcRelation.schema, resolver, timeZoneId, jdbcRelation.jdbcOptions)
    } else {
      ShardJDBCUtil.shardPartition(shards)
    }
    jdbcRelation.copy(parts = parts)(jdbcRelation.sparkSession)
  }
}


