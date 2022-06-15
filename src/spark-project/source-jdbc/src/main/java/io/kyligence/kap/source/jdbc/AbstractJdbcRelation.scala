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

package io.kyligence.kap.source.jdbc

import java.sql.{Connection, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.mutable

abstract class AbstractJdbcRelation(jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession) extends BaseRelation
  with PrunedFilteredScan with Logging {

  val KEY2OUTPUTS = new mutable.HashMap[CacheKey, WriteOutput]

  override lazy val schema: StructType = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val connection: Connection = dialect.createConnectionFactory(jdbcOptions)(-1)
    val schemaOption: Option[StructType] = getSchemaOption(connection, jdbcOptions)
    schemaOption.get
  }

  def getSchemaOption(conn: Connection, options: JDBCOptions): Option[StructType] = {
    val dialect = JdbcDialects.get(options.url)
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(options.tableOrQuery))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        Some(getSchema(statement.executeQuery(), dialect))
      } catch {
        case e: SQLException =>
          logError("Get schema sql throws error ", e)
          None
      } finally {
        statement.close()
      }
    } catch {
      case e: SQLException =>
        logError("Get schema sql throws error ", e)
        None
    }
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext


  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val cacheKey: CacheKey = CacheKey(requiredColumns, filters, jdbcOptions.tableOrQuery)
    var writeOutput: WriteOutput = null
    if (KEY2OUTPUTS.get(cacheKey).isDefined) {
      writeOutput = KEY2OUTPUTS(cacheKey)
      log.info("Using existing writeOutput {}", writeOutput)
    } else {
      writeOutput = writeToExternal(requiredColumns, filters)
    }
    KEY2OUTPUTS.put(CacheKey(requiredColumns, filters, jdbcOptions.tableOrQuery), writeOutput)
    val rdd: RDD[Row] = buildRDD(writeOutput)
    cleanUp()
    rdd
  }

  def writeToExternal(requiredColumns: Array[String], filters: Array[Filter]): WriteOutput

  def buildRDD(writeOutput: WriteOutput): RDD[Row]

  def cleanUp(): Unit = {
    // just implement it
  }

  case class CacheKey(requiredColumns: Array[String], filters: Array[Filter], sql: String)

  trait WriteOutput

}

