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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.sqlpushdown.{SQLStatement, SupportsSQLPushDown}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, ShardJDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.{OrderDesc, SQLBuilder, SingleCatalystStatement, SingleSQLStatement}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class ShardJDBCScanBuilder(
    session: SparkSession,
    schema: StructType,
    jdbcOptions: JDBCOptions)
  extends ScanBuilder with SupportsSQLPushDown {

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedFilter = Array.empty[Filter]

  private var statement: SingleSQLStatement = _

  private var prunedSchema = schema

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      val (pushed, unSupported) = filters.partition(JDBCRDD.compileFilter(_, dialect).isDefined)
      this.pushedFilter = pushed
      unSupported
    } else {
      filters
    }
  }

  def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // JDBC doesn't support nested column pruning.
    // TODO (SPARK-32593): JDBC support nested column and nested column pruning.
    val requiredCols = requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive))
      .toSet
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredCols.contains(colName)
    }
    prunedSchema = StructType(fields)
  }

  override def build(): Scan = {
    val relationSchema = if (statement != null) {
      prunedSchema
    } else {
      schema
    }
    val relation = ShardJDBCRelation(session, relationSchema, jdbcOptions)
    ShardJDBCScan(relation, prunedSchema, pushedFilter, statement)
  }


  private def toSQLStatement(catalystStatement: SingleCatalystStatement): SingleSQLStatement = {
    val projects = catalystStatement.projects
    val filters = catalystStatement.filters
    val groupBy = catalystStatement.groupBy
    val orders = catalystStatement.orders
    SingleSQLStatement (
      relation = jdbcOptions.tableOrQuery,
      projects = if (projects.isEmpty) None else Some(projects.map(SQLBuilder.expressionToSql(_))),
      filters = if (filters.isEmpty) None else Some(filters),
      groupBy = if (groupBy.isEmpty) None else Some(groupBy.map(SQLBuilder.expressionToSql(_))),
      orders = orders.map {order => OrderDesc(SQLBuilder.expressionToSql(order.child),
        order.direction.sql, order.nullOrdering.sql)},
      url = Some(jdbcOptions.url)
    )
  }

  override def isMultiplePartitionExecution: Boolean = true

  override def pushStatement(push: SQLStatement, outputSchema: StructType): Array[Filter] = {
    statement = toSQLStatement(push.asInstanceOf[SingleCatalystStatement])
    if (outputSchema != null) {
      prunedSchema = outputSchema
    }
    statement.filters.map(f => pushFilters(f.toArray)).getOrElse(Array.empty)
  }

  override def pushedStatement(): SQLStatement = statement
}
