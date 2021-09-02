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

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.read.sqlpushdown.SupportsSQL
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.SQLException
import scala.collection.JavaConverters._

class ShardJDBCTableCatalog extends  JDBCTableCatalog with SupportsSQL {
  private val DEFAULT_CLICKHOUSE_URL = "jdbc:clickhouse://localhost:9000"

  // TODO: remove these two variables
  protected var options2: JDBCOptions = _
  protected var dialect2: JdbcDialect = _

  protected def checkNamespace(namespace: Array[String]): Unit = {
    // In JDBC there is no nested database/schema
    if (namespace.length > 1) {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // when url is empty, use default url
    val urlKey = "url"
    var modifiedOptions = options.asCaseSensitiveMap().asScala.toMap
    if (!options.containsKey(urlKey)) {
      modifiedOptions += (urlKey -> DEFAULT_CLICKHOUSE_URL)
    }
    val newOptions = new CaseInsensitiveStringMap(modifiedOptions.asJava)
    super.initialize(name, newOptions)

    val map = newOptions.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options2 = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid_dbtable"))
    dialect2 = JdbcDialects.get(this.options2.url)
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    try {
      val name = getTableName(ident)
      val optionsWithTableName = new JDBCOptions(
        options2.parameters + (JDBCOptions.JDBC_TABLE_NAME -> name))
      val schema = resolveTable(ident).getOrElse(defaultResolveTable(optionsWithTableName))
      ShardJDBCTable(ident, schema, optionsWithTableName)
    } catch {
      case _: SQLException => throw new NoSuchTableException(ident)
    }
  }

  protected def resolveTable(ident: Identifier): Option[StructType] = {
    None
  }

  def defaultResolveTable(options: JDBCOptions): StructType = {
    JDBCRDD.resolveTable(options)
  }

  protected def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect2.quoteIdentifier).mkString(".")
  }

}
