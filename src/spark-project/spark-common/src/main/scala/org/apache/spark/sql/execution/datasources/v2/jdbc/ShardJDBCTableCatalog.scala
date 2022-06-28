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

import java.sql.SQLException

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.read.sqlpushdown.SupportsSQL
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ShardJDBCTableCatalog extends JDBCTableCatalog with SupportsSQL {
  private val DEFAULT_CLICKHOUSE_URL = "jdbc:clickhouse://localhost:9000"

  private var options: JDBCOptions = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // when url is empty, use default url
    val urlKey = "url"
    var modifiedOptions = options.asCaseSensitiveMap().asScala.toMap
    if (!options.containsKey(urlKey)) {
      modifiedOptions += (urlKey -> DEFAULT_CLICKHOUSE_URL)
    }
    val newOptions = new CaseInsensitiveStringMap(modifiedOptions.asJava)
    super.initialize(name, newOptions)

    val optionsField = classOf[JDBCTableCatalog].getDeclaredField("options")
    optionsField.setAccessible(true)
    this.options = optionsField.get(this).asInstanceOf[JDBCOptions]
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = new JDBCOptions(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    try {
      val schema = resolveTable(ident).getOrElse(JDBCRDD.resolveTable(optionsWithTableName))
      new ShardJDBCTable(ident, schema, optionsWithTableName)
    } catch {
      case _: SQLException => throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }

  protected def resolveTable(ident: Identifier): Option[StructType] = {
    None
  }

  private lazy val jdbcTableCatalog = classOf[JDBCTableCatalog]

  private lazy val methodGetTableName = {
    val method = jdbcTableCatalog.getDeclaredMethod("getTableName", classOf[Identifier])
    method.setAccessible(true)
    method
  }

  private lazy val methodCheckNamespace = {
    val method = jdbcTableCatalog.getDeclaredMethod("checkNamespace", classOf[Array[String]])
    method.setAccessible(true)
    method
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    methodCheckNamespace.invoke(this, namespace)
  }

  protected def getTableName(ident: Identifier): String = {
    methodGetTableName.invoke(this, ident).asInstanceOf[String]
  }
}
