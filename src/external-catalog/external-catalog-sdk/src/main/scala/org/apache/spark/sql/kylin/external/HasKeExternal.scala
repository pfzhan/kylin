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
package org.apache.spark.sql.kylin.external

import java.net.URI

import io.kyligence.api.ApiException
import io.kyligence.api.catalog.{FieldSchema, IExternalCatalog => KeExternalCatalog}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.types.{DataType, HIVE_TYPE_STRING, Metadata, MetadataBuilder, StructField, StructType}

import scala.collection.JavaConverters._

trait HasKeExternal {
  val KYLIN_FORMAT = "KYLIN_EXTERNAL"
  def keExternalCatalog: KeExternalCatalog

  protected def withClient[T](body: => T): T = {
    try {
      body
    } catch {
      case apiException: ApiException => throw new RuntimeException(apiException)
      case e: Throwable => throw e
    }
  }

  protected def getExternalDatabase(db: String): Option[CatalogDatabase] = withClient {
    Option(keExternalCatalog.getDatabase(db)).map{ dbObj =>
      CatalogDatabase(
        dbObj.getName,
        dbObj.getDescription,
        new URI(dbObj.getLocationUri),
        dbObj.getParameters.asScala.toMap) }
  }

  protected def databaseExistsInExternal(db: String): Boolean = withClient {
    return keExternalCatalog.getDatabase(db) != null
  }

  protected def listExternalDatabases(): Seq[String] = withClient {
    keExternalCatalog.getDatabases(".*").asScala.sorted
  }

  protected def tableExistsInExternal(db: String, table: String): Boolean = withClient{
    if (keExternalCatalog.getDatabase(db) != null) {
      keExternalCatalog.getTable(db, table, false) != null
    } else {
      false
    }
  }

  protected def listExternalTables(db: String, pattern: String): Seq[String] = withClient{
    if (keExternalCatalog.getDatabase(db) != null) {
      keExternalCatalog.getTables(db, pattern).asScala.sorted
    } else {
      Nil
    }
  }

  protected def getExternalTable(db: String, tableName: String): Option[CatalogTable] = withClient{
    Option(keExternalCatalog.getTable(db, tableName, false))
      .map { table =>
        val properties = Option(table.getParameters).map(_.asScala.toMap).orNull

        val excludedTableProperties = Set(
          // The property value of "comment" is moved to the dedicated field "comment"
          "comment",
          // createVersion
          HasKeExternal.CREATED_SPARK_VERSION
        )
        val filteredProperties = properties.filterNot {
          case (key, _) => excludedTableProperties.contains(key)
        }
        CatalogTable(
          identifier = TableIdentifier(table.getTableName, Option(table.getDbName)),
          tableType = CatalogTableType.EXTERNAL,
          schema = StructType(table.getFields.asScala.map(HasKeExternal.fromExternalColumn)),
          storage = CatalogStorageFormat(
            locationUri = None,
            inputFormat = Some(KYLIN_FORMAT),
            outputFormat = Some(KYLIN_FORMAT),
            serde = None,
            compressed = false,
            properties = Map()
          ),
          provider = None,
          owner = Option(table.getOwner).getOrElse(""),
          createTime = table.getCreateTime.toLong * 1000,
          lastAccessTime = table.getLastAccessTime.toLong * 1000,
          createVersion = properties.getOrElse(HasKeExternal.CREATED_SPARK_VERSION, "unknown external version"),
          comment = properties.get("comment"),
          viewText = None,
          properties = filteredProperties
        )
      }
  }
}

object HasKeExternal {
  val SPARK_SQL_PREFIX = "spark.sql."
  val CREATED_SPARK_VERSION = SPARK_SQL_PREFIX + "create.version"

  /** Builds the native StructField from Hive's FieldSchema. */
  def fromExternalColumn(hc: FieldSchema): StructField = {
    val columnType = getSparkSQLDataType(hc)
    val metadata = if (hc.getType != columnType.catalogString) {
      new MetadataBuilder().putString(HIVE_TYPE_STRING, hc.getType).build()
    } else {
      Metadata.empty
    }

    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  /** Get the Spark SQL native DataType from Hive's FieldSchema. */
  def getSparkSQLDataType(hc: FieldSchema): DataType = {
    try {
      CatalystSqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }
  }
}