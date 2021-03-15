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

import java.util.Locale

import io.kyligence.api.catalog.{FieldSchema, IExternalCatalog, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.kylin.external.HasKeExternal.getSparkSQLDataType
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, StructField, StructType}


class KylinExternalCatalog(
                            conf: SparkConf = new SparkConf,
                            hadoopConfig: Configuration = new Configuration,
                            val keExternalCatalog: IExternalCatalog)
  extends InMemoryCatalog(conf, hadoopConfig) with Logging with HasKeExternal {

  override def createDatabase(
                               dbDefinition: CatalogDatabase,
                               ignoreIfExists: Boolean): Unit = {
    super.createDatabase(dbDefinition, ignoreIfExists)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    getExternalDatabase(db).getOrElse(super.getDatabase(db))
  }

  override def databaseExists(db: String): Boolean = {
    databaseExistsInExternal(db) || super.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = {
    listExternalDatabases().union(super.listDatabases().map(_.toUpperCase(Locale.ROOT))).distinct.sorted
  }

  override def tableExists(db: String, table: String): Boolean = {
    tableExistsInExternal(db, table) || super.tableExists(db, table)
  }

  override def listPartitions(db: String,
                              table: String,
                              partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    listPartitionsInExternal(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    if (databaseExistsInExternal(db)) {
      listExternalTables(db, ".*")
    } else {
      super.listTables(db)
    }
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    listExternalTables(db, pattern)
      .union {
        if (super.databaseExists(db)) {
          StringUtils.filterPattern(super.listTables(db), pattern).map(_.toUpperCase(Locale.ROOT))
        } else {
          Nil
        }
      }
      .distinct
      .sorted
  }

  override def getTable(db: String, table: String): CatalogTable = {
    getExternalTable(db, table).getOrElse(super.getTable(db, table))
  }
}

object KylinExternalCatalog {

  def fromExternalFormat(format: String): String = {
    Enum.valueOf(classOf[Table.Format], format) match {
      case Table.Format.JSON => "org.apache.spark.sql.json"
      case Table.Format.CSV => "com.databricks.spark.csv"
      case Table.Format.PARQUET => "org.apache.spark.sql.parquet"
      case Table.Format.ORC => "org.apache.spark.sql.hive.orc"
      case _ => throw new UnsupportedOperationException(format)
    }
  }

  def toExternalFormate(provider: String): Table.Format = {
    provider.toLowerCase(Locale.ROOT) match {
      case "org.apache.spark.sql.json" | "json" => Table.Format.JSON
      case "com.databricks.spark.csv" | "csv" => Table.Format.CSV
      case "org.apache.spark.sql.parquet" | "parquet" => Table.Format.PARQUET
      case "org.apache.spark.sql.hive.orc" | "orc" => Table.Format.ORC
      case _ => throw new UnsupportedOperationException(provider)
    }
  }

  /** Converts the native StructField to Hive's FieldSchema. */
  def toExternalColumn(c: StructField): FieldSchema = {
    val typeString = if (c.metadata.contains(HIVE_TYPE_STRING)) {
      c.metadata.getString(HIVE_TYPE_STRING)
    } else {
      c.dataType.catalogString
    }
    new FieldSchema(c.name, typeString, c.getComment().orNull)
  }

  def verifyColumnDataType(schema: StructType): Unit = {
    schema.foreach(col => getSparkSQLDataType(toExternalColumn(col)))
  }

  def toExternalType(tableType: CatalogTableType): Table.Type = {
    tableType match {
      case CatalogTableType.VIEW => Table.Type.VIEW
      case _ => Table.Type.EXTERNAL_TABLE
    }
  }

  def fromExternalType(tableType: String): CatalogTableType = {
    Enum.valueOf(classOf[Table.Type], tableType) match {
      case Table.Type.VIEW => CatalogTableType.VIEW
      case Table.Type.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
      case _ => throw new UnsupportedOperationException(tableType)
    }
  }
}