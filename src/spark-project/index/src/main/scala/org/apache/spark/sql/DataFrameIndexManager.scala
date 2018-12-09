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

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.{CatalogTableSource, IndexedDataSource, Metastore}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{HashMap => MutableHashMap}


/**
  * Entrypoint for working with index functionality, e.g. reading indexed table, creating index
  * for provided file path, or deleting index for table.
  */
class DataFrameIndexManager(sparkSession: SparkSession) {
  private var source: String = IndexedDataSource.parquet
  private var extraOptions = new MutableHashMap[String, String]()
  private var userSpecifiedSchema: Option[StructType] = None


  /** File format for table */
  def format(source: String): DataFrameIndexManager = {
    this.source = source
    this
  }

  /** Add key-value to options */
  def option(key: String, value: String): DataFrameIndexManager = {
    this.extraOptions += (key -> value)
    this
  }

  /** Add boolean value to options, for compatibility with Spark */
  def option(key: String, value: Boolean): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add long value to options, for compatibility with Spark */
  def option(key: String, value: Long): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add double value to options, for compatibility with Spark */
  def option(key: String, value: Double): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add options from external map */
  def options(options: scala.collection.Map[String, String]): DataFrameIndexManager = {
    this.extraOptions ++= options
    this
  }

  /**
    * Load indexed table as DataFrame.
    *
    * @param path filepath to the table (directory)
    */
  def load(path: String): DataFrame = {
    option("path", path)
    sparkSession.baseRelationToDataFrame(
      IndexedDataSource(
        Metastore.getOrCreate(sparkSession),
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
    * Set ShardMetastoreSuport
    */

  def shardBy(columns: Array[String]): DataFrameIndexManager = {
    require(columns.length == 1)
    option("shardByColumn", columns.head)
    option("support", "shard")
  }

  /**
    * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
    * automatically from data. By specifying the schema here, the underlying data source can
    * skip the schema inference step, and thus speed up data loading.
    *
    * @since 1.4.0
    */
  def schema(schema: StructType): DataFrameIndexManager = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
    * Load indexed DataFrame from persistent table.
    *
    * @param tableName table name in catalog
    */
  def table(tableName: String): DataFrame = {
    sparkSession.baseRelationToDataFrame(
      CatalogTableSource(
        Metastore.getOrCreate(sparkSession),
        tableName = tableName,
        options = extraOptions.toMap).asDataSource.resolveRelation())
  }

  /**
    * Load indexed DataFrame from Parquet table.
    *
    * @param path filepath to the Parquet table (directory)
    */
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /** DDL command to create index for provided source with options */
  def create: CreateIndexCommand = {
    CreateIndexCommand(
      sparkSession = sparkSession,
      source = source,
      options = extraOptions)
  }

  /** DDL command to check if index exists in metastore */
  def exists: ExistsIndexCommand = {
    ExistsIndexCommand(
      sparkSession = sparkSession,
      source = source,
      options = extraOptions)
  }

  /** DDL command to delete index for provided source */
  def delete: DeleteIndexCommand = {
    DeleteIndexCommand(
      sparkSession = sparkSession,
      source = source,
      options = extraOptions)
  }

  /** Get currently set source, for testing only */
  private[sql] def getSource(): String = this.source

  /** Get currently set options, for testing only */
  private[sql] def getOptions(): Map[String, String] = this.extraOptions.toMap
}

/**
  * [[CreateIndexCommand]] provides functionality to create index for a table. Requires index
  * columns and valid table path. Also allows to specify different mode for creating index, similar
  * to writing DataFrame.
  */
private[sql] case class CreateIndexCommand(
                                            @transient val sparkSession: SparkSession,
                                            private var source: String,
                                            private var options: MutableHashMap[String, String]) {
  private var mode: SaveMode = SaveMode.ErrorIfExists
  private var columns: Seq[Column] = Nil

  /**
    * Provide mode for creating index.
    *
    * @param mode save mode
    */
  def mode(mode: SaveMode): CreateIndexCommand = {
    this.mode = mode
    this
  }

  /**
    * Provide string-like mode to create index.
    *
    * @param mode string value for save mode
    */
  def mode(mode: String): CreateIndexCommand = {
    val typedMode = mode.toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "error" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case other => throw new UnsupportedOperationException(
        s"Unsupported mode $mode, must be one of ${SaveMode.Append}, ${SaveMode.Overwrite}, " +
          s"${SaveMode.ErrorIfExists}, ${SaveMode.Ignore}")
    }
    this.mode = typedMode
    this
  }

  /** Specify columns to index, at least one column is required */
  def indexBy(column: Column, columns: Column*): CreateIndexCommand = {
    this.columns = column +: columns
    this
  }

  /** Specify column names to index, at least one column is required */
  def indexBy(columnName: String, columnNames: String*): CreateIndexCommand = {
    indexBy(col(columnName), columnNames.map(col): _*)
  }

  /**
    * Java-friendly API to index by columns.
    * For Scala it is recommended to use other more convenient API methods.
    */
  def indexBy(columns: Array[Column]): CreateIndexCommand = {
    require(columns.nonEmpty, "At least one column is required, " +
      "use 'indexByAll()' method to infer all columns that can be indexed")
    this.columns = columns.toSeq
    this
  }

  /**
    * Java-friendly API to index by column names.
    * For Scala it is recommended to use other more convenient API methods.
    */
  def indexBy(columnNames: Array[String]): CreateIndexCommand = {
    indexBy(columnNames.map(col))
  }

  /**
    * Java-friendly API to index by column names. Also used in Python API.
    * For Scala it is recommended to use other more convenient API methods.
    */
  def indexBy(columnNames: java.util.List[String]): CreateIndexCommand = {
    val cols = new Array[String](columnNames.size())
    for (i <- 0 until cols.length) {
      cols(i) = columnNames.get(i)
    }
    indexBy(cols)
  }

  /** Use all available columns that can be indexed */
  def indexByAll(): CreateIndexCommand = {
    // assign empty list, will infer all columns, see `MetastoreSupport` API for more info
    this.columns = Nil
    this
  }

  /** Public for Python API */
  def createIndex(path: String): Unit = {
    this.options += "path" -> path
    IndexedDataSource(
      Metastore.getOrCreate(sparkSession),
      className = source,
      mode = mode,
      options = this.options.toMap).createIndex(this.columns)
  }

  /** Create index for Spark persistent table */
  def table(tableName: String): Unit = {
    CatalogTableSource(
      Metastore.getOrCreate(sparkSession),
      tableName = tableName,
      options = this.options.toMap,
      mode = mode).asDataSource.createIndex(this.columns)
  }

  /** Create index for Parquet table as datasource */
  def parquet(path: String): Unit = {
    this.source = IndexedDataSource.parquet
    createIndex(path)
  }

  /** Get currently set source, for testing only */
  private[sql] def getSource(): String = this.source

  /** Get currently set options, for testing only */
  private[sql] def getOptions(): Map[String, String] = this.options.toMap

  /** Get currently set mode, for testing only */
  private[sql] def getMode(): SaveMode = this.mode

  /** Get currently set columns, for testing only */
  private[sql] def getColumns(): Seq[Column] = this.columns.toList
}

/**
  * [[ExistsIndexCommand]] reports whether or not given table path is indexed.
  */
private[sql] case class ExistsIndexCommand(
                                            @transient val sparkSession: SparkSession,
                                            private var source: String,
                                            private val options: MutableHashMap[String, String]) {

  /** Public for Python API */
  def existsIndex(path: String): Boolean = {
    this.options += "path" -> path
    IndexedDataSource(
      Metastore.getOrCreate(sparkSession),
      className = source,
      options = this.options.toMap).existsIndex()
  }

  /** Check index for Spark persistent table */
  def table(tableName: String): Boolean = {
    CatalogTableSource(
      Metastore.getOrCreate(sparkSession),
      tableName = tableName,
      options = this.options.toMap).asDataSource.existsIndex()
  }

  /** Check index for Parquet table as datasource */
  def parquet(path: String): Boolean = {
    this.source = IndexedDataSource.parquet
    existsIndex(path)
  }

  /** Get currently set source, for testing only */
  private[sql] def getSource(): String = this.source

  /** Get currently set options, for testing only */
  private[sql] def getOptions(): Map[String, String] = this.options.toMap
}

/**
  * [[DeleteIndexCommand]] provides functionality to delete existing index. Current behaviour is
  * no-op when deleting non-existent index.
  */
private[sql] case class DeleteIndexCommand(
                                            @transient val sparkSession: SparkSession,
                                            private var source: String,
                                            private val options: MutableHashMap[String, String]) {

  /** Public for Python API */
  def deleteIndex(path: String): Unit = {
    this.options += "path" -> path
    IndexedDataSource(
      Metastore.getOrCreate(sparkSession),
      className = source,
      options = this.options.toMap).deleteIndex()
  }

  /** Delete index for Spark persistent table */
  def table(tableName: String): Unit = {
    CatalogTableSource(
      Metastore.getOrCreate(sparkSession),
      tableName = tableName,
      options = this.options.toMap).asDataSource.deleteIndex()
  }

  /** Delete index for Parquet table as datasource */
  def parquet(path: String): Unit = {
    this.source = IndexedDataSource.parquet
    deleteIndex(path)
  }

  /** Get currently set source, for testing only */
  private[sql] def getSource(): String = this.source

  /** Get currently set options, for testing only */
  private[sql] def getOptions(): Map[String, String] = this.options.toMap
}
