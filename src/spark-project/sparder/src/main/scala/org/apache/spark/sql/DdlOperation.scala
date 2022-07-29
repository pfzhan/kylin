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
package org.apache.spark.sql

import org.apache.spark.sql.DDLDesc.DDLType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{CreateNamespace, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, CreateDatabaseCommand, CreateTableCommand, CreateViewCommand, DropDatabaseCommand, DropTableCommand, ExecutedCommandExec, ShowCreateTableAsSerdeCommand, ShowPartitionsCommand}
import org.apache.spark.sql.types.StructField
import java.lang.{String => JString}
import java.util.{List => JList}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.v2.{CreateNamespaceExec, DropNamespaceExec}

import scala.collection.JavaConverters._


object DdlOperation extends Logging {

  def executeSQL(sqlText: String): DDLDesc = {
    val logicalPlan: LogicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    val currentDatabase: String = SparderEnv.getSparkSession.catalog.currentDatabase
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(create: CreateTableCommand) =>
        val tableIdentifier: TableIdentifier = create.table.identifier
        if (create.table.tableType == CatalogTableType.MANAGED) {
          throw new RuntimeException(s"Table ${tableIdentifier} is managed table.Please modify to external table")
        }
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, tableIdentifier.database.getOrElse(currentDatabase), tableIdentifier.table, DDLType.CREATE_TABLE)
      case ExecutedCommandExec(view: CreateViewCommand) =>
        val viewIdentifier: TableIdentifier = view.name
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, viewIdentifier.database.getOrElse(currentDatabase), viewIdentifier.table, DDLType.CREATE_VIEW)
      case ExecutedCommandExec(drop: DropTableCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, drop.tableName.database.getOrElse(currentDatabase),
          drop.tableName.table,
          DDLType.DROP_TABLE)
      case ExecutedCommandExec(db: CreateDatabaseCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, db.databaseName,
          null,
          DDLType.CREATE_DATABASE)
      case CreateNamespaceExec(_,namespace:Seq[String],_,_) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, namespace(0),
          null,
          DDLType.CREATE_DATABASE)  
      case ExecutedCommandExec(db: DropDatabaseCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, db.databaseName,
          null,
          DDLType.DROP_DATABASE)
      case DropNamespaceExec(_,namespace:Seq[String],_,_) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, namespace(0),
          null,
          DDLType.DROP_DATABASE)
      case ExecutedCommandExec(addPartition: AlterTableAddPartitionCommand) =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, addPartition.tableName.database.getOrElse(currentDatabase),
          addPartition.tableName.table, DDLType.ADD_PARTITION)
      case _ =>
        SparderEnv.getSparkSession.sql(sqlText)
        new DDLDesc(sqlText, null, null, DDLType.NONE)
    }
  }

  implicit class RichStructField(structField: StructField) {
    def toViewDDL: String = {
      val comment: Option[JString] = structField.getComment()
        .map(escapeSingleQuotedString)
        .map(" COMMENT '" + _ + "'")
      s"${quoteIdentifier(structField.name)}${comment.getOrElse("")}"
    }
  }

  def msck(database: JString, table: JString): JList[String] = {
    val before: Seq[Row] = calculatePartition(database, table)
    val tableIdentifier: JString = database + "." + table
    logInfo(s"Before ${tableIdentifier} msck partition number is ${before.size}")
    SparderEnv.getSparkSession.sql(s"msck repair table ${tableIdentifier}")
    val after: Seq[Row] = calculatePartition(database, table)
    logInfo(s"After ${tableIdentifier} msck partition number is ${after.size}")
    val diff: Seq[Row] = after.diff(before)
    diff.map(row => row.getString(0)).asJava
  }

  def hasPartition(database: String, table: String): Boolean = {
    val catalog: SessionCatalog = SparderEnv.getSparkSession.sessionState.catalog
    val catalogTable: CatalogTable = catalog.getTableMetadata(TableIdentifier(table, Some(database)))
    if (catalogTable.tableType != CatalogTableType.VIEW) {
      catalogTable.partitionColumnNames.nonEmpty && catalogTable.storage.locationUri.nonEmpty
    } else {
      false
    }
  }

  def getTableDesc(database: String, table: String): String = {
    val sql = s"show create table ${database}.${table} as serde"
    var ddl = ""
    val logicalPlan: LogicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(sql)
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(show: ShowCreateTableAsSerdeCommand) =>
        val catalog: SessionCatalog = SparderEnv.getSparkSession.sessionState.catalog
        val metadata: CatalogTable = catalog.getTableMetadata(show.table)
        metadata.tableType match {
          case CatalogTableType.VIEW =>
            val builder = new StringBuilder
            builder ++= s"CREATE VIEW ${show.table.quotedString}"
            if (metadata.schema.nonEmpty) {
              builder ++= metadata.schema.map(_.toViewDDL).mkString("(", ", ", ")")
            }
            builder ++= metadata.viewText.mkString(" AS\n", "", "\n")
            ddl = builder.toString()
          case CatalogTableType.MANAGED => ddl = ""
          case CatalogTableType.EXTERNAL => ddl = SparderEnv.getSparkSession.sql(sql).takeAsList(1).get(0).getString(0)
        }
    }
    ddl
  }

  def calculatePartition(database: String, table: String): Seq[Row] = {
    val logicalPlan: LogicalPlan = SparderEnv.getSparkSession.sessionState.sqlParser.parsePlan(s"show partitions ${database}.${table}")
    val queryExecution: QueryExecution = SparderEnv.getSparkSession.sessionState.executePlan(logicalPlan,
      CommandExecutionMode.SKIP)
    stripRootCommandResult(queryExecution.executedPlan) match {
      case ExecutedCommandExec(showPartitions: ShowPartitionsCommand) =>
        val rows: Seq[Row] = showPartitions.run(SparderEnv.getSparkSession)
        rows
    }
  }

  private def stripRootCommandResult(executedPlan: SparkPlan): SparkPlan = executedPlan match {
    case CommandResultExec(_, plan, _) => plan
    case other => other
  }
}
