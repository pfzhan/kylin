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
package org.apache.spark.sql.execution.utils

import org.apache.kylin.gridtable.GTInfo
import org.apache.kylin.metadata.model.ColumnDesc
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{SparderConstants, SparderTypeUtil}

import scala.collection.JavaConverters._

// scalastyle:off
object SchemaProcessor {

  def buildGTSchema(coolumnMapping: Array[String],
                    gTInfo: GTInfo,
                    tableName: String): StructType = {
    val measures = gTInfo.getAllColumns.andNot(gTInfo.getPrimaryKey).asScala
    val dimensionStructType = gTInfo.getPrimaryKey.asScala.map { i =>
      StructField(
        FactTableCulumnInfo(tableName, i, coolumnMapping.apply(i)).toString,
        SparderTypeUtil.kylinCubeDataTypeToSparkType(gTInfo.getColumnType(i)),
        nullable = true
      )
    }.toSeq
    val measuresStructType = StructType(
      measures
        .map(i => {
          StructField(
            FactTableCulumnInfo(tableName, i, coolumnMapping.apply(i)).toString,
            SparderTypeUtil.kylinCubeDataTypeToSparkType(
              gTInfo.getColumnType(i)),
            nullable = true)
        })
        .toSeq)
    StructType(dimensionStructType).merge(measuresStructType)
  }

  def factTableSchemaNameToColumnId(schemaName: String): Int = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    data.apply(data.length - 1).toInt
  }

  def parseDeriveTableSchemaName(schemaName: String): DeriveTableColumnInfo = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    try {
      DeriveTableColumnInfo(data.apply(2), data.apply(3).toInt, data.apply(1))
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def generateDeriveTableSchemaName(deriveTableName: String,
                                    colId: Int,
                                    columnName: String = "N"): String = {
    DeriveTableColumnInfo(deriveTableName, colId, columnName).toString
  }

  def replaceToAggravateSchemaName(index: Int,
                                   aggFuncName: String,
                                   hash: String,
                                   aggArgs: String*): String = {
    AggColumnInfo(index, aggFuncName, hash, aggArgs: _*).toString
  }

  def buildFactTableSortNames(sourceSchema: StructType): Array[String] = {
    sourceSchema.fieldNames
      .filter(name => name.startsWith("F__") || name.startsWith("R__"))
      .map(name => (factTableSchemaNameToColumnId(name), name))
      .sortBy(_._1)
      .map(_._2)
  }
  def buildSchemaWithRawTable(columnDescs: Array[ColumnDesc]): StructType = {

    StructType(columnDescs.map { columnDesc =>
      StructField(
        columnDesc.getName,
        SparderTypeUtil.kylinRawTableSQLTypeToSparkType(columnDesc.getType))
    })
  }
}

sealed abstract class ColumnInfo(tableName: String,
                                 columnId: Int,
                                 columnName: String) {
  val prefix: String

  override def toString: String =
    s"$prefix${SparderConstants.COLUMN_NAME_SEPARATOR}$columnName${SparderConstants.COLUMN_NAME_SEPARATOR}$tableName${SparderConstants.COLUMN_NAME_SEPARATOR}$columnId"
}

case class FactTableCulumnInfo(tableName: String,
                               columnId: Int,
                               columnName: String)
    extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "F"
}

case class DeriveTableColumnInfo(tableName: String,
                                 columnId: Int,
                                 columnName: String)
    extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "D"
}

case class AggColumnInfo(index: Int,
                         funcName: String,
                         hash: String,
                         args: String*) {
  override def toString: String =
    s"$funcName(${args.mkString("_")})_${index}_$hash"
}
