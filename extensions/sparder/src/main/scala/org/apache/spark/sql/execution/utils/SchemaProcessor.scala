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
import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.sparder.SparderConstants
import org.apache.spark.sql.types._


object SchemaProcessor {

  def buildSchemaV2(gTInfo: GTInfo, tableName: String): StructType = {
    val measures =
      gTInfo.getAllColumns.asScala.toSet.--(gTInfo.getPrimaryKey.asScala)
    val dimensionStructType = gTInfo.getPrimaryKey.asScala
      .map(
        i =>
          StructField(generateFactTableSchema(tableName, i.toInt),
                      StringType,
                      nullable = false))
      .toSeq
    val measuresStructType = StructType(
      measures
        .map(
          i =>
            StructField(generateFactTableSchema(tableName, i.toInt),
                        BinaryType,
                        nullable = false))
        .toSeq)
    StructType(dimensionStructType).merge(measuresStructType)
  }

  def parseFactTableSchemaName(schemaName: String): (String, Int) = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    (data.apply(0), data.apply(1).toInt)
  }

  def parseDeriveTableSchemaName(schemaName: String): (String, String, Int) = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    (data.apply(0), data.apply(1), data.apply(2).toInt)
  }

  def cleanToSchemaName(
      dataType: org.apache.kylin.metadata.datatype.DataType): String = {
    dataType.toString
      .replace("(", "START")
      .replace(")", "END")
      .replace(",", "KYLINSPLIT")
  }

  def generateFactTableSchema(factTableName: String, colId: Int): String = {
    s"$factTableName${SparderConstants.COLUMN_NAME_SEPARATOR}$colId"
  }

  def generateDeriveTableSchema(deriveTableName: String, colId: Int): String = {
    s"$deriveTableName${SparderConstants.COLUMN_NAME_SEPARATOR}" +
      s"${SparderConstants.DERIVE_TABLE}${SparderConstants.COLUMN_NAME_SEPARATOR}$colId"
  }

  def buildFactTableSortNames(dataFrame: DataFrame): Array[String] = {
    dataFrame.schema.fieldNames
      .map(name => (parseFactTableSchemaName(name), name))
      .sortBy(_._1._2)
      .map(_._2)
  }

  def buildDeriveTableSortNames(dataFrame: DataFrame): Array[String] = {
    dataFrame.schema.fieldNames
      .map(name => (parseDeriveTableSchemaName(name), name))
      .sortBy(_._1._3)
      .map(_._2)
  }
}
