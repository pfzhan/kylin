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
package org.apache.spark.sql.manager

import com.google.common.cache.RemovalNotification
import com.google.common.collect.Lists
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.dict.lookup.{
  SnapshotManager,
  SnapshotTable,
  SnapshotTableSerializer
}
import org.apache.kylin.metadata.TableMetadataManager
import org.apache.kylin.metadata.model.TableDesc

import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparderLookupManager extends ResourceManager[String, DataFrame] {
  lazy val serializer: SnapshotTableSerializer =
    SnapshotTableSerializer.FULL_SERIALIZER
  override val DEFAULT_MAXSIZE: Int = 100

  override def removeLister(
      notification: RemovalNotification[String, DataFrame]): Unit = {
    notification.getValue.unpersist()
  }

  override def create(name: String,
                      sourcePath: String,
                      kylinConfig: KylinConfig): DataFrame = {
    val manager: SnapshotManager = SnapshotManager.getInstance(kylinConfig)
    val metaMgr = TableMetadataManager.getInstance(kylinConfig)

    val names = name.split("@")
    val projectName = names.apply(0)
    val tableName = names.apply(1)
    val snapshot = manager.getSnapshotTable(sourcePath)
    val tableDesc = metaMgr.getTableDesc(tableName, projectName)
    val data = convertRow(tableDesc, snapshot)
    val dfTableName = Integer.toHexString(System.identityHashCode(name))
    val schema = StructType(Range(0, data.get(0).size).map(index =>
      StructField(
        SchemaProcessor.generateDeriveTableSchema(dfTableName, index).toString,
        StringType)))
    val df = SparkSession.getDefaultSession.get.createDataFrame(data, schema)
    logInfo(
      s"add snapshot table :$tableName, table path:$sourcePath, row count:${data.size()}")
    sourceCache.put(sourcePath, df)
    df
  }

  def convertRow(tableDesc: TableDesc,
                 snapshotTable: SnapshotTable): java.util.ArrayList[Row] = {
    val dataTimeIndex = tableDesc.getColumns.indices
      .zip(tableDesc.getColumns)
      .filter(_._2.getType.isDateTimeFamily)
      .map(_._1)
    val data: java.util.ArrayList[Row] =
      Lists.newArrayListWithCapacity(snapshotTable.getRowCount)
    val reader = snapshotTable.getReader
    while (reader.next()) {
      val rowdata = reader.getRow
      for (index <- dataTimeIndex) {
        rowdata(index) =
          String.valueOf(DateFormat.stringToMillis(rowdata.apply(index)))
      }
      data.add(Row.fromSeq(rowdata))
    }
    data
  }
}
