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

import java.util.concurrent.TimeUnit

import com.google.common.cache.{
  Cache,
  CacheBuilder,
  RemovalListener,
  RemovalNotification
}
import com.google.common.collect.Lists
import io.kyligence.kap.metadata.model.NTableMetadataManager
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.dict.lookup.{
  SnapshotManager,
  SnapshotTable,
  SnapshotTableSerializer
}
import org.apache.kylin.metadata.model.TableDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.utils.DeriveTableColumnInfo
import org.apache.spark.sql.types.{
  DateType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparderEnv, SparkSession}

// scalastyle:off
object SparderLookupManager extends Logging {
  lazy val serializer: SnapshotTableSerializer =
    SnapshotTableSerializer.FULL_SERIALIZER
  val DEFAULT_MAXSIZE = 100
  val DEFAULT_EXPIRE_TIME = 1
  val DEFAULT_TIME_UNIT = TimeUnit.HOURS

  val sourceCache: Cache[String, Dataset[Row]] = CacheBuilder.newBuilder
    .maximumSize(DEFAULT_MAXSIZE)
    .expireAfterWrite(DEFAULT_EXPIRE_TIME, DEFAULT_TIME_UNIT)
    .removalListener(new RemovalListener[String, Dataset[Row]]() {
      override def onRemoval(
          notification: RemovalNotification[String, Dataset[Row]]): Unit = {
        logInfo("Remove lookup table from spark : " + notification.getKey)
        notification.getValue.unpersist()
      }
    })
    .build
    .asInstanceOf[Cache[String, Dataset[Row]]]

  def create(name: String,
             sourcePath: String,
             kylinConfig: KylinConfig): Dataset[Row] = {
    val names = name.split("@")
    val projectName = names.apply(0)
    val tableName = names.apply(1)
    val metaMgr = NTableMetadataManager.getInstance(kylinConfig, projectName)
    val tableDesc = metaMgr.getTableDesc(tableName)
    val columns = tableDesc.getColumns
    val dfTableName = Integer.toHexString(System.identityHashCode(name))
    val schema = StructType(Range(0, columns.size).map(index => {
      val sparderType =
        if (SparderTypeUtil.isDateTimeFamilyType(
              columns(index).getType.getName)) DateType
        else StringType
      StructField(DeriveTableColumnInfo(dfTableName,
                                        index,
                                        columns(index).getName).toString,
                  sparderType)
    }))
    SparderEnv.getSparkSession.read
      .parquet(sourcePath)
      .toDF(schema.fieldNames: _*)
  }

  def convertRow(tableDesc: TableDesc,
                 snapshotTable: SnapshotTable): java.util.ArrayList[Row] = {
    val dataTimeIndex = tableDesc.getColumns.indices
      .zip(tableDesc.getColumns)
      .filter(_._2.getType.isDateTimeFamily)
      .map(_._1)

    val dataIndex = tableDesc.getColumns.indices
      .zip(tableDesc.getColumns)
      .filter(_._2.getType.isDate)
      .map(_._1)

    val data: java.util.ArrayList[Row] =
      Lists.newArrayListWithCapacity(snapshotTable.getRowCount)
    val reader = snapshotTable.getReader
    while (reader.next()) {
      val rowData = reader.getRow
      val sparderData = new Array[Any](rowData.size)
      Array.copy(rowData, 0, sparderData, 0, rowData.size)
      for (index <- dataTimeIndex) {
        if (dataIndex.contains(index)) {
          sparderData(index) =
            new java.sql.Date(DateFormat.stringToMillis(rowData.apply(index)))
        } else {
          sparderData(index) = String.valueOf(
            SparderTypeUtil.toSparkTimestamp(
              DateFormat.stringToMillis(rowData.apply(index))))
        }
      }
      data.add(Row.fromSeq(sparderData))
    }
    data
  }

  def getOrCreate(name: String,
                  sourcePath: String,
                  kylinConfig: KylinConfig): DataFrame = {
    val value = sourceCache.getIfPresent(sourcePath)
    if (value != null) {
      value
    } else {
      create(name, sourcePath, kylinConfig)
    }
  }
}
