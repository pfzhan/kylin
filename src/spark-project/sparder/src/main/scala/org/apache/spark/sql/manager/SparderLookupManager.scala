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

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import io.kyligence.kap.metadata.model.NTableMetadataManager
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.ColumnDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.utils.DeriveTableColumnInfo
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparderEnv}
import org.apache.spark.sql.util.SparderTypeUtil
import io.kyligence.kap.query.util.PartitionsFilter.PARTITION_COL
import io.kyligence.kap.query.util.PartitionsFilter.PARTITIONS

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// scalastyle:off
object SparderLookupManager extends Logging {
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
    val cols = tableDesc.getColumns.toList

    var columns = cols.filter(col => !col.getName.equals(tableDesc.getSnapshotPartitionCol))
    if (tableDesc.getSnapshotPartitionCol != null) {
      columns = columns :+ tableDesc.findColumnByName(tableDesc.getSnapshotPartitionCol)
    }
    val dfTableName = Integer.toHexString(System.identityHashCode(name))


    val orderedCol = new ListBuffer[(ColumnDesc, Int)]
    var partitionCol: (ColumnDesc, Int) = null
    for ((col, index) <- tableDesc.getColumns.zipWithIndex) {
      if (!col.getName.equals(tableDesc.getSnapshotPartitionCol)) {
        orderedCol.append((col, index))
      } else {
        partitionCol = (col, index)
      }
    }
    val options = new scala.collection.mutable.HashMap[String, String]
    if (partitionCol != null) {
      orderedCol.append(partitionCol)
      options.put(PARTITION_COL, tableDesc.getSnapshotPartitionCol)
      options.put(PARTITIONS, String.join(",", tableDesc.getSnapshotPartitions.keySet()))
      options.put("mapreduce.input.pathFilter.class", "io.kyligence.kap.query.util.PartitionsFilter")
    }
    val originSchema = StructType(orderedCol.map { case (col, index) => StructField(col.getName, SparderTypeUtil.toSparkType(col.getType)) })
    val schema = StructType(orderedCol.map { case (col, index) => StructField(DeriveTableColumnInfo(dfTableName, index, col.getName).toString, SparderTypeUtil.toSparkType(col.getType)) })
    val resourcePath = KapConfig.getInstanceFromEnv.getReadHdfsWorkingDirectory + sourcePath

    SparderEnv.getSparkSession.read.options(options)
      .schema(originSchema)
      .parquet(resourcePath)
      .toDF(schema.fieldNames: _*)
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
