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

package io.kyligence.kap.engine.spark.builder

import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.engine.spark.utils.LogUtils
import io.kyligence.kap.metadata.model.NTableMetadataManager
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.TableDesc
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils

import java.io.IOException
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class SnapshotPartitionBuilder extends SnapshotBuilder {

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, table: TableDesc, partitionCol: String, partitions: java.util.Set[String]): Unit = {
    executeBuildSnapshot(ss, table, partitionCol, partitions.asScala.toSet)
  }

  def checkPointForPartition(project: String, tableName: String, partition: String, result: Result): Unit = {
    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableDesc] {
      override def process(): TableDesc = {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copyTable = tableMetadataManager.copyForWrite(tableMetadataManager.getTableDesc(tableName))
        val copyExt = tableMetadataManager.copyForWrite(tableMetadataManager.getOrCreateTableExt(tableName))
        if (result.totalRows != -1) {
          copyExt.setTotalRows(copyExt.getTotalRows + result.totalRows - copyTable.getPartitionRow(partition))
          copyTable.putPartitionSize(partition, result.originalSize)
          copyTable.setSnapshotTotalRows(copyTable.getSnapshotTotalRows + result.totalRows - copyTable.getPartitionRow(partition))
          copyTable.putPartitionRow(partition, result.totalRows)
        } else {
          // -1 in partitionSize means not build
          copyTable.putPartitionSize(partition, 0)
          copyTable.putPartitionRow(partition, 0)
        }
        tableMetadataManager.updateTableDesc(copyTable)
        tableMetadataManager.saveTableExt(copyExt)
        copyTable
      }
    }
    UnitOfWork.doInTransactionWithRetry(new TableUpdateOps, project)
    log.info(s"check point partitions for $tableName , partition $partition")
  }

  def executeBuildSnapshot(ss: SparkSession, table: TableDesc, partitionCol: String, partitions: Set[String]): Unit = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val resourcePath = table.getTempSnapshotPath
    val snapshotTablePath = baseDir + '/' + resourcePath

    val kylinConf = KylinConfig.getInstanceFromEnv
    val snapshotParallelBuildTimeoutSeconds = kylinConf.snapshotParallelBuildTimeoutSeconds()
    val maxThread = if (kylinConf.snapshotPartitionBuildMaxThread() >= 2) kylinConf.snapshotPartitionBuildMaxThread() else 2
    val service = Executors.newFixedThreadPool(maxThread)
    implicit val executorContext = ExecutionContext.fromExecutorService(service)

    val futures = partitions.map { partition =>
      Future {
        wrapConfigExecute[Unit](() => {
          val result = buildSingleSnapshotWithoutMd5(ss, table, partitionCol, partition, snapshotTablePath)
          checkPointForPartition(table.getProject, table.getIdentity, partition, result)
        }, table.getIdentity + ":" + partition)
      }
    }

    try {
      val eventualTuples = Future.sequence(futures.toList)
      // only throw the first exception
      ProxyThreadUtils.awaitResult(eventualTuples, snapshotParallelBuildTimeoutSeconds seconds)

    } finally {
      ProxyThreadUtils.shutdown(service)
    }
  }


  def newFilter(partitionCol: String, partition: String, colType: DataType): String = {
    if (colType.isDate) {
      "`" + partitionCol + "`" + "= cast('" + partition + "' as date)"
    } else if (colType.isNumberFamily) {
      "`" + partitionCol + "`" + "= " + partition + ""
    } else {
      "`" + partitionCol + "`" + "= '" + partition + "'"
    }
  }

  def buildSingleSnapshotWithoutMd5(ss: SparkSession, tableDesc: TableDesc,
                                    partitionCol: String, partition: String, snapshotTablePath: String): Result = {
    var sourceData = getSourceData(ss, tableDesc)
    sourceData = sourceData.filter(newFilter(partitionCol, partition, tableDesc.findColumnByName(partitionCol).getType))

    sourceData = sourceData.selectExpr(sourceData.columns.filter(!_.equals(partitionCol)).map("`" + _ + "`"): _*)

    var newPartition = partition.replaceAll(" ", "_")
    newPartition = newPartition.replaceAll(":", "_")

    val partitionName = partitionCol + '=' + newPartition
    val resourcePath = snapshotTablePath + "/" + partitionName


    val (repartitionNum, sizeMB) = decideSparkJobArg(sourceData)

    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableDesc.getIdentity}.")
    lazy val snapshotInfo = Map(
      "source" -> tableDesc.getIdentity,
      "snapshot" -> snapshotTablePath,
      "sizeMB" -> sizeMB,
      "partition" -> repartitionNum,
      "buildPartition" -> partition
    )
    logInfo(s"Building snapshot: ${LogUtils.jsonMap(snapshotInfo)}")


    if (repartitionNum == 0) {
      sourceData.write.mode(SaveMode.Overwrite).parquet(resourcePath)
    } else {
      sourceData.repartition(repartitionNum).write.mode(SaveMode.Overwrite).parquet(resourcePath)
    }
    val (originSize, totalRows) = computeSnapshotSize(sourceData)
    Result(snapshotTablePath, originSize, totalRows)
  }

}
