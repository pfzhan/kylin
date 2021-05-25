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

import java.io.IOException
import java.util
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}

import com.google.common.collect.Maps
import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.{DFChooser, KylinBuildEnv}
import io.kyligence.kap.engine.spark.utils.{FileNames, LogUtils}
import io.kyligence.kap.metadata.model.{NDataModel, NTableMetadataManager}
import io.kyligence.kap.metadata.project.NProjectManager
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.{TableDesc, TableExtDesc}
import org.apache.kylin.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class SnapshotBuilder extends Logging with Serializable {

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024
  protected val kylinConfig = KylinConfig.getInstanceFromEnv

  @transient
  private val ParquetPathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(PARQUET_SUFFIX)
    }
  }

  @transient
  private val Md5PathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(MD5_SUFFIX)
    }
  }

  // scalastyle:of
  def updateMeta(toBuildTableDesc: Set[TableDesc], resultMap: util.Map[String, Result]): Unit = {
    val project = toBuildTableDesc.iterator.next.getProject
    toBuildTableDesc.foreach(table => {
      updateTableSnapshot(project, table, resultMap)
      updateTableExt(project, table, resultMap)
    }
    )
  }

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, toBuildTables: java.util.Set[TableDesc]): Unit = {
    buildSnapshot(ss, toBuildTables.asScala.toSet)
  }

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, tables: Set[TableDesc]): Unit = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val toBuildTables = tables
    val kylinConf = KylinConfig.getInstanceFromEnv
    if (toBuildTables.isEmpty) {
      return
    }

    // scalastyle:off
    val resultMap = executeBuildSnapshot(ss, toBuildTables, baseDir, kylinConf.isSnapshotParallelBuildEnabled, kylinConf.snapshotParallelBuildTimeoutSeconds)
    // update metadata
    updateMeta(toBuildTables, resultMap)
  }

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, model: NDataModel, ignoredSnapshotTables: util.Set[String]): Unit = {
    val toBuildTableDesc = distinctTableDesc(model, ignoredSnapshotTables)
    buildSnapshot(ss, toBuildTableDesc)
  }

  private def updateTableSnapshot(project: String, table: TableDesc, resultMap: util.Map[String, Result]): Unit = {
    val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
    val copy = tableMetadataManager.copyForWrite(table);
    copy.setLastSnapshotPath(resultMap.get(copy.getIdentity).path)

    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableDesc] {
      override def process(): TableDesc = {
        tableMetadataManager.updateTableDesc(copy)
        copy
      }
    }
    UnitOfWork.doInTransactionWithRetry(new TableUpdateOps, project)
  }

  private def updateTableExt(project: String, table: TableDesc, map: util.Map[String, Result]): Unit = {
    val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
    var tableExt = tableMetadataManager.getOrCreateTableExt(table)
    tableExt = tableMetadataManager.copyForWrite(tableExt)

    val result = map.get(table.getIdentity)
    if (result.totalRows != -1) {
      tableExt.setOriginalSize(result.originalSize)
    }
    tableExt.setTotalRows(result.totalRows)

    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableExtDesc] {
      override def process(): TableExtDesc = {
        tableMetadataManager.saveTableExt(tableExt)
        tableExt
      }
    }
    UnitOfWork.doInTransactionWithRetry(new TableUpdateOps, project)
  }

  @throws[IOException]
  def calculateTotalRows(ss: SparkSession, model: NDataModel, ignoredSnapshotTables: util.Set[String]): Unit = {
    val toCalculateTableDesc = toBeCalculateTableDesc(model, ignoredSnapshotTables)
    val map = new ConcurrentHashMap[String, Result]

    toCalculateTableDesc.foreach(tableDesc => {
      val totalRows = calculateTableTotalRows(tableDesc.getLastSnapshotPath, tableDesc, ss)
      map.put(tableDesc.getIdentity, Result("", -1, totalRows))
    })

    toCalculateTableDesc.foreach(table => {
      updateTableExt(model.getProject, table, map)
    })
  }

  def calculateTableTotalRows(snapshotPath: String, tableDesc: TableDesc, ss: SparkSession): Long = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val fs = HadoopUtil.getWorkingFileSystem
    try {
      if (snapshotPath != null) {
        val path = new Path(baseDir, snapshotPath)
        if (fs.exists(path)) {
          logInfo(s"Calculate table ${tableDesc.getIdentity}'s total rows from snapshot ${path}")
          val totalRows = ss.read.parquet(path.toString).count()
          logInfo(s"Table ${tableDesc.getIdentity}'s total rows is ${totalRows}'")
          return totalRows
        }
      }
    } catch {
      case e: Throwable => logWarning(s"Calculate table ${tableDesc.getIdentity}'s total rows exception", e)
    }
    logInfo(s"Calculate table ${tableDesc.getIdentity}'s total rows from source data")
    val sourceData = getSourceData(ss, tableDesc)
    val totalRows = sourceData.count()
    logInfo(s"Table ${tableDesc.getIdentity}'s total rows is ${totalRows}'")
    totalRows
  }

  // scalastyle:off
  def executeBuildSnapshot(ss: SparkSession, toBuildTableDesc: Set[TableDesc], baseDir: String,
                           isParallelBuild: Boolean, snapshotParallelBuildTimeoutSeconds: Int): util.Map[String, Result] = {
    val snapSizeMap = Maps.newConcurrentMap[String, Result]
    val fs = HadoopUtil.getWorkingFileSystem
    val kylinConf = KylinConfig.getInstanceFromEnv

    if (isParallelBuild) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc.map(tableDesc =>
        Future {
          var config: SetAndUnsetThreadLocalConfig = null
          try {
            config = KylinConfig.setAndUnsetThreadLocalConfig(kylinConf)
            buildSingleSnapshotWithoutMd5(ss, tableDesc, baseDir, snapSizeMap)
          } catch {
            case exception: Exception =>
              logError(s"Error for build snapshot table with $tableDesc", exception)
              throw exception
          } finally {
            if (config != null) {
              config.close()
            }
          }
        }
      )
      try {
        val eventualTuples = Future.sequence(futures.toList)
        // only throw the first exception
        ProxyThreadUtils.awaitResult(eventualTuples, snapshotParallelBuildTimeoutSeconds seconds)
      } catch {
        case e: Exception =>
          ProxyThreadUtils.shutdown(service)
          throw e
      }
    } else {
      toBuildTableDesc.foreach(buildSingleSnapshot(ss, _, baseDir, fs, snapSizeMap))
    }
    snapSizeMap
  }


  private def isIgnoredSnapshotTable(tableDesc: TableDesc, ignoredSnapshotTables: java.util.Set[String]): Boolean = {
    if (ignoredSnapshotTables == null || tableDesc.getLastSnapshotPath == null) {
      return false
    }
    ignoredSnapshotTables.contains(tableDesc.getIdentity)

  }

  private def toBeCalculateTableDesc(model: NDataModel, ignoredSnapshotTables: java.util.Set[String]): Set[TableDesc] = {
    val project = model.getRootFactTable.getTableDesc.getProject
    val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
    val toBuildTableDesc = model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && !isIgnoredSnapshotTable(tableDesc, ignoredSnapshotTables)
      })
      .map(_.getTableRef.getTableDesc)
      .filter(tableDesc => {
        val tableExtDesc = tableManager.getTableExtIfExists(tableDesc)
        tableExtDesc == null || tableExtDesc.getTotalRows == 0L
      })
      .toSet

    val toBuildTableDescTableName = toBuildTableDesc.map(_.getIdentity)
    logInfo(s"table to be calculate total rows: $toBuildTableDescTableName")
    toBuildTableDesc
  }

  def distinctTableDesc(model: NDataModel, ignoredSnapshotTables: java.util.Set[String]): Set[TableDesc] = {
    val toBuildTableDesc = model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && !isIgnoredSnapshotTable(tableDesc, ignoredSnapshotTables)
      })
      .map(_.getTableRef.getTableDesc)
      .toSet

    val toBuildTableDescTableName = toBuildTableDesc.map(_.getIdentity)
    logInfo(s"table snapshot to be build: $toBuildTableDescTableName")
    toBuildTableDesc
  }

  def getSourceData(ss: SparkSession, tableDesc: TableDesc): Dataset[Row] = {
    val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
      .getProject(tableDesc.getProject).getOverrideKylinProps
    SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, params)
  }

  def getFileMd5(file: FileStatus): String = {
    val dfs = HadoopUtil.getWorkingFileSystem
    val in = dfs.open(file.getPath)
    Try(DigestUtils.md5Hex(in)) match {
      case Success(md5) =>
        in.close()
        md5
      case Failure(error) =>
        in.close()
        logError(s"building snapshot get file: ${file.getPath} md5 error,msg: ${error.getMessage}")
        throw new IOException(s"Failed to generate file: ${file.getPath} md5 ", error)
    }
  }

  def buildSingleSnapshot(ss: SparkSession, tableDesc: TableDesc, baseDir: String, fs: FileSystem, resultMap: util.Map[String, Result]): Unit = {
    val sourceData = getSourceData(ss, tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
    var snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    sourceData.coalesce(1).write.parquet(resourcePath)
    val (originSize, totalRows) = computeSnapshotSize(sourceData, calculateTableTotalRows(snapshotTablePath, tableDesc, ss))
    val currSnapFile = fs.listStatus(new Path(resourcePath), ParquetPathFilter).head
    val currSnapMd5 = getFileMd5(currSnapFile)
    val md5Path = resourcePath + "/" + "_" + currSnapMd5 + MD5_SUFFIX

    var isReuseSnap = false
    val existPath = baseDir + "/" + tablePath
    val existSnaps = fs.listStatus(new Path(existPath))
      .filterNot(_.getPath.getName == new Path(snapshotTablePath).getName)
    breakable {
      for (snap <- existSnaps) {
        Try(fs.listStatus(snap.getPath, Md5PathFilter)) match {
          case Success(list) =>
            list.headOption match {
              case Some(file) =>
                val md5Snap = file.getPath.getName
                  .replace(MD5_SUFFIX, "")
                  .replace("_", "")
                if (currSnapMd5 == md5Snap) {
                  snapshotTablePath = tablePath + "/" + snap.getPath.getName
                  fs.delete(new Path(resourcePath), true)
                  isReuseSnap = true
                  break()
                }
              case None =>
                logInfo(s"Snapshot path: ${snap.getPath} not exists snapshot file")
            }
          case Failure(error) =>
            logInfo(s"File not found", error)
        }
      }
    }

    if (!isReuseSnap) {
      fs.createNewFile(new Path(md5Path))
      logInfo(s"Create md5 file: ${md5Path} for snap: ${currSnapFile}")
    }

    resultMap.put(tableDesc.getIdentity, Result(snapshotTablePath, originSize, totalRows))
  }

  def buildSingleSnapshotWithoutMd5(ss: SparkSession, tableDesc: TableDesc, baseDir: String,
                                    resultMap: ConcurrentMap[String, Result]): Unit = {
    val sourceData = getSourceData(ss, tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
    val snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    val (repartitionNum, sizeMB) = try {
      val sizeInMB = ResourceDetectUtils.getPaths(sourceData.queryExecution.sparkPlan)
        .map(path => HadoopUtil.getContentSummary(path.getFileSystem(HadoopUtil.getCurrentConfiguration), path).getLength)
        .sum * 1.0 / MB
      val num = Math.ceil(sizeInMB / KylinBuildEnv.get().kylinConfig.getSnapshotShardSizeMB).intValue()
      (num, sizeInMB)
    } catch {
      case t: Throwable =>
        logWarning("Error occurred when estimate repartition number.", t)
        (0, 0)
    }
    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableDesc.getIdentity}.")
    lazy val snapshotInfo = Map(
      "source" -> tableDesc.getIdentity,
      "snapshot" -> snapshotTablePath,
      "sizeMB" -> sizeMB,
      "partition" -> repartitionNum
    )
    logInfo(s"Building snapshot: ${LogUtils.jsonMap(snapshotInfo)}")
    if (repartitionNum == 0) {
      sourceData.write.parquet(resourcePath)
    } else {
      sourceData.repartition(repartitionNum).write.parquet(resourcePath)
    }
    val (originSize, totalRows) = computeSnapshotSize(sourceData, calculateTableTotalRows(snapshotTablePath, tableDesc, ss))
    resultMap.put(tableDesc.getIdentity, Result(snapshotTablePath, originSize, totalRows))
  }

  private[builder] def computeSnapshotSize(sourceData: Dataset[Row], totalRows: Long) = {
    val columnSize = sourceData.columns.length
    val ds = sourceData.mapPartitions {
      iter =>
        var totalSize = 0L;
        iter.foreach(row => {
          for (i <- 0 until columnSize - 1) {
            val value = row.get(i)
            val strValue = if (value == null) null
            else value.toString
            totalSize += DFChooser.utf8Length(strValue)
          }
        })
        List(totalSize).toIterator
    }(Encoders.scalaLong)

    if (ds.isEmpty) {
      (0L, totalRows)
    } else {
      val size = ds.reduce(_ + _)
      (size, totalRows)
    }
  }

  private[builder] def wrapConfigExecute[R](callable: () => R, taskInfo: String): R = {
    var config: SetAndUnsetThreadLocalConfig = null
    try {
      config = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)
      callable.apply()
    } catch {
      case exception: Exception =>
        logError(s"Error for build snapshot table with $taskInfo", exception)
        throw exception
    } finally {
      if (config != null) {
        config.close();
      }
    }
  }

  private[builder] def decideSparkJobArg(sourceData: Dataset[Row]): (Int, Double) = {
    try {
      val sizeInMB = ResourceDetectUtils.getPaths(sourceData.queryExecution.sparkPlan)
        .map(path => HadoopUtil.getContentSummary(path.getFileSystem(HadoopUtil.getCurrentConfiguration), path).getLength)
        .sum * 1.0 / MB
      val num = Math.ceil(sizeInMB / KylinBuildEnv.get().kylinConfig.getSnapshotShardSizeMB).intValue()
      (num, sizeInMB)
    } catch {
      case t: Throwable =>
        logWarning("Error occurred when estimate repartition number.", t)
        (0, 0D)
    }
  }


  case class Result(path: String, originalSize: Long, totalRows: Long)

}
