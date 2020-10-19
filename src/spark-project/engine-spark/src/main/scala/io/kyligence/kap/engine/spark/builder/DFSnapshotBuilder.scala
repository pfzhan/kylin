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
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.{DFChooser, KylinBuildEnv}
import io.kyligence.kap.engine.spark.utils.{FileNames, LogUtils}
import io.kyligence.kap.metadata.cube.model.NDataSegment
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.TableDesc
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

class DFSnapshotBuilder extends Logging with Serializable {

  var ss: SparkSession = _
  var seg: NDataSegment = _
  var ignoredSnapshotTables: java.util.Set[String] = _

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024

  def this(seg: NDataSegment, ss: SparkSession) {
    this()
    this.seg = seg
    this.ss = ss
  }

  def this(seg: NDataSegment, ss: SparkSession, ignoredSnapshotTables: java.util.Set[String]) {
    this(seg, ss)
    this.ignoredSnapshotTables = ignoredSnapshotTables
  }

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

  @throws[IOException]
  def buildSnapshot: NDataSegment = {
    logInfo(s"Building snapshots for: $seg")
    val model = seg.getDataflow.getModel
    val newSnapMap = Maps.newHashMap[String, String]
    val snapSizeMap = new ConcurrentHashMap[String, Long]
    val fs = HadoopUtil.getWorkingFileSystem
    val baseDir = KapConfig.wrap(seg.getConfig).getMetadataWorkingDirectory
    val toBuildTableDesc = distinctTableDesc(model)
    val kylinConf = KylinConfig.getInstanceFromEnv
    if (seg.getConfig.isSnapshotParallelBuildEnabled) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc
        .map {
          tableDesc =>
            Future[(String, String)] {
              if (seg.getConfig.isUTEnv) {
                Thread.sleep(1000L)
              }
              try {
                KylinConfig.setAndUnsetThreadLocalConfig(kylinConf)
                buildSingleSnapshotWithoutMd5(tableDesc, baseDir, snapSizeMap)
              } catch {
                case exception: Exception =>
                  logError(s"Error for build snapshot table with $tableDesc", exception)
                  throw exception
              }
            }
        }
      // scalastyle:off
      try {
        val eventualTuples = Future.sequence(futures.toList)
        // only throw the first exception
        val result = ProxyThreadUtils.awaitResult(eventualTuples, seg.getConfig.snapshotParallelBuildTimeoutSeconds seconds)
        if (result.nonEmpty) {
          newSnapMap.putAll(result.toMap.asJava)
        }
      } catch {
        case e: Exception =>
          ProxyThreadUtils.shutdown(service)
          throw e
      }
    } else {
      toBuildTableDesc.foreach {
        tableDesc =>
          val tuple = buildSingleSnapshot(tableDesc, baseDir, fs, snapSizeMap)
          newSnapMap.put(tuple._1, tuple._2)
      }
    }

    // be careful
    // make a copy of the changing segment, avoid changing the cached object
    DFBuilderHelper.checkPointSegment(seg, (copied: NDataSegment) => {
      copied.getSnapshots.putAll(newSnapMap)
      copied.setSnapshotReady(true)
      for ((k: String, v: Long) <- snapSizeMap.asScala) {
        logInfo(s"Snapshot size is: $k : $v")
        copied.getOriSnapshotSize.put(k, v)
      }
    })
  }

  def isIgnoredSnapshotTable(tableDesc: TableDesc): Boolean = {
    if (ignoredSnapshotTables == null || tableDesc.getLastSnapshotPath == null) {
      return false
    }
    ignoredSnapshotTables.contains(tableDesc.getIdentity)

  }

  def distinctTableDesc(model: NDataModel): Set[TableDesc] = {
    val toBuildTableDesc = model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && seg.getSnapshots.get(tableDesc.getIdentity) == null && !isIgnoredSnapshotTable(tableDesc)
      })
      .map(_.getTableRef.getTableDesc)
      .toSet

    val toBuildTableDescTableName = toBuildTableDesc.map(_.getIdentity)
    logInfo(s"table snapshot to be build: $toBuildTableDescTableName")
    toBuildTableDesc
  }

  def getSourceData(tableDesc: TableDesc): Dataset[Row] = {
    SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, Maps.newHashMap[String, String])
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

  def buildSingleSnapshot(tableDesc: TableDesc, baseDir: String, fs: FileSystem, concurrentMap: ConcurrentMap[String, Long]): (String, String) = {
    val sourceData = getSourceData(tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
    var snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    sourceData.coalesce(1).write.parquet(resourcePath)
    val columnSize = sourceData.columns.length
    computeSnapshotSize(sourceData, tableDesc, concurrentMap, columnSize)
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
    (tableDesc.getIdentity, snapshotTablePath)
  }

  def computeSnapshotSize(sourceData: Dataset[Row], tableDesc: TableDesc, concurrentMap: ConcurrentMap[String, Long], columnSize: Int): Unit = {
    val ds = sourceData.mapPartitions {
      iter =>
        var totalSize = 0l;
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
      concurrentMap.put(tableDesc.getIdentity, 0)
    } else {
      val size = ds.reduce(_ + _)
      concurrentMap.put(tableDesc.getIdentity, size)
    }
  }

  def buildSingleSnapshotWithoutMd5(tableDesc: TableDesc, baseDir: String, concurrentMap: ConcurrentMap[String, Long]): (String, String) = {
    val sourceData = getSourceData(tableDesc)
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
    val columnSize = sourceData.columns.length
    computeSnapshotSize(sourceData, tableDesc, concurrentMap, columnSize)
    (tableDesc.getIdentity, snapshotTablePath)
  }
}
