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

import com.google.common.collect.Maps
import io.kyligence.kap.cube.model.{NDataSegment, NDataflowManager, NDataflowUpdate}
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.persistence.ResourceStore
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class DFSnapshotBuilder extends Logging {

  var ss: SparkSession = _
  var seg: NDataSegment = _

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"

  def this(seg: NDataSegment, ss: SparkSession) {
    this()
    this.seg = seg
    this.ss = ss
  }

  private val ParquetPathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(PARQUET_SUFFIX)
    }
  }

  private val Md5PathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(MD5_SUFFIX)
    }
  }

  @throws[IOException]
  def buildSnapshot: NDataSegment = {
    logInfo(s"building snapshots for: $seg")
    val model = seg.getDataflow.getModel
    val newSnapMap = Maps.newHashMap[String, String]
    val fs = HadoopUtil.getWorkingFileSystem
    val baseDir = KapConfig.wrap(seg.getConfig).getReadHdfsWorkingDirectory

    model.getJoinTables.asScala.foreach(lookupDesc => {
      val tableDesc = lookupDesc.getTableRef.getTableDesc
      val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
      if (isLookupTable && seg.getSnapshots.get(tableDesc.getIdentity) == null) {
        val sourceData = getSourceData(tableDesc)
        val tablePath = tableDesc.getProject + ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + tableDesc.getName
        var snapshotTablePath = tablePath + "/" + UUID.randomUUID
        val resourcePath = baseDir + "/" + snapshotTablePath
        sourceData.coalesce(1).write.parquet(resourcePath)

        val currSnapFile = fs.listStatus(new Path(resourcePath), ParquetPathFilter).head
        val currSnapMd5 = getFileMd5(currSnapFile)
        val md5Path = resourcePath + "/" + "_" + currSnapMd5 + MD5_SUFFIX
        fs.createNewFile(new Path(md5Path))

        val existPath = baseDir + "/" + tablePath
        val existSnaps = fs.listStatus(new Path(existPath))
          .filterNot(_.getPath.getName == new Path(snapshotTablePath).getName)
        breakable {
          for (snap <- existSnaps) {
            val existsSnapMd5 = fs.listStatus(snap.getPath, Md5PathFilter).head
            if (existsSnapMd5 == null) {
              logInfo(s"snapshot path: ${snap.getPath} not exists snapshot file")
            } else {
              val md5Snap = existsSnapMd5.getPath.getName
                .replace(MD5_SUFFIX, "")
                .replace("_", "")
              if (currSnapMd5 == md5Snap) {
                snapshotTablePath = tablePath + "/" + snap.getPath.getName
                fs.delete(new Path(resourcePath), true)
                break()
              }
            }
          }
        }


        newSnapMap.put(tableDesc.getIdentity, snapshotTablePath)
      }
    })

    val dataflow = seg.getDataflow
    // make a copy of the changing segment, avoid changing the cached object
    val dfCopy = dataflow.copy
    val segCopy = dfCopy.getSegment(seg.getId)
    val update = new NDataflowUpdate(dataflow.getUuid)
    segCopy.getSnapshots.putAll(newSnapMap)
    update.setToUpdateSegs(segCopy)
    val updatedDataflow = NDataflowManager.getInstance(seg.getConfig, seg.getProject).updateDataflow(update)
    updatedDataflow.getSegment(seg.getId)
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

}
