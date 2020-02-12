/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */
package io.kyligence.kap.engine.spark.streaming

import java.util.HashMap

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.streaming.util.MetaInfoUpdater
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataLayout, NDataSegment}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.storage.StorageFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object StreamingCommitter extends Logging {

  val fs = HadoopUtil.getWorkingFileSystem
  val TEMP_DIR_SUFFIX = "_temp"
  val cuboidRowCount = new HashMap[Long, Long]()

  def saveAndCachedataset(dataset: Dataset[Row], ss: SparkSession,
                          layout: LayoutEntity): Unit = {
    dataset.persist(StorageLevel.MEMORY_ONLY)
    var start = System.currentTimeMillis()
    val rowsNum = dataset.count()
    cuboidRowCount.put(layout.getId, rowsNum)
    logInfo(s"eval rowNum cost time ${System.currentTimeMillis() - start}")
  }

  def commit(ss: SparkSession, cuboidsDatasets: HashMap[java.lang.Long, Dataset[Row]], seg: NDataSegment, project: String): Unit = {

    val cuboidPar = cuboidsDatasets.asScala.par
    cuboidPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(cuboidsDatasets.size()))

    val start = System.currentTimeMillis()
    logInfo("start save all file")
    cuboidPar.foreach { case (layoutId, dataset) =>
      ss.sparkContext.setLocalProperty("spark.scheduler.pool", "build")
      logInfo(s"cuboid ${layoutId} dataset start save to file")
      val layout = seg.getLayout(layoutId)

      logInfo(s"cuboid ${layout.getLayoutId} save start")
      val path = NSparkCubingUtil.getStoragePath(seg, layoutId)
      val tempPath = path + TEMP_DIR_SUFFIX;
      val dimsCols = NSparkCubingUtil.getColumns(layout.getLayout.getOrderedDimensions.keySet())
      val start = System.currentTimeMillis()
      val afterSort = dataset.sortWithinPartitions(dimsCols: _*).repartition(1)
      afterSort.write.mode(SaveMode.Overwrite).parquet(tempPath)
      movePath(tempPath, path)
      fillCuboidInfo(layout, path)
      layout.setPartitionNum(1)
      MetaInfoUpdater.update(project, seg, layout)
      logInfo(s"cuboid ${layout.getLayoutId} save  cost ${System.currentTimeMillis() - start}")
    }

    logInfo(s"save all file cost ${System.currentTimeMillis() - start}")
    cuboidPar.foreach { case (layoutId, dataset) =>
      val start = System.currentTimeMillis()
      dataset.unpersist(false)
      logInfo(s"unpersist cuboid : ${layoutId} dataset cost ${System.currentTimeMillis() - start}")
    }
  }


  def fillCuboidInfo(cuboid: NDataLayout, strPath: String): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem
    if (fs.exists(new Path(strPath))) {
      val cs = HadoopUtil.getContentSummary(fs, new Path(strPath))
      cuboid.setFileCount(cs.getFileCount)
      cuboid.setByteSize(cs.getLength)
    } else {
      cuboid.setFileCount(0)
      cuboid.setByteSize(0)
    }
    cuboid.setRows(cuboidRowCount.getOrDefault(cuboid.getLayoutId, 0))
  }


  def movePath(tempPath: String, goalPath: String): Unit = {
    if (fs.rename(new Path(tempPath), new Path((goalPath)))) {
      logInfo(s"Rename temp path to target path successfully. Temp path: ${tempPath}, target path: ${goalPath}.")
    } else {
      throw new RuntimeException(s"Rename temp path to target path wrong. Temp path: ${tempPath}, target path: ${goalPath}.")
    }
  }

}
