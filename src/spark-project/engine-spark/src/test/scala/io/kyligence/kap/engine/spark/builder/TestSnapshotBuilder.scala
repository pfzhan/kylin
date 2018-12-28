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
package io.kyligence.kap.engine.spark.builder

import com.google.common.collect.Maps
import io.kyligence.kap.cube.model.{NDataflow, NDataflowManager}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.persistence.ResourceStore
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

import scala.collection.JavaConverters._

class TestSnapshotBuilder extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"

  private val DF_NAME = "ncube_basic"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("snapshot -- check snapshot reuse") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var df: NDataflow = dsMgr.getDataflow(DF_NAME)
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + df.getProject + ResourceStore.SNAPSHOT_RESOURCE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)

    buildSnapshot(df, false, 1)
    buildSnapshot(df, false, 1)
    buildSnapshot(df, true, 2)
    buildSnapshot(df, true, 2)
  }

  private def buildSnapshot(df: NDataflow, isMock: Boolean, expectedSize: Int): Unit = {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + df.getProject + ResourceStore.SNAPSHOT_RESOURCE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem

    for (segment <- df.getSegments.asScala) {
      val dfCopy = segment.getDataflow.copy
      val segCopy = dfCopy.getSegment(segment.getId)
      segCopy.setSnapshots(Maps.newHashMap())
      var snapshotBuilder = new DFSnapshotBuilder(segCopy, spark)
      if (isMock) {
        snapshotBuilder = new MockDFSnapshotBuilder(segCopy, spark)
      }
      snapshotBuilder.buildSnapshot
    }

    for (fstatus <- fs.listStatus(new Path(snapPath))) {
      val list = fs.listStatus(fstatus.getPath)
      Assert.assertEquals(expectedSize, list.size)
    }
  }
}
