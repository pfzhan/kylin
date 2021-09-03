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

import java.util.concurrent.TimeoutException
import io.kyligence.kap.metadata.model.{NDataModel, NDataModelManager, NTableMetadataManager}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.SparkException
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

class TestSnapshotBuilder extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"

  // this model contain 7 dim table
  private val MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"

  override val master = "local[1]"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("snapshot -- check snapshot reuse") {
    val dataModel = getModel(MODEL_ID)

    overwriteSystemProp("kylin.snapshot.parallel-build-enabled", "false")
    buildSnapshot(dataModel, isMock = false, 1, null)
    buildSnapshot(dataModel, isMock = false, 1, null)
    buildSnapshot(dataModel, isMock = true, 2, null)
    buildSnapshot(dataModel, isMock = true, 2, null)
  }

  test("test concurrent snapshot success") {
    overwriteSystemProp("kylin.snapshot.parallel-build-enabled", "true")
    buildSnapshot(getModel(MODEL_ID))
  }

  test("test concurrent snapshot with timeout") {
    overwriteSystemProp("kylin.snapshot.parallel-build-timeout-seconds", "0")
    try {
      buildSnapshot(getModel(MODEL_ID))
      Assert.fail("build successfully, but this test should throw TimeoutException")
    } catch {
      case _: TimeoutException =>
      case e =>
        e.printStackTrace()
        Assert.fail(s"This test should throw TimeoutException")
    }
  }

  test("test concurrent snapshot with build error") {
    spark.stop()
    try {
      buildSnapshot(getModel(MODEL_ID))
      Assert.fail("This test should throw SparkException")
    } catch {
      case _: SparkException =>
      case e => Assert.fail(s"This test should throw SparkException, but it is ${e.getStackTrace.mkString("\n")}")
    }
    super.beforeAll()
  }


  private def buildSnapshot(dm: NDataModel): Unit = {
    buildSnapshot(dm, false, 1, null)
  }

  private def buildSnapshot(dm: NDataModel, isMock: Boolean, expectedSize: Int, ignoredSnapshotTables: java.util.Set[String]): Unit = {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + dm.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem

    var snapshotBuilder = new SnapshotBuilder()
    if (isMock) {
      snapshotBuilder = new MockSnapshotBuilder()
    }

    // using getModel not dm is due to snapshot building do metaupdate
    snapshotBuilder.buildSnapshot(spark, getModel(dm.getId), ignoredSnapshotTables)
    Assert.assertEquals(snapshotBuilder.distinctTableDesc(dm, ignoredSnapshotTables).size, 7)

    val statuses = fs.listStatus(new Path(snapPath))
    for (fst <- statuses) {
      val list = fs.listStatus(fst.getPath)
      Assert.assertEquals(expectedSize, list.size)
    }

    val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, dm.getProject)
    for (table <- snapshotBuilder.distinctTableDesc(dm, ignoredSnapshotTables)) {
      val tableMeta = tableMetadataManager.getTableDesc(table.getIdentity)
      Assert.assertNotNull(tableMeta.getLastSnapshotPath)
      Assert.assertNotEquals(tableMeta.getLastSnapshotSize, 0)
    }
  }


  def getModel(modelId: String): NDataModel = {
    NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT).getDataModelDesc(modelId)
  }

  def clearSnapshot(project: String) {
    val snapPath = KapConfig.wrap(getTestConfig).getReadHdfsWorkingDirectory + project + HadoopUtil.SNAPSHOT_STORAGE_ROOT
    val fs = HadoopUtil.getWorkingFileSystem
    fs.delete(new Path(snapPath), true)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clearSnapshot(DEFAULT_PROJECT)
  }

}
