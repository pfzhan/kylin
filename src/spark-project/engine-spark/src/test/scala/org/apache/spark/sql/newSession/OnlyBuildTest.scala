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
package org.apache.spark.sql.newSession

import com.google.common.collect.Sets
import io.kyligence.kap.engine.spark.IndexDataConstructor
import io.kyligence.kap.job.util.JobContextUtil
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
import io.kyligence.kap.metadata.model.NDataModelManager.NDataModelUpdater
import io.kyligence.kap.metadata.model.{NDataModel, NDataModelManager}
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.sql.test.SQLTestUtils

import java.util

abstract class OnlyBuildTest extends SQLTestUtils with WithKylinExternalCatalog {

  val project = "file_pruning"
  protected override val ut_meta = "../kap-it/src/test/resources/ut_meta/file_pruning"
  val dfID = "8c670664-8d05-466a-802f-83c023b56c77"

  protected def storageType: Integer

  def setStorage(modelMgr: NDataModelManager, modelName: String): Unit = {
    case class Updater(storageType: Integer) extends NDataModelUpdater {
      override def modify(copyForWrite: NDataModel): Unit = copyForWrite.setStorageType(storageType)
    }
    if (storageType != 1) {
      modelMgr.updateDataModel(modelName, Updater(storageType))
    }
  }

  override def beforeAll(): Unit = {
    overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1")
    super.beforeAll()
    setStorage(NDataModelManager.getInstance(kylinConf, project), dfID)

    JobContextUtil.cleanUp()
    JobContextUtil.getJobContext(kylinConf)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    JobContextUtil.cleanUp()
  }

  test("testNonExistTimeRange") {
    val start: Long = SegmentRange.dateToLong("2023-01-01 00:00:00")
    val end: Long = SegmentRange.dateToLong("2025-01-01 00:00:00")
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(kylinConf, project)
    val df: NDataflow = dsMgr.getDataflow(dfID)
    val layouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    new IndexDataConstructor(project).buildIndex(dfID,
      new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts), true)
  }
}
