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

import io.kyligence.kap.guava20.shaded.common.collect.{Lists, Sets}
import io.kyligence.kap.metadata.cube.model.{IndexEntity, NDataSegDetails, NDataSegment, NDataflow, NDataflowManager}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.mockito.Mockito

class TestPartitionDictionaryBuilderHelper extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {
  private val DEFAULT_PROJECT = "default"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val MODEL_NAME2 = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94"
  private val MODEL_NAME3 = "741ca86a-1f13-46da-a59f-95fb68615e3a"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("test extractTreeRelatedGlobalDictToBuild mock") {
    val segment = Mockito.mock(classOf[NDataSegment])

    val toBuildIndexEntities = Sets.newHashSet[IndexEntity]
    val indexEntity = new IndexEntity()
    indexEntity.setLayouts(Lists.newArrayList())
    toBuildIndexEntities.add(indexEntity)

    Mockito.when(segment.getSegDetails).thenReturn(null)

    var result = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(segment, toBuildIndexEntities)
    assert(result.isEmpty)

    Mockito.when(segment.getSegDetails).thenReturn(new NDataSegDetails())
    result = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(segment, toBuildIndexEntities)
    assert(result.isEmpty)
  }
}
