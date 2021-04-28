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
package io.kyligence.kap.engine.spark.job

import io.kyligence.kap.engine.spark.builder.SegmentBuildSource
import io.kyligence.kap.metadata.cube.model.LayoutEntity
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.DataFrame

import java.util


abstract class AbstractSanityCheckHandler[T <: SegmentBuildSource, Key] extends SanityCheckHandlerTrait[T, Long] {
  protected final val keyCountMap = new util.HashMap[Key, Long]

  override def getOrComputeFromFlatTable(buildSource: T, computeFunc: () => Long): Long = {
    getOrCompute(buildSource, computeFunc)
  }

  override def getOrComputeFromLayout(buildSource: T, parentDS: DataFrame, parentLayout: LayoutEntity): Long = {
    getOrCompute(buildSource, () => SanityChecker.getCount(parentDS, buildSource.getParent))
  }

  def getOrCompute(source: T, computeFunc: () => Long): Long = {
    if (!KylinConfig.getInstanceFromEnv.isSanityCheckEnabled) {
      return SanityChecker.SKIP_FLAG
    }

    val key = generateKey(source)

    if (!keyCountMap.containsKey(key)) {
      keyCountMap.put(key, computeFunc.apply())
    }
    getCountByKey(key)
  }

  def getCountByKey(key: Key): Long = {
    keyCountMap.get(key)
  }

  def generateKey(buildSource: T): Key
}
