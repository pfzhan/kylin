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

import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.cube.model.{IndexEntity, LayoutEntity}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.KylinException
import org.apache.kylin.common.exception.code.ErrorCodeJob.SANITY_CHECK_ERROR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.datasource.storage.StorageListener
import org.apache.spark.sql.functions.{col, sum}

import scala.collection.JavaConverters._

class SanityChecker(expect: Long) extends StorageListener with LogEx {

  override def onPersistBeforeRepartition(dataFrame: DataFrame, layout: LayoutEntity): Unit = {
    // just implement it
  }

  override def onPersistAfterRepartition(dataFrame: DataFrame, layout: LayoutEntity): Unit = {
    if (!KylinConfig.getInstanceFromEnv.isSanityCheckEnabled) {
      return
    }
    if (expect == SanityChecker.SKIP_FLAG) {
      log.info(s"Cannot find count constant measure in root, skip sanity check")
      return
    }

    val actual = SanityChecker.getCount(dataFrame, layout)

    if (actual == SanityChecker.SKIP_FLAG) {
      log.info(s"Cannot find count constant measure in current cuboid, skip sanity check")
      return
    }

    checkRowCount(actual, expect, layout)
  }

  private def checkRowCount(dfRowCount: Long, parentRowCount: Long, layout: LayoutEntity): Unit = {
    if (dfRowCount != parentRowCount) {
      throw new KylinException(SANITY_CHECK_ERROR, layout.getId + "")
    }
  }
}

object SanityChecker extends LogEx {

  val SKIP_FLAG: Long = -1L

  def getCount(df: DataFrame, layout: LayoutEntity): Long = {
    if (IndexEntity.isTableIndex(layout.getId)) {
      df.count()
    } else {
      layout.getOrderedMeasures.values().asScala.find(m => m.getFunction.isCountConstant && !m.isTomb) match {
        case Some(countMeasure) =>
          val countMeasureCol = countMeasure.getId.toString
          val row = df.select(countMeasureCol).agg(sum(col(countMeasureCol))).collect().head
          if (row.isNullAt(0)) 0L else row.getLong(0)
        case _ =>
          SKIP_FLAG
      }
    }
  }
}

