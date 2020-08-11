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

package io.kyligence.kap.engine.spark.job

import io.kyligence.kap.engine.spark.builder.CreateFlatTable.replaceDot
import io.kyligence.kap.query.util.KapQueryUtil
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

object FlatTableHelper extends Logging {

  def applyPartitionDesc(
                                  flatTable: IJoinedFlatTableDesc,
                                  ds: Dataset[Row],
                                  needReplaceDot: Boolean): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    val partDesc = model.getPartitionDesc
    if (partDesc != null && partDesc.getPartitionDateColumn != null) {
      val segRange = flatTable.getSegRange
      if (segRange != null && !segRange.isInfinite) {
        var afterConvertPartition = partDesc.getPartitionConditionBuilder
          .buildDateRangeCondition(partDesc, null, segRange)
        if (needReplaceDot) afterConvertPartition = replaceDot(afterConvertPartition, model)
        logInfo(s"Partition filter $afterConvertPartition")
        afterFilter = afterFilter.where(afterConvertPartition)
      }
    }

    afterFilter
  }

  def applyFilterCondition(
                                    flatTable: IJoinedFlatTableDesc,
                                    ds: Dataset[Row],
                                    needReplaceDot: Boolean): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      var filterCond = model.getFilterCondition
      filterCond = KapQueryUtil.massageExpression(model, model.getProject, filterCond, null);
      if (needReplaceDot) filterCond = replaceDot(filterCond, model)
      filterCond = s" (1=1) AND (" + filterCond + s")"
      logInfo(s"Filter condition is $filterCond")
      afterFilter = afterFilter.where(filterCond)
    }

    afterFilter
  }
}
