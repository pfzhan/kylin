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
package io.kyligence.kap.query.runtime.plan

import io.kyligence.kap.query.relnode.KapFilterRel
import io.kyligence.kap.query.runtime.SparderRexVisitor
import org.apache.calcite.DataContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}

object FilterPlan extends Logging {
  def filter(
              inputs: java.util.List[DataFrame],
              rel: KapFilterRel,
              dataContext: DataContext): DataFrame = {
    val start = System.currentTimeMillis()

    val df = inputs.get(0)
    val visitor = new SparderRexVisitor(df,
      rel.getInput.getRowType,
      dataContext)
    val filterColumn = rel.getCondition.accept(visitor).asInstanceOf[Column]
    val filterPlan = df.filter(filterColumn)
    logInfo(s"Gen filter cost Time :${System.currentTimeMillis() - start} ")
    filterPlan
  }
}
