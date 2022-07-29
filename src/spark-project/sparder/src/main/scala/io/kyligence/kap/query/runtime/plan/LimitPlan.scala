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

import io.kyligence.kap.query.relnode.KapLimitRel
import io.kyligence.kap.query.runtime.SparderRexVisitor
import org.apache.calcite.DataContext
import org.apache.spark.sql.DataFrame

// scalastyle:off
object LimitPlan {
  def limit(inputs: java.util.List[DataFrame],
    rel: KapLimitRel,
    dataContext: DataContext): DataFrame = {
    //    val schema = statefulDF.indexSchema
    val visitor = new SparderRexVisitor(inputs.get(0),
      rel.getInput.getRowType,
      dataContext)
    val limit = BigDecimal(rel.localFetch.accept(visitor).toString).toInt
    if (rel.localOffset == null) {
      inputs
        .get(0)
        .limit(limit)
    } else {
      val offset = BigDecimal(rel.localOffset.accept(visitor).toString).toInt
      inputs
        .get(0)
        .offset(offset).limit(limit)
    }
  }
}
