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

package io.kyligence.kap.engine.spark.job.stage

import java.util.Objects

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class BuildParamTest extends AnyFunSuite {
  test("basic") {
    val param = new BuildParam()
    assert(!param.isSkipGenerateFlatTable)

    param.setSkipGenerateFlatTable(true)
    assert(param.isSkipGenerateFlatTable)

    param.setSkipGenerateFlatTable(false)
    assert(!param.isSkipGenerateFlatTable)

    assert(!param.isSkipMaterializedFactTableView)

    param.setSkipMaterializedFactTableView(true)
    assert(param.isSkipMaterializedFactTableView)

    param.setSkipMaterializedFactTableView(false)
    assert(!param.isSkipMaterializedFactTableView)

    param.setCachedLayoutDS(mutable.HashMap[Long, Dataset[Row]]())
    assert(Objects.nonNull(param.getCachedLayoutDS))

    param.setCachedLayoutSanity(Some(Map(1L -> 10001L)))
    assert(param.getCachedLayoutSanity.isDefined)

    param.setCachedIndexInferior(Some(Map(1L -> InferiorGroup(null, null))))
    assert(param.getCachedIndexInferior.isDefined)
    assert(Objects.isNull(param.getCachedIndexInferior.get(1L).tableDS))
    assert(Objects.isNull(param.getCachedIndexInferior.get(1L).reapCount))
    val ig = InferiorGroup.unapply(param.getCachedIndexInferior.get(1L)).get
    assert(Objects.isNull(ig._1))
    assert(Objects.isNull(ig._2))
  }
}
