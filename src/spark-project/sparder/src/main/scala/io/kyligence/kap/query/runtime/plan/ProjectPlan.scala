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

import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.query.relnode.KapProjectRel
import io.kyligence.kap.query.runtime.SparderRexVisitor
import org.apache.calcite.DataContext
import org.apache.calcite.rex.RexInputRef
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.KapFunctions._

import scala.collection.JavaConverters._


object ProjectPlan extends LogEx {
  def select(inputs: java.util.List[DataFrame],
             rel: KapProjectRel,
             dataContext: DataContext): DataFrame = logTime("project", debug = true) {
    val df = inputs.get(0)
    val duplicatedColumnsCount = collection.mutable.Map[Column, Int]()
    val selectedColumns = rel.rewriteProjects.asScala
      .map(rex => {
        val visitor = new SparderRexVisitor(df,
          rel.getInput.getRowType,
          dataContext)
        (rex.accept(visitor), rex.isInstanceOf[RexInputRef])
      })
      .zipWithIndex
      .map(c => {
        //  add p0,p1 suffix for window queries will generate
        // indicator columns like false,false,false
        if (c._1._2) {
          k_lit(c._1._1)
        } else {
          k_lit(c._1._1).as(s"${System.identityHashCode(rel)}_prj${c._2}")
        }
      })
      .map(c => { // find and rename the duplicated columns KAP#16751
        if (!(duplicatedColumnsCount contains c)) {
          duplicatedColumnsCount += (c -> 0)
          c
        } else {
          val columnCnt = duplicatedColumnsCount(c) + 1
          duplicatedColumnsCount += (c -> columnCnt)
          c.as(s"${c.toString}_duplicated$columnCnt")
        }
      })

    df.select(selectedColumns: _*)
  }
}
