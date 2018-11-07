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

package io.kyligence.kap.query.runtime

import java.util

import com.google.common.collect.Lists
import io.kyligence.kap.query.relnode._
import io.kyligence.kap.query.runtime.plan.{
  AggregatePlan,
  FilterPlan,
  LimitPlan,
  ProjectPlan,
  SortPlan,
  TableScanPlan,
  ValuesPlan,
  WindowPlan
}
import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class CalciteToSparkPlaner(dataContext: DataContext) extends RelVisitor {
  private val stack = new util.Stack[DataFrame]()
  private val unionStack = new util.Stack[Int]()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[KapUnionRel]) {
      unionStack.push(stack.size())
    }
    if (!node.isInstanceOf[KapJoinRel]) {
      node.childrenAccept(this)
    } else if (node.asInstanceOf[KapJoinRel].isRuntimeJoin) {
      node.childrenAccept(this)
    }
    stack.push(node match {
      case rel: KapTableScan =>
        rel.genExecFunc() match {
          case "executeLookupTableQuery" =>
            TableScanPlan.createLookupTable(rel, dataContext)
          case "executeOLAPQuery" =>
            TableScanPlan.createOLAPTable(rel, dataContext);
        }
      case rel: KapFilterRel =>
        FilterPlan.filter(Lists.newArrayList(stack.pop()), rel, dataContext)
      case rel: KapProjectRel =>
        ProjectPlan.select(Lists.newArrayList(stack.pop()), rel, dataContext)
      case rel: KapLimitRel =>
        LimitPlan.limit(Lists.newArrayList(stack.pop()), rel, dataContext)
      case rel: KapSortRel =>
        SortPlan.sort(Lists.newArrayList(stack.pop()), rel, dataContext)
      case rel: KapWindowRel =>
        WindowPlan.window(Lists.newArrayList(stack.pop()), rel, dataContext)
      case rel: KapAggregateRel =>
        AggregatePlan.agg(Lists.newArrayList(stack.pop()), rel, dataContext)

      case rel: KapJoinRel =>
        if (!rel.isRuntimeJoin) {
          TableScanPlan.createOLAPTable(rel, dataContext)
        } else {
          val right = stack.pop()
          val left = stack.pop()
          plan.JoinPlan.join(Lists.newArrayList(left, right), rel)
        }

      case rel: KapUnionRel =>
        val size = unionStack.pop()
        val java = Range(0, stack.size() - size).map(a => stack.pop()).asJava
        plan.UnionPlan.union(Lists.newArrayList(java), rel, dataContext)
      case rel: KapValuesRel =>
        ValuesPlan.values(rel)
    })
  }

  def getResult(): DataFrame = {
    stack.pop()
  }
}
