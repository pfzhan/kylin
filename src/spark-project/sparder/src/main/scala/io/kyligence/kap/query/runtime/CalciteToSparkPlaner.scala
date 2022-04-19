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
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.query.relnode._
import io.kyligence.kap.query.runtime.plan._
import io.kyligence.kap.query.util.KapRelUtil
import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class CalciteToSparkPlaner(dataContext: DataContext) extends RelVisitor with LogEx {
  private val stack = new util.Stack[DataFrame]()
  private val setOpStack = new util.Stack[Int]()
  private var unionLayer = 0

  // clear cache before any op
  cleanCache()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[KapUnionRel]) {
      unionLayer = unionLayer + 1
    }
    if (node.isInstanceOf[KapUnionRel] || node.isInstanceOf[KapMinusRel]) {
      setOpStack.push(stack.size())
    }
    // skip non runtime joins children
    // cases to skip children visit
    // 1. current node is a KapJoinRel and is not a runtime join
    // 2. current node is a KapNonEquiJoinRel and is not a runtime join
    if (!(node.isInstanceOf[KapJoinRel] && !node.asInstanceOf[KapJoinRel].isRuntimeJoin) &&
      !(node.isInstanceOf[KapNonEquiJoinRel] && !node.asInstanceOf[KapNonEquiJoinRel].isRuntimeJoin)) {
      node.childrenAccept(this)
    }
    stack.push(node match {
      case rel: KapTableScan =>
        rel.genExecFunc() match {
          case "executeLookupTableQuery" =>
            logTime("createLookupTable") { TableScanPlan.createLookupTable(rel) }
          case "executeOLAPQuery" =>
            logTime("createOLAPTable") { TableScanPlan.createOLAPTable(rel) }
          case "executeSimpleAggregationQuery" =>
            logTime("createSingleRow") { TableScanPlan.createSingleRow() }
        }
      case rel: KapFilterRel =>
        logTime("filter") { FilterPlan.filter(Lists.newArrayList(stack.pop()), rel, dataContext) }
      case rel: KapProjectRel =>
        logTime("project") {
          actionWithCache(rel) {
            ProjectPlan.select(Lists.newArrayList(stack.pop()), rel, dataContext)
          }
        }
      case rel: KapLimitRel =>
        logTime("limit") { LimitPlan.limit(Lists.newArrayList(stack.pop()), rel, dataContext) }
      case rel: KapSortRel =>
        logTime("sort") { SortPlan.sort(Lists.newArrayList(stack.pop()), rel, dataContext) }
      case rel: KapWindowRel =>
        logTime("window") { WindowPlan.window(Lists.newArrayList(stack.pop()), rel, dataContext) }
      case rel: KapAggregateRel =>
        logTime("agg") {
          actionWithCache(rel) {
            AggregatePlan.agg(Lists.newArrayList(stack.pop()), rel)
          }
        }
      case rel: KapJoinRel =>
        if (!rel.isRuntimeJoin) {
          logTime("join with table scan") { TableScanPlan.createOLAPTable(rel) }
        } else {
          val right = stack.pop()
          val left = stack.pop()
          logTime("join") { plan.JoinPlan.join(Lists.newArrayList(left, right), rel) }
        }
      case rel: KapNonEquiJoinRel =>
        if (!rel.isRuntimeJoin) {
          logTime("join with table scan") {
            TableScanPlan.createOLAPTable(rel)
          }
        } else {
          val right = stack.pop()
          val left = stack.pop()
          logTime("non-equi join") {
            plan.JoinPlan.nonEquiJoin(Lists.newArrayList(left, right), rel, dataContext)
          }
        }
      case rel: KapUnionRel =>
        val size = setOpStack.pop()
        val java = Range(0, stack.size() - size).map(a => stack.pop()).asJava
        logTime("union") { plan.UnionPlan.union(Lists.newArrayList(java), rel, dataContext) }
      case rel: KapMinusRel =>
        val size = setOpStack.pop()
        logTime("minus") { plan.MinusPlan.minus(Range(0, stack.size() - size).map(a => stack.pop()).reverse, rel, dataContext) }
      case rel: KapValuesRel =>
        logTime("values") { ValuesPlan.values(rel) }
      case rel: KapModelViewRel =>
        logTime("modelview") { stack.pop() }
    })
    if (node.isInstanceOf[KapUnionRel]) {
      unionLayer = unionLayer - 1
    }
  }

  private def actionWithCache(rel: KapRel)(body: => DataFrame): DataFrame = {
    var layoutId = 0L
    var modelId = ""
    if (rel.getDigest == null) {
      body
    } else {
      try {
        val layoutEntity = rel.getContext.storageContext.getCandidate.getLayoutEntity
        layoutId = layoutEntity.getId
        modelId = layoutEntity.getModel.getId
      } catch {
        case e: Throwable => logWarning(s"Calculate layoutId modelId failed ex:${e.getMessage}")
      }
      rel.recomputeDigest()
      val digestWithoutId = KapRelUtil.getDigestWithoutRelNodeId(rel.getDigest, layoutId, modelId)
      if (unionLayer >= 1 && TableScanPlan.cacheDf.get.containsKey(digestWithoutId)) {
        stack.pop()
        logInfo("Happen Optimized from cache dataframe CacheKey:" + digestWithoutId)
        TableScanPlan.cacheDf.get.get(digestWithoutId)
      } else {
        val df = body
        if (unionLayer >= 1) {
          TableScanPlan.cacheDf.get.put(digestWithoutId, df)
        }
        df
      }
    }
  }

  def cleanCache(): Unit = {
    TableScanPlan.cacheDf.get().clear()
  }

  def getResult(): DataFrame = {
    stack.pop()
  }
}
