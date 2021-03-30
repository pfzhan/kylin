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

package io.kyligence.kap.engine.spark.builder

import io.kyligence.kap.engine.spark.job.FiltersUtil
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThanOrEqual}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.junit.Assert

import scala.collection.JavaConverters._
import scala.collection.mutable.Set


class TestInferFilters extends SparderBaseFunSuite with AdaptiveSparkPlanHelper with SharedSparkSession with LocalMetadata {


  private val PROJECT = "infer_filter"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309ab"

  def getTestConfig: KylinConfig = {
    KylinConfig.getInstanceFromEnv
  }

  override def beforeEach(): Unit = {
    SegmentFlatTable.inferFiltersEnabled = true
  }

  override def afterEach(): Unit = {
    SegmentFlatTable.inferFiltersEnabled = false
  }

  test("infer filters from join desc") {
    getTestConfig.setProperty("kylin.engine.persist-flattable-enabled", "false")
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val seg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val toBuildTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, MODEL_NAME1)
    val flatTableDesc = new SegmentFlatTableDesc(getTestConfig, seg, toBuildTree)
    val flatTable = new SegmentFlatTable(spark, flatTableDesc)

    val filters = getFilterPlan(flatTable.getDS().queryExecution.executedPlan)

    Assert.assertTrue(Set("EDW.TEST_CAL_DT.CAL_DT", "DEFAULT.TEST_KYLIN_FACT.CAL_DT",
      "DEFAULT.TEST_ORDER.TEST_DATE_ENC").subsetOf(FiltersUtil.getAllEqualColSets))
    Assert.assertEquals(filters.size, 3)

  }

  private def getFilterPlan(plan: SparkPlan): Set[SparkPlan] = {
    val filterPlanSet: Set[SparkPlan] = Set.empty[SparkPlan]
    foreach(plan) {
      case node: FilterExec =>
        splitConjunctivePredicates(node.condition).find { p =>
          p.isInstanceOf[GreaterThanOrEqual]
        } match {
          case Some(x) =>
            filterPlanSet.add(node)
          case None =>
        }
      case _ =>
    }
    filterPlanSet
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }
}
