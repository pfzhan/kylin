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

import java.util

import io.kyligence.kap.query.relnode.{KapJoinRel, KapNonEquiJoinRel}
import io.kyligence.kap.query.runtime.SparderRexVisitor
import io.kyligence.kap.query.util.KapRelUtil
import org.apache.calcite.DataContext
import org.apache.calcite.rex.RexCall
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._

object JoinPlan {
  def nonEquiJoin(inputs: java.util.List[DataFrame],
           rel: KapNonEquiJoinRel, dataContext: DataContext): DataFrame = {
    val lDataFrame = inputs.get(0)
    val rDataFrame = inputs.get(1)
    val lSchemaNames = lDataFrame.schema.fieldNames.map("l_" + _)
    val rSchemaNames = rDataFrame.schema.fieldNames.map("r_" + _)
    // val schema = statefulDF.indexSchema
    val newLDataFrame = inputs.get(0).toDF(lSchemaNames: _*)
    val newRDataFrame = inputs.get(1).toDF(rSchemaNames: _*)
    // slice lSchemaNames with rel.getLeftInputSizeBeforeRewrite
    // to strip off the fields added during rewrite
    // as those field will disturb the original index based join condition
    val visitor = new SparderRexVisitor(Array(lSchemaNames.slice(0, rel.getLeftInputSizeBeforeRewrite), rSchemaNames).flatten,
      null,
      dataContext)
    val pairs = new util.ArrayList[org.apache.kylin.common.util.Pair[Integer, Integer]]()
    val filterNuls = new util.ArrayList[java.lang.Boolean]()
    val actRemaining = KapRelUtil.isNotDistinctFrom(rel.getInput(0), rel.getInput(1), rel.getCondition, pairs, filterNuls)
    if (filterNuls.contains(java.lang.Boolean.FALSE)) {
      var equalCond = makeEqualCond(col(visitor.inputFieldNames.apply(pairs.get(0).getFirst)),
        col(visitor.inputFieldNames.apply(pairs.get(0).getSecond)), !filterNuls.get(0))

      var i = 1
      while (i < filterNuls.size()) {
        equalCond = equalCond.and(makeEqualCond(col(visitor.inputFieldNames.apply(pairs.get(i).getFirst)),
          col(visitor.inputFieldNames.apply(pairs.get(i).getSecond)), !filterNuls.get(i)))
        i = i + 1
      }

      if (actRemaining != null && actRemaining.isInstanceOf[RexCall]) {
        equalCond = equalCond.and(actRemaining.accept(visitor).asInstanceOf[Column])
      }

      newLDataFrame.join(newRDataFrame, equalCond, rel.getJoinType.lowerName)
    } else {
      val conditionExprCol = rel.getCondition.accept(visitor).asInstanceOf[Column]
      newLDataFrame.join(newRDataFrame, conditionExprCol, rel.getJoinType.lowerName)
    }
  }

  def join(inputs: java.util.List[DataFrame],
           rel: KapJoinRel): DataFrame = {

    val lDataFrame = inputs.get(0)
    val rDataFrame = inputs.get(1)
    val lSchemaNames = lDataFrame.schema.fieldNames.map("l_" + _)
    val rSchemaNames = rDataFrame.schema.fieldNames.map("r_" + _)
    // val schema = statefulDF.indexSchema
    val newLDataFrame = inputs.get(0).toDF(lSchemaNames: _*)
    val newRDataFrame = inputs.get(1).toDF(rSchemaNames: _*)
    var joinCol: Column = null

    //  todo   utils
    rel.getLeftKeys.asScala
      .zip(rel.getRightKeys.asScala)
      .foreach(tuple => {
        val col1 = col(lSchemaNames.apply(tuple._1))
        val col2 = col(rSchemaNames.apply(tuple._2))
        val equalCond = makeEqualCond(col1, col2, rel.isJoinCondEqualNullSafe)

        if (joinCol == null) {
          joinCol = equalCond
        } else {
          joinCol = joinCol.and(equalCond)
        }
      })
    if (joinCol == null) {
      newLDataFrame.crossJoin(newRDataFrame)
    } else {
      newLDataFrame.join(newRDataFrame, joinCol, rel.getJoinType.lowerName)
    }
  }

  def makeEqualCond(col1: Column, col2: Column, nullSafe: Boolean): Column = {
    if (nullSafe) {
      col1.eqNullSafe(col2)
    } else {
      col1.equalTo(col2)
    }
  }
}
