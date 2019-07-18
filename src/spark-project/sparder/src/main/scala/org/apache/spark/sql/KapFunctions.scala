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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, In, KapAddMonths, KapDayOfWeek, KapSubtractMonths, Like, Literal, RoundBase, Sum0, TimestampAdd, TimestampDiff, Truncate}
import org.apache.spark.sql.udf.{ApproxCountDistinct, PreciseCountDistinct}

object KapFunctions {
  private def withAggregateFunction(func: AggregateFunction,
                                    isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def kap_add_months(startDate: Column, numMonths: Column): Column = {
    Column(KapAddMonths(startDate.expr, numMonths.expr))
  }

  def kap_subtract_months(date0: Column, date1: Column): Column = {
    Column(KapSubtractMonths(date0.expr, date1.expr))
  }

  def kap_day_of_week(date: Column): Column = Column(KapDayOfWeek(date.expr))

  def k_like(left:Column, right:Column):Column = Column(Like(left.expr, right.expr))

  def sum0(e: Column): Column = withAggregateFunction {
    Sum0(e.expr)
  }

  // special lit for KE.
  def k_lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ => Column(Literal(literal))
  }

  def in(value: Expression, list: Seq[Expression]): Column = Column(In(value, list))

  def precise_count_distinct(column: Column): Column =
    Column(PreciseCountDistinct(column.expr).toAggregateExpression())

  def approx_count_distinct(column: Column, precision: Int): Column =
    Column(ApproxCountDistinct(column.expr, precision).toAggregateExpression())

  def kap_truncate(column: Column, scale: Int): Column = {
    Column(TRUNCATE(column.expr, Literal(scale)))
  }

  case class TRUNCATE(child: Expression, scale: Expression)
    extends RoundBase(child, scale, BigDecimal.RoundingMode.DOWN, "DOWN")
      with Serializable with ImplicitCastInputTypes {
    def this(child: Expression) = this(child, Literal(0))
  }


  val builtin: Seq[FunctionEntity] = Seq(
    FunctionEntity(expression[TimestampAdd]("TIMESTAMPADD")),
    FunctionEntity(expression[TimestampDiff]("TIMESTAMPDIFF")),
    FunctionEntity(expression[Truncate]("TRUNCATE")))
}

case class FunctionEntity(name: FunctionIdentifier,
                          info: ExpressionInfo,
                          builder: FunctionBuilder)

object FunctionEntity {
  def apply(tuple: (String, (ExpressionInfo, FunctionBuilder))): FunctionEntity = {
    new FunctionEntity(FunctionIdentifier.apply(tuple._1), tuple._2._1, tuple._2._2)
  }
}
