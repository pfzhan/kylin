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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.types._

// Returns the date that is num_months after start_date.
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    "_FUNC_(start_date, num_months) - Returns the date that is `num_months` after `start_date`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 1);
       2016-09-30
  """
)
// scalastyle:on line.size.limit
case class KapAddMonths(startDate: Expression, numMonths: Expression)
    extends BinaryExpression
    with ImplicitCastInputTypes {

  override def left: Expression = startDate

  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, IntegerType)

  override def dataType: TimestampType = TimestampType

  override def nullSafeEval(start: Any, months: Any): Any = {
    val time = start.asInstanceOf[Long]
    val month = months.asInstanceOf[Int]
    KapDateTimeUtils.addMonths(time, month)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.addMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "kap_add_months"
}

// Returns the date that is num_months after start_date.
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    "_FUNC_(date0, date1) - Returns the num of months between `date0` after `date1`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-08-31', '2017-08-31');
       12
  """
)
// scalastyle:on line.size.limit
case class KapSubtractMonths(a: Expression, b: Expression)
    extends BinaryExpression
    with ImplicitCastInputTypes {

  override def left: Expression = a

  override def right: Expression = b

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def nullSafeEval(date0: Any, date1: Any): Any = {
    KapDateTimeUtils.dateSubtractMonths(date0.asInstanceOf[Int],
                                        date1.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d0, d1) => {
      s"""$dtu.dateSubtractMonths($d0, $d1)"""
    })
  }

  override def prettyName: String = "kap_months_between"
}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group. " +
    "It differs in that when no non null values are applied zero is returned instead of null")
case class Sum0(child: Expression)
    extends DeclarativeAggregate
    with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _               => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()

  private lazy val zero = Cast(Literal(0), sumDataType)

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
//    /* sum = */ Literal.create(0, sumDataType)
//    /* sum = */ Literal.create(null, sumDataType)
    Cast(Literal(0), sumDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        Coalesce(
          Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum))
      )
    } else {
      Seq(
        /* sum = */
        Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType))
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left))
    )
  }

  override lazy val evaluateExpression: Expression = sum
}

case class KapDayOfWeek(a: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {

  override def child: Expression = a

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d) => {
      s"""$dtu.dayOfWeek($d)"""
    })
  }

  override def nullSafeEval(date: Any): Any = {
    KapDateTimeUtils.dayOfWeek(date.asInstanceOf[Int])
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "kap_day_of_week"
}
