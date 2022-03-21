/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2.pushdown

import java.util.Locale
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Alias, AliasHelper, And, Asin, Atan, Atan2, AttributeMap, AttributeReference, CaseWhen, Cast, Concat, Cos, DayOfMonth, DayOfYear, Exp, Expression, Floor, Hour, IntegerLiteral, Literal, Lower, Minute, Month, NamedExpression, Pow, ProjectionOverSchema, Quarter, RegExpReplace, Remainder, ScalaUDF, Second, Signum, Sin, SortOrder, StringLocate, StringTrimLeft, StringTrimRight, Substring, Tan, ToDegrees, ToRadians, TruncTimestamp}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, GlobalLimit, GlobalLimitAndOffset, Limit, LocalLimit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
import org.apache.spark.sql.connector.read.sqlpushdown.{NotSupportPushDown, SupportsSQL, SupportsSQLPushDown}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PushableColumnWithoutNestedColumn}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation, PushDownUtils2, V1ScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.{OrderDesc, PushDownAggUtils, SQLBuilder, SingleCatalystStatement}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{NullType, StructType}

abstract sealed class PushQuery extends Logging {
  def push(): LogicalPlan
}

case class OldScanPush(
    project: Seq[NamedExpression],
    filters: Seq[Expression],
    scanRelation: DataSourceV2ScanRelation) extends PushQuery {
  override def push(): LogicalPlan = scanRelation
}

class OldPush (
    project: Seq[NamedExpression],
    filters: Seq[Expression],
    relation: DataSourceV2Relation,
    scanBuilder: ScanBuilder,
    sortOpt: Option[Sort] = None,
    limitOpt: Option[GlobalLimit] = None,
    limitAndOffsetOpt: Option[GlobalLimitAndOffset] = None) extends PushQuery {

  private[pushdown] lazy val(pushedFilters, postScanFilters) =
    PushDownUtils2.pushDownFilter(scanBuilder, filters, relation)

  private[pushdown] lazy val normalizedProjects = DataSourceStrategy
    .normalizeExprs(project, relation.output)
    .asInstanceOf[Seq[NamedExpression]]

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the `Scan` instance (since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  def pruningColumns(): (Scan, Seq[AttributeReference]) = {
    PushDownUtils2.pruneColumns(
      scanBuilder, relation, normalizedProjects, postScanFilters)
  }

  def newScanRelation(): DataSourceV2ScanRelation = {
    val (scan, output) = pruningColumns()

    logInfo(
      s"""
         |Pushing operators to ${relation.name}
         |Pushed Filters: ${pushedFilters.mkString(", ")}
         |Post-Scan Filters: ${postScanFilters.mkString(",")}
         |Output: ${output.mkString(", ")}
         """.stripMargin)

    val wrappedScan = scan match {
      case v1: V1Scan =>
        V1ScanWrapper(v1, pushedFilters, None)
      case _ => scan
    }
    DataSourceV2ScanRelation(relation, wrappedScan, output)
  }

  private def buildPushedPlan(scanRelation: DataSourceV2ScanRelation): LogicalPlan = {
    val projectionOverSchema = ProjectionOverSchema(scanRelation.output.toStructType)
    val projectionFunc = (expr: Expression) => expr transformDown {
      case projectionOverSchema(newExpr) => newExpr
    }
    val filterCondition = postScanFilters.reduceLeftOption(And)
    val newFilterCondition = filterCondition.map(projectionFunc)
    val withFilter = newFilterCondition.map(logical.Filter(_, scanRelation)).getOrElse(scanRelation)

    val withProjection = if (withFilter.output != project) {
      val newProjects = normalizedProjects
        .map(projectionFunc)
        .asInstanceOf[Seq[NamedExpression]]
      Project(newProjects, withFilter)
    } else {
      withFilter
    }

    val withSort = if (sortOpt.isDefined) {
      val originSort = sortOpt.get
      Sort(originSort.order, originSort.global, withProjection)
    } else {
      withProjection
    }

    val withLimit = limitOpt.map {
      case g @ GlobalLimit(_, l @ LocalLimit(_, _)) =>
        g.copy(child = l.copy(child = withSort))
    }.getOrElse(withSort)

    val withLimitAndOffset = limitAndOffsetOpt.map {
      case g @GlobalLimitAndOffset(_, _, l @ LocalLimit(_, _)) =>
        g.copy(child = l.copy(child = withSort))
    }.getOrElse(withLimit)

    withLimitAndOffset
  }

  override def push(): LogicalPlan = {
    buildPushedPlan(newScanRelation())
  }
}

case class PushScanQuery(
    project: Seq[NamedExpression],
    filters: Seq[Expression],
    relation: DataSourceV2Relation,
    scanBuilder: SupportsSQLPushDown,
    sortOpt: Option[Sort] = None,
    limitOpt: Option[GlobalLimit] = None,
    limitAndOffsetOpt: Option[GlobalLimitAndOffset] = None)
  extends OldPush(project, filters, relation, scanBuilder, sortOpt, limitOpt, limitAndOffsetOpt) {

  /**
   * Sometimes, the order by col is an alias, not the real column name in ClickHouse.
   * We should replace it with the real one.
   */
  private def translateSortOrder(sortOrder: SortOrder): SortOrder = {
    val aliasToAttr = project.collect {
      case Alias(child: AttributeReference, name: String) => (name, child)
    }.toMap

    sortOrder match {
      case order @ SortOrder(PushableColumnWithoutNestedColumn(name), _, _, _)
        if aliasToAttr.contains(name) =>
        order.copy(child = aliasToAttr(name))
      case other => other
    }
  }

  override def pruningColumns(): (Scan, Seq[AttributeReference]) = {
    val prunedSchema = PushDownUtils2.prunedColumns(
      scanBuilder, relation, normalizedProjects, postScanFilters)

    prunedSchema.map { prunedSchema =>
      scanBuilder.pruneColumns(prunedSchema)
      val output = PushDownUtils2.toOutputAttrs(prunedSchema, relation)
      val orders: Seq[SortOrder] = sortOpt.map(_.order.map(translateSortOrder)).getOrElse(Seq.empty)
      val localLimit = limitOpt.map(_.child).orElse(limitAndOffsetOpt.map(_.child))
      val limit = localLimit.map(_.asInstanceOf[LocalLimit].limitExpr).map(IntegerLiteral.unapply(_).get)
      val pushStatement = SingleCatalystStatement.of(relation, output, pushedFilters, Seq.empty, orders, limit)
      /**
       * output schema set by `SupportsPushDownRequiredColumns#pruneColumns`
       */
      scanBuilder.pushStatement(pushStatement, null)
      scanBuilder.build() -> output
    }.getOrElse( scanBuilder.build() -> relation.output)
  }
}

case class PushAggregateQuery(
    groupingExpressions: Seq[Expression],
    resultExpressions: Seq[NamedExpression],
    child: PushQuery) extends PushQuery with AliasHelper {

  private val scanChild = child.asInstanceOf[PushScanQuery]

  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final lazy val alias = Iterator.from(0).map(n => s"pag_$n")
  private final lazy val aliasMap = getAliasMap(scanChild.project)

  override def push(): LogicalPlan = {

    val aggregateExpressionsToAliases =
      PushDownAggUtils.getAggregationToPushedAliasMap(resultExpressions, Some(() => alias.next()))

    val namedGroupingExpressions =
      PushDownAggUtils.getNamedGroupingExpressions(groupingExpressions, resultExpressions)

    // make expression out of the tuples
    val pushedPartialGroupings = namedGroupingExpressions.map(_._2)

    val pushedPartialAggregates =
      PushDownAggUtils.getPushDownNameExpression(resultExpressions ++ pushedPartialGroupings,
        aggregateExpressionsToAliases)

    /** This step is separate to keep the input order of the groupingExpressions */
    val rewrittenResultExpressions = PushDownAggUtils.rewriteResultExpressions(resultExpressions,
      aggregateExpressionsToAliases, namedGroupingExpressions.toMap)

    val output = pushedPartialAggregates
      .map(_.toAttribute)
      .asInstanceOf[Seq[AttributeReference]]

    val scanRelation = newScanRelation(pushedPartialAggregates, pushedPartialGroupings, output)

    Aggregate(
      groupingExpressions = groupingExpressions,
      aggregateExpressions = rewrittenResultExpressions,
      child = scanRelation)
  }

  def replaceAliasMayKeepName(oldAttr: AttributeReference): NamedExpression = {
    aliasMap.get(oldAttr).map {
      case Alias(a: AttributeReference, _) => a
      case x => x
    }.getOrElse(oldAttr)
  }

  def newScanRelation(
      aggregations: Seq[NamedExpression],
      groupBy: Seq[NamedExpression],
      output: Seq[AttributeReference]): LogicalPlan = {
    val SQLPushDown = scanChild.scanBuilder

    val outputAndProjectMap = output.map{ attr =>
      aliasMap.get(attr)
        .flatMap { alias =>
          alias match {
            case Alias(attr: AttributeReference, _) => Some(attr -> alias)
            case _ => None
          }
        }
        .getOrElse( attr -> attr)
    }
    val newProjects = outputAndProjectMap.map(_._2)
    val newOutput = outputAndProjectMap.map(_._1)

    val aggregationsWithoutAlias = aggregations.map {
      e => e.transformDown {
        case agg: AggregateExpression => replaceAlias(agg, aliasMap)
        case reference: AttributeReference => replaceAliasMayKeepName(reference)
      }
    }.asInstanceOf[Seq[NamedExpression]]

    val groupByWithoutAlias = groupBy.map {
      e => e.transformDown {
        case reference: AttributeReference => replaceAliasMayKeepName(reference)
      }
    }.asInstanceOf[Seq[NamedExpression]]

    val pushStatement = SingleCatalystStatement.of(scanChild.relation,
        aggregationsWithoutAlias,
        scanChild.pushedFilters,
        groupByWithoutAlias)

    SQLPushDown.pushStatement(pushStatement, StructType.fromAttributes(newOutput))

    val scan = SQLPushDown.build() match {
      case v1: V1Scan =>
        V1ScanWrapper(v1, Seq.empty[sources.Filter], None)
      case scan => scan
    }

    val scanRelation = DataSourceV2ScanRelation(scanChild.relation, scan, newOutput)
    if(newOutput == output) {
      scanRelation
    } else {
      Project(newProjects, scanRelation)
    }
  }
}

/**
 * [[PushQuery]] currently finds the [[LogicalPlan]] which can be partially executed in an
 * individual partition. Extractor for basic SQL queries (not counting subqueries).
 *
 * The output type is a tuple with the values corresponding to
 * `SELECT`, `FROM`, `WHERE`, `GROUP BY`
 *
 * We inspect the given [[logical.LogicalPlan]] top-down and stop
 * at any point where a sub-query would be introduced or if nodes
 * need any re-ordering.
 *
 * The expected order of nodes is:
 *  - Project / Aggregate
 *  - Filter
 *  - Any logical plan as the source relation.
 *
 * TODO: support push down sql as can as possible in single partition
 */
object PushQuery extends Logging {

  /**
   * Determine if the given function is eligible for partial aggregation.
   *
   * @param aggregateFunction The aggregate function.
   * @return `true` if the given aggregate function is not supported for partial aggregation,
   * `false` otherwise.
   */
  private def nonSupportedAggregateFunction(aggregateFunction: AggregateFunction): Boolean =
    aggregateFunction match {
      case _: Count => false
      case _: Sum => false
      case _: Min => false
      case _: Max => false
      case _: Average => false
      case _ =>
        logWarning("Found an aggregate function" +
          s"(${aggregateFunction.prettyName.toUpperCase(Locale.getDefault)})" +
          "that could not be pushed down - falling back to normal behavior")
        true
    }

  private def containNonSupportedAggregateFunction(
      aggregateExpressions: Seq[NamedExpression]): Boolean = {
    val (aggExpressions, nonAggExpressions) =
      aggregateExpressions.partition(expr => expr.collect { case agg: AggregateExpression => agg }.nonEmpty)
    val hasNonSupportAgg = aggExpressions
      .flatMap(expr => expr.collect { case agg: AggregateExpression => agg })
      .exists(agg => agg.isDistinct || nonSupportedAggregateFunction(agg.aggregateFunction))

    hasNonSupportAgg ||
      nonAggExpressions.flatMap { expr =>
        expr.collect {
          case nullAlias@Alias(l: Literal, _) if l.dataType.isInstanceOf[NullType] => nullAlias
          case n: NotSupportPushDown => n  // sql036
        }}
      .nonEmpty
  }

  private def containNonSupportProjects(
      projects: Seq[NamedExpression]): Boolean = {
    projects
      .flatMap { expr =>
        expr.collect {
          case u: ScalaUDF => u
          case c: CaseWhen => c           // sql008
          case s: StringTrimLeft => s     // sql192
          case q: Quarter => q            // sql338
          case d1: DayOfYear => d1        // sql345
          case t: ToDegrees => t          // sql690
          case t1: ToRadians => t1        // sql714
          case s4: Signum => s4           // sql722
          case r: RegExpReplace => r      // sql819
          case s5: StringTrimRight => s5  // sql824
          case t2: TruncTimestamp => t2   // sql294
          case n: NotSupportPushDown => n // sql005 KapSubtractMonths
                                          // sql163 TimestampDiff
                                          // sql199 KapAddMonths
                                          // sql384 KapDayOfWeek
        }}
      .nonEmpty
  }
  private def subqueryPlan(op: LogicalPlan): Boolean =
    op match {
      case _: logical.Aggregate => true
      case _ => false
    }

  def unapply(plan: logical.LogicalPlan): Option[PushQuery] = {
    resolvePushQuery(plan)
  }

  private def resolvePushQuery(plan: logical.LogicalPlan, hasCountStar: Boolean = false): Option[PushQuery] = {
    import DataSourceV2Implicits._

    plan match {
      case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
        relation.table.asReadable.newScanBuilder(relation.options) match {
          case down: SupportsSQLPushDown if relation.catalog.exists(_.isInstanceOf[SupportsSQL])  &&
            (project.nonEmpty || hasCountStar) =>
            Some(PushScanQuery(project, filters, relation, down))
          case builder: ScanBuilder =>
            Some(new OldPush(project, filters, relation, builder))
        }
      case Aggregate(groupBy, aggExpressions, child) =>
        // Spark considers the COUNT(*) is not a project for relation
        val counts = aggExpressions.flatMap { _.collect {
          case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[Count] => ae.aggregateFunction
        }}
        resolvePushQuery(child, counts.exists(count => count.children(0) == Literal(1))).flatMap {
          case s @ PushScanQuery(_, _, _, _, _, _, _) if !subqueryPlan(child) &&
            !containNonSupportedAggregateFunction(aggExpressions) &&
            !containNonSupportProjects(s.project) &&
            s.postScanFilters.isEmpty =>
            Some(PushAggregateQuery(groupBy, aggExpressions, s))
          case _ => None
        }
      case s @ Sort(_, _, child) =>
        unapply(child).flatMap {
          case p @ PushScanQuery(_, _, _, _, _, _, _) if !subqueryPlan(child) &&
            !containNonSupportProjects(p.project) =>
            Some(p.copy(sortOpt = Some(s)))
          case _ => None
        }
      case globalLimit @ Limit(_, child) =>
        unapply(child).flatMap {
          case p @ PushScanQuery(_, _, _, _, _, _, _) if !subqueryPlan(child) &&
            !containNonSupportProjects(p.project) =>
            Some(p.copy(limitOpt = Some(globalLimit)))
          case _ => None
        }
      case globalLimitAndOffset @ GlobalLimitAndOffset(_, _, LocalLimit(_, child)) =>
        unapply(child).flatMap {
          case p @ PushScanQuery(_, _, _, _, Some(_: Sort), _, _) if !subqueryPlan(child) &&
            !containNonSupportProjects(p.project) =>
            Some(p.copy(limitAndOffsetOpt = Some(globalLimitAndOffset)))
          case _ => None
        }

      case _ => None
    }
  }
}
