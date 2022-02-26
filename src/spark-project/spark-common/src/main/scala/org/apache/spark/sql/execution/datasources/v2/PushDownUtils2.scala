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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, Literal, NamedExpression, PredicateHelper, SchemaPruning, SubqueryExpression}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PushableColumn, PushableColumnBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}

import scala.collection.mutable

object PushDownUtils2 extends PredicateHelper {
  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression]): (Seq[sources.Filter], Seq[Expression]) = {
    scanBuilder match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        val pushableColumn = PushableColumn(true)
        for (filterExpr <- filters) {
          val transformedFilter = transformFilter(filterExpr, pushableColumn)
          val translated =
            DataSourceStrategy.translateFilterWithMapping(transformedFilter, Some(translatedFilterToExpr),
              nestedPredicatePushdownEnabled = true)
          if (translated.isEmpty) {
            untranslatableExprs += transformedFilter
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        (r.pushedFilters(), untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  private def unsupportedDecimalType(dt: DataType): Boolean = dt match {
    case decimal: DecimalType => decimal.scale != 0 && decimal.scale != decimal.precision
    case _ => false
  }

  /**
   * Spark doesn't support push-down filter with function and ClickHouse doesn't support comparable
   * operator which contains decimal with scale != 0 and scale != precision.
   * In order to push-down the above comparable operator in filter into ClickHouse, make 'castFloat'
   * enclose column name and then replace it with 'toFloat64'.
   */
  private def transformFilter(filter: Expression, pushableColumn: PushableColumnBase): Expression = {
    filter match {
      case et @ expressions.EqualTo(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            et.withNewChildren(Seq(attr.withName(newName), l))
          case _ => et
        }
      case et @ expressions.EqualTo(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            et.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => et
        }
      case ens @ expressions.EqualNullSafe(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            ens.withNewChildren(Seq(attr.withName(newName), l))
          case _ => ens
        }
      case ens @ expressions.EqualNullSafe(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            ens.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => ens
        }
      case gt @ expressions.GreaterThan(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            gt.withNewChildren(Seq(attr.withName(newName), l))
          case _ => gt
        }
      case gt @ expressions.GreaterThan(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            gt.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => gt
        }
      case lt @ expressions.LessThan(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            lt.withNewChildren(Seq(attr.withName(newName), l))
          case _ => lt
        }
      case lt @ expressions.LessThan(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            lt.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => lt
        }
      case gte @ expressions.GreaterThanOrEqual(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            gte.withNewChildren(Seq(attr.withName(newName), l))
          case _ => gte
        }
      case gte @ expressions.GreaterThanOrEqual(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            gte.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => gte
        }
      case lte @ expressions.LessThanOrEqual(attr: AttributeReference, l: Literal)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            lte.withNewChildren(Seq(attr.withName(newName), l))
          case _ => lte
        }
      case lte @ expressions.LessThanOrEqual(l: Literal, attr: AttributeReference)
        if unsupportedDecimalType(attr.dataType) =>
        attr match {
          case pushableColumn(name) =>
            val newName = s"castFloat${name}castFloat"
            lte.withNewChildren(Seq(l, attr.withName(newName)))
          case _ => lte
        }
      case nt @ expressions.Not(child) =>
        val newChild = transformFilter(child, pushableColumn)
        nt.withNewChildren(Seq(newChild))
      case and @ expressions.And(left, right) =>
        val newLeft = transformFilter(left, pushableColumn)
        val newRight = transformFilter(right, pushableColumn)
        and.withNewChildren(Seq(newLeft, newRight))
      case or @ expressions.Or(left, right) =>
        val newLeft = transformFilter(left, pushableColumn)
        val newRight = transformFilter(right, pushableColumn)
        or.withNewChildren(Seq(newLeft, newRight))
      case other => other
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the `Scan` instance (since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  def pruneColumns(
      scanBuilder: ScanBuilder,
      relation: DataSourceV2Relation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Scan, Seq[AttributeReference]) = {
    prunedColumns(scanBuilder, relation, projects, filters)
      .map { prunedSchema =>
        scanBuilder.asInstanceOf[SupportsPushDownRequiredColumns]
          .pruneColumns(prunedSchema)
        val scan = scanBuilder.build()
        scan -> toOutputAttrs(scan.readSchema(), relation)}
      .getOrElse(scanBuilder.build() -> relation.output)
  }

  def prunedColumns(
    scanBuilder: ScanBuilder,
    relation: DataSourceV2Relation,
    projects: Seq[NamedExpression],
    filters: Seq[Expression]): Option[StructType] = {
    scanBuilder match {
      case _: SupportsPushDownRequiredColumns if SQLConf.get.nestedSchemaPruningEnabled =>
        val rootFields = SchemaPruning.identifyRootFields(projects, filters)
        val prunedSchema = if (rootFields.nonEmpty) {
          SchemaPruning.pruneDataSchema(relation.schema, rootFields)
        } else {
          new StructType()
        }
        Some(prunedSchema)

      case _: SupportsPushDownRequiredColumns =>
        val exprs = projects ++ filters
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        // always project, in case the relation's output has been updated and doesn't match
        // the underlying table schema
        Some(neededOutput.toStructType)
      case _ => None
    }
  }
  def pushDownFilter(
       scanBuilder: ScanBuilder,
       filters: Seq[Expression],
       relation: DataSourceV2Relation): (Seq[sources.Filter], Seq[Expression]) = {
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, relation.output)
    val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
    // `postScanFilters` need to be evaluated after the scan.
    // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
    val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils2.pushFilters(
      scanBuilder, normalizedFiltersWithoutSubquery)
    val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery
    (pushedFilters, postScanFilters)
  }

  def toOutputAttrs(
      schema: StructType,
      relation: DataSourceV2Relation): Seq[AttributeReference] = {
    val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
    val cleaned = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    cleaned.toAttributes.map {
      // we have to keep the attribute id during transformation
      a => a.withExprId(nameToAttr(a.name).exprId)
    }
  }
}
