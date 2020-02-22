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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasource.FilePruner

object FilePrunerListFileTriggerRule extends Rule[LogicalPlan] {
  var cached: Option[LogicalPlan] = None
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown  {
    case op@PhysicalOperation(projects, filters,
    l@LogicalRelation(
    fsRelation@HadoopFsRelation(
    _: FilePruner,
    _,
    _,
    _,
    _,
    _),
    _, table, _)) =>
      if (cached.isDefined && cached.get.collectFirst{case a if a.equals(op) => a}.isDefined) {
        return op
      }
      val filterSet = ExpressionSet(filters)
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }
      val partitionColumns = l.resolve(
        fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters
          .filterNot(SubqueryExpression.hasSubquery)
          .filter(_.references.subsetOf(partitionSet)))

      // inject segment pruning //
      val filePruner = fsRelation.location.asInstanceOf[FilePruner]
      filePruner.resolve(l, fsRelation.sparkSession.sessionState.analyzer.resolver)
      //      inject end       //

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      filePruner.listFiles(partitionKeyFilters.iterator.toSeq, dataFilters.iterator.toSeq)
      cached = Some(op)
      op
  }
}
