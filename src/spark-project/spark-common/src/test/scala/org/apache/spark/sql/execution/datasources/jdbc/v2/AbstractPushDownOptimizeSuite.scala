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

package org.apache.spark.sql.execution.datasources.jdbc.v2

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{If, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LogicalPlanIntegrity}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.connector.catalog.{SupportsNamespaces, SupportsRead, Table, TableCatalog}
import org.apache.spark.sql.connector.read.{ScanBuilder, V1Scan}
import org.apache.spark.sql.connector.read.sqlpushdown.{SQLStatement, SupportsSQL, SupportsSQLPushDown}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, V1ScanWrapper, V2ScanRelationPushDown2}
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.{SQLBuilder, SingleCatalystStatement, SingleSQLStatement}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

abstract class MockCatalog extends TableCatalog with SupportsNamespaces with SupportsSQL
abstract class MockSQLTable extends Table with SupportsRead
abstract class MockScanBuilder extends ScanBuilder with SupportsSQLPushDown

abstract class MockScan extends V1Scan {
  def pushedStatement(): SingleSQLStatement
}

abstract class AbstractPushDownOptimizeSuite extends PlanTest {

  def earlyScanPushDownRules: Seq[Rule[LogicalPlan]]

  private val emptyMap = CaseInsensitiveStringMap.empty

  /**
   * It simulates `Optimizer#batches`
   */
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Batch] = Batch("Pre CBO Rules", Once, V2ScanRelationPushDown2) ::
      Batch("Push-Down", Once, earlyScanPushDownRules: _*) :: Nil
  }

  private def toSQLStatement(catalystStatement: SingleCatalystStatement): SingleSQLStatement = {
    val projects = catalystStatement.projects
    val filters = catalystStatement.filters
    val groupBy = catalystStatement.groupBy
    SingleSQLStatement (
      relation = catalystStatement.relation.name,
      projects = Some(projects.map(SQLBuilder.expressionToSql(_))),
      filters = if (filters.isEmpty) None else Some(filters),
      groupBy = if (groupBy.isEmpty) None else Some(groupBy.map(SQLBuilder.expressionToSql(_))),
      url = None
    )
  }

  private def prepareV2Relation(): DataSourceV2Relation = {
    val catalog = mock(classOf[MockCatalog])
    val scan = mock(classOf[MockScan])
    val scanBuilder = mock(classOf[MockScanBuilder])

    when(scanBuilder.build()).thenReturn(scan)
    when(scanBuilder.pushStatement(any[SQLStatement], any[StructType]))
      .thenAnswer { invocationOnMock =>
        val thisStatement: SingleSQLStatement = {
          toSQLStatement(invocationOnMock.getArguments()(0).asInstanceOf[SingleCatalystStatement])
        }
        when(scan.pushedStatement()).thenReturn(thisStatement)
        when(scanBuilder.pushedStatement()).thenReturn(thisStatement)
      null
    }
    when(scanBuilder.pushFilters(any[Array[sources.Filter]])).thenAnswer { invocationOnMock =>
      val pushedFilters = invocationOnMock.getArguments()(0).asInstanceOf[Array[sources.Filter]]
      when(scanBuilder.pushedFilters()).thenReturn(pushedFilters)
      Array.empty[sources.Filter]
    }

    when(scanBuilder.pruneColumns(any[StructType])).thenAnswer { invocationOnMock =>
      val prunedSchema =
        invocationOnMock.getArguments()(0).asInstanceOf[StructType]
      when(scan.readSchema()).thenReturn(prunedSchema)
    }


    lazy val schema = StructType.fromAttributes('a.int::'b.int::'c.int::'t1.string :: Nil)
    val table = mock(classOf[MockSQLTable])
    when(table.schema()).thenReturn(schema)
    when(table.name()).thenReturn("DB1.Table1")
    when(table.newScanBuilder(any[CaseInsensitiveStringMap])).thenReturn(scanBuilder)

    DataSourceV2Relation.create(table, Some(catalog), None, emptyMap)
  }

  private def check(plan: LogicalPlan): Unit = {
    val dsv2 = plan.find(_.isInstanceOf[DataSourceV2ScanRelation])
      .map(_.asInstanceOf[DataSourceV2ScanRelation])
      .orNull

    assert(dsv2 != null)
    assert(dsv2.scan != null)
    assert(dsv2.scan.isInstanceOf[V1ScanWrapper])

    val statement = dsv2.scan.asInstanceOf[V1ScanWrapper].v1Scan.asInstanceOf[MockScan].pushedStatement()
    assert(statement != null)
    assertResult(dsv2.output.size)(statement.projects.get.size)
    logWarning(plan.toString)
    logWarning(statement.toSQL())
    assert(plan.missingInput.isEmpty)
    assert(plan.resolved)
    assert(LogicalPlanIntegrity.checkIfExprIdsAreGloballyUnique(plan))
  }

  test("mock with only aggregation") {
    val relationV2 = prepareV2Relation()
    val sumIf = If('a > Literal(10), Literal(1), Literal(0))
    val query = relationV2.groupBy('t1.attr)('t1, sum(sumIf), avg('a)).analyze

    val plan = Optimize.execute(query)
    assert(plan != null)
    check(plan)
  }

  test("mock with only filter") {
    val relationV2 = prepareV2Relation()
    val query =
      relationV2.where('t1.attr =!= "xxx" && 'a.attr < 10 ).select('b.attr, 'c.attr).analyze

    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with group by and filter") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.where('t1.attr =!= "xxx" && 'a.attr < 10 )
        .groupBy('t1.attr)('t1, sum('b), avg('a)).analyze

    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with group by expressions not in aggregation expression ") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(max('b), sum('c), avg('a)).analyze

    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with max(a) + 1") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(max('b) + 1).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with count(1)") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(count(1)).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with count(*)") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(count("*")).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with count(*) + 1") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(count("*") + 1).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with count(a), count(a) + 1, count(1), count(*)") {
    val relationV2 = prepareV2Relation()
    val query = relationV2.groupBy('t1.attr)(count('a), count('a) + 1, count(1), count("*")).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  test("mock with aggregation with alias") {
    val relationV2 = prepareV2Relation()
    val query = relationV2
      .select('t1.as("g1"), 'b.as("m1"))
      .groupBy('g1.attr)(max('m1) + 1).analyze
    val plan = Optimize.execute(query)
    check(plan)
  }

  ignore("mock with Non-correlated subquery") {
    val relationV2 = prepareV2Relation()
    relationV2.groupBy()(max('b))
  }
}
