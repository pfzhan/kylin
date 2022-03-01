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
package org.apache.spark.sql.execution.datasources.jdbc.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog
import org.apache.spark.sql.functions.{sum, udf}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.{InjectNewPushDownRule, SharedSparkSession}
import org.apache.spark.util.Utils
import org.h2.api.CustomDataTypesHandler
import org.h2.store.DataHandler
import org.h2.util.JdbcUtils
import org.h2.value.{DataType, Value}

import java.sql.{Connection, DriverManager}
import java.util.Properties


class ShardJDBCWithoutShardSuite extends QueryTest
  with SharedSparkSession
  with InjectNewPushDownRule {

  import testImplicits._

  private val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  var conn: java.sql.Connection = _

  override def sparkConf: SparkConf =
    super.sparkConf
    .set("spark.sql.catalog.h2", classOf[ShardJDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("CREATE SCHEMA \"test\"").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"empty_table\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"people\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('fred', 1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('mary', 2)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"employee\" (dept INTEGER, name TEXT(32), salary NUMERIC(20, 2) NOT NULL," +
          " bonus NUMERIC(6, 2) NOT NULL)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300)")
        .executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  ignore("simple scan") {
    checkAnswer(sql("SELECT * FROM h2.test.empty_table"), Seq())
    checkAnswer(sql("SELECT * FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
    checkAnswer(sql("SELECT name, id FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
  }

  test("aggregate pushdown with alias") {
    val df1 = spark.table("h2.test.employee")
    var query1 = df1.select($"DEPT", $"SALARY".as("value"))
      .groupBy($"DEPT")
      .agg(sum($"value").as("total"))
      .filter($"total" > 1000)
    // query1.explain(true)
    checkAnswer(query1, Seq(Row(1, 19000.00), Row(2, 22000.00)))
    val decrease = udf { (x: Double, y: Double) => x - y}
    var query2 = df1.select($"DEPT", decrease($"SALARY", $"BONUS").as("value"), $"SALARY", $"BONUS")
      .groupBy($"DEPT")
      .agg(sum($"value"), sum($"SALARY"), sum($"BONUS"))
    // query2.explain(true)
    checkAnswer(query2,
      Seq(Row(1, 16800.00, 19000.00, 2200.00), Row(2, 19500.00, 22000.00, 2500.00)))

    val cols = Seq("a", "b", "c", "d")
    val df2 = sql("select * from h2.test.employee").toDF(cols: _*)
    val df3 = df2.groupBy().sum("c")
    // df3.explain(true)
    checkAnswer(df3, Seq(Row(41000.00)))
  }

  test("scan with aggregate push-down") {
    val df1 = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT")
    // df1.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('MAX('SALARY), None), unresolvedalias('MIN('BONUS), None)]
    // +- 'Filter ('dept > 0)
    //    +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // max(SALARY): int, min(BONUS): int
    // Aggregate [DEPT#0], [max(SALARY#2) AS max(SALARY)#6, min(BONUS#3) AS min(BONUS)#7]
    // +- Filter (dept#0 > 0)
    //    +- SubqueryAlias h2.test.employee
    //       +- RelationV2[DEPT#0, NAME#1, SALARY#2, BONUS#3] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#0], [max(max(SALARY)#13) AS max(SALARY)#6, min(min(BONUS)#14) AS min(BONUS)#7]
    // +- RelationV2[DEPT#0, max(SALARY)#13, min(BONUS)#14] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[DEPT#0], functions=[max(max(SALARY)#13), min(min(BONUS)#14)], output=[max(SALARY)#6, min(BONUS)#7])
    // +- Exchange hashpartitioning(DEPT#0, 5), true, [id=#10]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@3d9f0a5 [DEPT#0,max(SALARY)#13,min(BONUS)#14] PushedAggregates: [*Max(SALARY,false,None), *Min(BONUS,false,None)], PushedFilters: [IsNotNull(dept), GreaterThan(dept,0)], PushedGroupby: [*DEPT], ReadSchema: struct<DEPT:int,max(SALARY):int,min(BONUS):int>// scalastyle:on line.size.limit
    //
    // df1.show
    // +-----------+----------+
    // |max(SALARY)|min(BONUS)|
    // +-----------+----------+
    // |      10000|      1000|
    // |      12000|      1200|
    // +-----------+----------+
    checkAnswer(df1, Seq(Row(10000, 1000), Row(12000, 1200)))

    val df2 = sql("select MAX(ID), MIN(ID) FROM h2.test.people where id > 0")
    // df2.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('MAX('ID), None), unresolvedalias('MIN('ID), None)]
    // +- 'Filter ('id > 0)
    //    +- 'UnresolvedRelation [h2, test, people], []
    //
    // == Analyzed Logical Plan ==
    // max(ID): int, min(ID): int
    // Aggregate [max(ID#29) AS max(ID)#32, min(ID#29) AS min(ID)#33]
    // +- Filter (id#29 > 0)
    //    +- SubqueryAlias h2.test.people
    //       +- RelationV2[NAME#28, ID#29] test.people
    //
    // == Optimized Logical Plan ==
    // Aggregate [max(max(ID)#37) AS max(ID)#32, min(min(ID)#38) AS min(ID)#33]
    // +- RelationV2[max(ID)#37, min(ID)#38] test.people
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[max(max(ID)#37), min(min(ID)#38)], output=[max(ID)#32, min(ID)#33])
    // +- Exchange SinglePartition, true, [id=#44]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@5ed31735 [max(ID)#37,min(ID)#38] PushedAggregates: [*Max(ID,false,None), *Min(ID,false,None)], PushedFilters: [IsNotNull(id), GreaterThan(id,0)], PushedGroupby: [], ReadSchema: struct<max(ID):int,min(ID):int>
    // scalastyle:on line.size.limit

    //  df2.show()
    // +-------+-------+
    // |max(ID)|min(ID)|
    // +-------+-------+
    // |      2|      1|
    // +-------+-------+
    checkAnswer(df2, Seq(Row(2, 1)))

    val df3 = sql("select AVG(ID) FROM h2.test.people where id > 0")
    checkAnswer(df3, Seq(Row(1.5)))

    val df4 = sql("select MAX(SALARY) + 1 FROM h2.test.employee")
    // df4.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias(('MAX('SALARY) + 1), None)]
    // +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // (max(SALARY) + 1): int
    // Aggregate [(max(SALARY#68) + 1) AS (max(SALARY) + 1)#71]
    // +- SubqueryAlias h2.test.employee
    //    +- RelationV2[DEPT#66, NAME#67, SALARY#68, BONUS#69] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [(max((max(SALARY) + 1)#74) + 1) AS (max(SALARY) + 1)#71]
    // +- RelationV2[(max(SALARY) + 1)#74] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[max((max(SALARY) + 1)#74)], output=[(max(SALARY) + 1)#71])
    // +- Exchange SinglePartition, true, [id=#112]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@20864cd1 [(max(SALARY) + 1)#74] PushedAggregates: [*Max(SALARY,false,None)], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<(max(SALARY) + 1):int>
    // scalastyle:on line.size.limit
    checkAnswer(df4, Seq(Row(12001)))

    // COUNT push down is not supported yet
    val df5 = sql("select COUNT(*) FROM h2.test.employee")
    // df5.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('COUNT(1), None)]
    // +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // count(1): bigint
    // Aggregate [count(1) AS count(1)#87L]
    // +- SubqueryAlias h2.test.employee
    //    +- RelationV2[DEPT#82, NAME#83, SALARY#84, BONUS#85] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [count(1) AS count(1)#87L]
    // +- RelationV2[] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#87L])
    // *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#87L])
    // +- Exchange SinglePartition, true, [id=#149]
    //    +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#90L])
    //       +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@63262071 [] PushedAggregates: [], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<>
    // scalastyle:on line.size.limit
    checkAnswer(df5, Seq(Row(4)))

    val df6 = sql("select MIN(SALARY), MIN(BONUS), MIN(SALARY) * MIN(BONUS) FROM h2.test.employee")
    // df6.explain(true)
    checkAnswer(df6, Seq(Row(9000, 1000, 9000000)))

//    val df7 = sql("select MIN(SALARY), MIN(BONUS), SUM(SALARY * BONUS) FROM h2.test.employee")
    // df7.explain(true)
//    checkAnswer(df7, Seq(Row(9000, 1000, 48200000)))

//    val df8 = sql("select BONUS, SUM(SALARY+BONUS), SALARY FROM h2.test.employee" +
//      " GROUP BY SALARY, BONUS")
    // df8.explain(true)
//    checkAnswer(df8, Seq(Row(1000, 11000, 10000), Row(1200, 13200, 12000),
//      Row(1200, 10200, 9000), Row(1300, 11300, 10000)))
  }
}
