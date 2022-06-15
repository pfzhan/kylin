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
package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}
import org.scalatest.BeforeAndAfter

import java.sql.DriverManager
import java.util.Properties

class ShardJDBCWithoutShardSuite extends QueryTest
  with BeforeAndAfter
  with SharedSparkSession {

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = _

  private val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
      Some(StringType)
  }

  before {
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      "insert into test.people values ('joe ''foo'' \"bar\"', 3)").executeUpdate()
    conn.commit()

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW foobar
         |USING org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider
         |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("CREATE TABLE test.partition (THEID INTEGER, `THE ID` INTEGER) " +
      "AS SELECT 1, 1")
      .executeUpdate()
    conn.commit()

  }

  after {
    conn.close()
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM foobar").collect().length === 3)
  }

  test("partition") {
    def testJdbcPartitionColumn(partColName: String, expectedColumnName: String): Unit = {
      val df = spark.read.format("org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider")
        .option("url", urlWithUserAndPass)
        .option("dbtable", "TEST.PARTITION")
        .option("partitionColumn", partColName)
        .option("lowerBound", 1)
        .option("upperBound", 4)
        .option("numPartitions", 3)
        .load()

      val quotedPrtColName = testH2Dialect.quoteIdentifier(expectedColumnName)
      df.logicalPlan match {
        case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
          val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
          assert(whereClauses === Set(
            s"$quotedPrtColName < 2 or $quotedPrtColName is null",
            s"$quotedPrtColName >= 2 AND $quotedPrtColName < 3",
            s"$quotedPrtColName >= 3"))
      }
    }

    testJdbcPartitionColumn("THEID", "THEID")
  }
}
