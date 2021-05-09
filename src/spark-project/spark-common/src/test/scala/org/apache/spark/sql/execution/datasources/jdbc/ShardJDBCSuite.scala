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
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfter

import java.sql.DriverManager
import java.util.Properties

class ShardJDBCSuite extends QueryTest
  with BeforeAndAfter
  with SharedSparkSession {

  val url0 = "jdbc:h2:mem:testdb0"
  var conn0: java.sql.Connection = _
  val url1 = "jdbc:h2:mem:testdb1"
  var conn1: java.sql.Connection = _

  private def createDB(url: String): java.sql.Connection = {
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    val conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      s"create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement(s"insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement(s"insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      s"insert into test.people values ('bob', 3)").executeUpdate()
    conn.commit()
    conn
  }

  before {
    conn0 = createDB(url0)
    conn1 = createDB(url1)
    val sharding = s"${ShardOptions.SHARD_URLS} '${ShardOptions.buildSharding(url1, url0)}'"
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW foobar
         |USING org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider
         |OPTIONS (url '$url0', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass', $sharding)
       """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn0.close()
    conn1.close()
  }

  test("simple scan") {
    assert(sql("SELECT * FROM foobar").collect().length === 6)
  }
}
