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
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfter

import java.sql.DriverManager
import java.util.Properties

class ShardJDBCSuite extends QueryTest with BeforeAndAfter with SharedSparkSession {

  // TODO: duplicated codes
  val url0 = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn0: java.sql.Connection = _
  val url1 = "jdbc:h2:mem:testdb1;user=testUser;password=testPass"
  var conn1: java.sql.Connection = _

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[ShardJDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url0)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
    .set(s"spark.sql.catalog.h2.${ShardOptions.SHARD_URLS}", ShardOptions.buildSharding(url0, url1))

  private def createDB(url: String): java.sql.Connection = {
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    val conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema \"test\"").executeUpdate()
    conn.prepareStatement(
      "create table \"test\".\"people\" (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into \"test\".\"people\" values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into \"test\".\"people\" values ('mary', 2)").executeUpdate()
    conn.prepareStatement("insert into \"test\".\"people\" values ('bob', 3)").executeUpdate()
    conn.commit()
    conn
  }

  before {
    conn0 = createDB(url0)
    conn1 = createDB(url1)
  }

  after {
    conn0.close()
    conn1.close()
  }

  test("simple scan") {
    assert(sql("SELECT * FROM h2.test.people").collect().length === 6)
  }
}
