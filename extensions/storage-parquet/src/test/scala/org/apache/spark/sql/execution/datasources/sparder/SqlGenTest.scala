/*
 *
 *  * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *  *
 *  * http://kyligence.io
 *  *
 *  * This software is the confidential and proprietary information of
 *  * Kyligence Inc. ("Confidential Information"). You shall not disclose
 *  * such Confidential Information and shall use it only in accordance
 *  * with the terms of the license agreement you entered into with
 *  * Kyligence Inc.
 *  *
 *  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.execution.datasources.sparder

import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, RelBuilder}
import org.apache.spark.sql.execution.datasources.sparder.sqlgen.SqlGen
import org.scalatest.BeforeAndAfterEach


class SqlGenTest extends SparderFunSuite with BeforeAndAfterEach {
  sparkNeed = false

  override def beforeEach() {

  }

  override def afterEach() {

  }

  test(" test build sql") {
    import org.apache.calcite.plan.RelOptUtil
    import org.apache.calcite.sql.fun.SqlStdOperatorTable
    val config = Frameworks.newConfigBuilder.parserConfig(SqlParser.Config.DEFAULT).build()
    val builder = RelBuilder.create(config)
    val node = builder.scan("EMP").aggregate(builder.groupKey("DEPTNO"), builder.count(false, "C"), builder.sum(false, "S", builder.field("SAL"))).filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(10))).build
    System.out.println(RelOptUtil.toString(node))
  }


  test("request to sql") {
    val str = SqlGen.genSql(scanRequest, Map(1 -> "cost_int"))
    assert(str.equals("select col_0, col_1, cost_int(col_1) from Cuboid_114696 group by col_0, col_1 order by col_0, col_1"))
  }
}
