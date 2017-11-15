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
package org.apache.spark.sql.execution.datasources.sparder.v2

import org.apache.spark.sql.execution.datasources.sparder.SparderFunSuite
import org.apache.spark.sql.{QueryTest, Row}

class SparderFileFormatV2Test extends SparderFunSuite {

  test("testBuildReaderWithPartitionValues") {
    val rows = spark.sql(
      "select col_0, col_1, col_2, col_3, col_4, col_5 from test limit 1 ")
    QueryTest.checkAnswer(
      rows,
      Seq(
        Row(Array[Byte](65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9),
            Array[Byte](0),
            Array[Byte](0),
            Array[Byte](7, -36),
            Array[Byte](69),
            Array[Byte](-114, -104, 78))))
    print(rows)
  }

  test("testBuildReader") {
    val rows = spark.sql("select  col_3 from test  ").show(1000)
  }

}
