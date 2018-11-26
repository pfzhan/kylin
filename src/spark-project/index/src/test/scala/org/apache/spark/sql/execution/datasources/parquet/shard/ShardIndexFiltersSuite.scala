/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql.execution.datasources.parquet.shard

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class ShardIndexFiltersSuite extends UnitTestSuite with SparkLocal {

  test("filterFiles") {
    /**
      * mock repartition to 3 file, and 123 in file1, 124 in file3
      */
    val file1 = new FileStatus()
    file1.setPath(new Path("file1"))
    val file2 = new FileStatus()
    file2.setPath(new Path("file2"))
    val file3 = new FileStatus()
    file3.setPath(new Path("file3"))
    val fileStatuses = Seq(file1, file2, file3)
    val schema = new StructType(Array(StructField("id", LongType)))
    val filters = ShardIndexFilters(fileStatuses, schema)

    val filter1 = EqualTo("id", 123)
    val files1 = filters.filterFiles(filter1)
    assert(files1.size == 1)
    assert(files1.head.equals(file1))

    val filter2 = And(EqualTo("id", 123), EqualTo("id", 124))
    val files2 = filters.filterFiles(filter2)
    assert(files2.isEmpty)

    val filter3 = Or(EqualTo("id", 123), EqualTo("id", 124))
    val files3 = filters.filterFiles(filter3)
    assert(files3.size == 2)
    assert(files3.head.equals(file1) && files3.last.equals(file3))

    val filter4 = Invalid()
    val files4 = filters.filterFiles(filter4)
    assert(files4.size == 3)

    val filter5 = In("id", Array(123, 124))
    val files5 = filters.filterFiles(filter5)
    assert(files5.size == 2)
    assert(files5.head.equals(file1) && files5.last.equals(file3))
  }

  test("pruningFilter -- Unsupported filter") {
    val filter1 = GreaterThan("name", "values")
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.isInstanceOf[Invalid])
  }

  test("pruningFilter -- In filter") {
    val filter1 = In("name", Array("values"))
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.equals(filter1))
  }

  test("pruningFilter -- EqualTo filter") {
    val filter1 = EqualTo("name", "value")
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.equals(filter1))
  }

  test("pruningFilter -- Not filter") {
    val filter1 = Not(Invalid())
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.isInstanceOf[Invalid])

    val filter2 = Not(EqualTo("name", "child"))
    val actual2 = ShardIndexFilters(null, null).pruningFilter(filter2)
    assert(actual2.equals(filter2))
  }

  test("pruningFilter -- And filter") {
    val filter1 = And(EqualTo("name", "left"), Invalid())
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.equals(EqualTo("name", "left")))

    val filter2 = And(Invalid(), EqualTo("name", "right"))
    val actual2 = ShardIndexFilters(null, null).pruningFilter(filter2)
    assert(actual2.equals(EqualTo("name", "right")))

    val filter3 = And(Invalid(), Invalid())
    val actual3 = ShardIndexFilters(null, null).pruningFilter(filter3)
    assert(actual3.isInstanceOf[Invalid])

    val filter4 = And(EqualTo("name", "left"), EqualTo("name", "right"))
    val actual4 = ShardIndexFilters(null, null).pruningFilter(filter4)
    assert(actual4.equals(filter4))
  }

  test("pruningFilter -- Or filter") {
    val filter1 = Or(EqualTo("name", "left"), Invalid())
    val actual1 = ShardIndexFilters(null, null).pruningFilter(filter1)
    assert(actual1.isInstanceOf[Invalid])

    val filter2 = Or(Invalid(), EqualTo("name", "right"))
    val actual2 = ShardIndexFilters(null, null).pruningFilter(filter2)
    assert(actual2.isInstanceOf[Invalid])

    val filter3 = Or(Invalid(), Invalid())
    val actual3 = ShardIndexFilters(null, null).pruningFilter(filter3)
    assert(actual3.isInstanceOf[Invalid])

    val filter4 = Or(EqualTo("name", "left"), EqualTo("name", "right"))
    val actual4 = ShardIndexFilters(null, null).pruningFilter(filter4)
    assert(actual4.equals(filter4))
  }
}
