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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

// Test catalog to check internal methods
private[datasources] class TestIndex extends MetastoreIndex {
  private var internalIndexFilters: Seq[Filter] = Nil
  override def tablePath(): Path = ???
  override def partitionSchema: StructType = ???
  override def indexSchema: StructType = ???
  override def dataSchema: StructType = ???
  override def setIndexFilters(filters: Seq[Filter]) = {
    internalIndexFilters = filters
  }
  override def indexFilters: Seq[Filter] = internalIndexFilters
  override def listFilesWithIndexSupport(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      indexFilters: Seq[Filter]): Seq[PartitionDirectory] = ???
  override def inputFiles: Array[String] = ???
  override def sizeInBytes: Long = ???
}

class MetastoreIndexSuite extends UnitTestSuite {
  test("provide sequence of path based on table path") {
    val catalog = new TestIndex() {
      override def tablePath(): Path = new Path("test")
    }

    catalog.rootPaths should be (Seq(new Path("test")))
  }

  test("when using listFiles directly supply empty index filter") {
    var indexSeq: Seq[Filter] = null
    var filterSeq: Seq[Expression] = null
    val catalog = new TestIndex() {
      override def listFilesWithIndexSupport(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression],
          indexFilters: Seq[Filter]): Seq[PartitionDirectory] = {
        indexSeq = indexFilters
        filterSeq = partitionFilters
        Seq.empty
      }
    }

    catalog.listFiles(Seq.empty, Seq.empty)
    indexSeq should be (Nil)
    filterSeq should be (Nil)
  }

  test("refresh should be no-op by default") {
    val catalog = new TestIndex()
    catalog.refresh()
  }
}
