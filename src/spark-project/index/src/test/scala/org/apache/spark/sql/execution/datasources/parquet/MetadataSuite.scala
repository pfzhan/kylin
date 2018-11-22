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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.sources._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class MetadataSuite extends UnitTestSuite {
  test("ParquetColumnMetadata - withFilter") {
    val meta = ParquetColumnMetadata("field", 100, IntColumnStatistics(), None)
    meta.withFilter(None).filter should be (None)
    meta.withFilter(Some(null)).filter should be (None)
    meta.withFilter(Some(BloomFilterStatistics())).filter.isDefined should be (true)
  }

  test("ParquetFileStatus - numRows for empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array.empty)
    status.numRows should be (0)
  }

  test("ParquetFileStatus - numRows for single block") {
    val status = ParquetFileStatus(status = null, "schema",
      Array(ParquetBlockMetadata(123, Map.empty)))
    status.numRows should be (123)
  }

  test("ParquetFileStatus - numRows for non-empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array(
      ParquetBlockMetadata(11, Map.empty),
      ParquetBlockMetadata(12, Map.empty),
      ParquetBlockMetadata(13, Map.empty)))
    status.numRows should be (36)
  }
}
