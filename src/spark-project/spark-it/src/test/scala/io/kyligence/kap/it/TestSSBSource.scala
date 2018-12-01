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

package io.kyligence.kap.it

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.common.{LocalMetadata, ParquetSSBSource, SparderBaseFunSuite}

class TestSSBSource
  extends SparderBaseFunSuite
    with ParquetSSBSource
    with LocalMetadata {

  override def ssbDataFolder(tableName: String): String =
    s"$SSB_BASE_DIR/${tableName}.csv"

  override val FORMAT = "com.databricks.spark.csv"

  val tables = List(
    "customer",
    "lineorder",
    "dates",
    "supplier",
    "part")

  test("generate parqeut file") {
    tables.foreach { table =>
      FileSystem
        .get(new Configuration())
        .deleteOnExit(new Path(s"$SSB_BASE_DIR/$table"))
      spark.table(s"$table").write.parquet(s"$SSB_BASE_DIR/$table")
    }

    spark.table("p_lineorder").write.parquet(s"$SSB_BASE_DIR/p_lineorder")
    //scalastyle:off
    println()

  }
}
