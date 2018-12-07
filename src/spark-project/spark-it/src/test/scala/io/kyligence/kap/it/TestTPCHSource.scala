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
import org.apache.spark.sql.common.{LocalMetadata, ParquetTPCHSource, SparderBaseFunSuite}

class TestTPCHSource
    extends SparderBaseFunSuite
    with ParquetTPCHSource
    with LocalMetadata {

  override def tpchDataFolder(tableName: String): String =
    s"$TPCH_BASE_DIR/${tableName}_csv/"

  override val FORMAT = "com.databricks.spark.csv"

  val tables = List("customer",
                    "lineitem",
                    "custnation",
                    "orders",
                    "part",
                    "partsupp",
                    "suppregion",
                    "supplier",
                    "nation",
                    "region")

  ignore("generate parqeut file") {
    tables.foreach { table =>
      FileSystem
        .get(new Configuration())
        .deleteOnExit(new Path(s"$TPCH_BASE_DIR/$table"))
      spark.table(s"$table").write.parquet(s"$TPCH_BASE_DIR/$table")
    }

    spark.table("v_lineitem").write.parquet(s"$TPCH_BASE_DIR/v_lineitem")
    spark.table("v_orders").write.parquet(s"$TPCH_BASE_DIR/v_orders")
    spark.table("v_partsupp").write.parquet(s"$TPCH_BASE_DIR/v_partsupp")
  }

  ignore("generate indexr file") {
    tables.foreach { table =>
      val value = s"$TPCH_BASE_DIR/${table}_indexr"
      FileSystem.get(new Configuration()).deleteOnExit(new Path(value))
      spark
        .table(s"$table")
        .write
        .format(
          "org.apache.spark.sql.execution.datasources.indexr.IndexRFileFormat")
        .save(value)
    }

    spark
      .table("v_lineitem")
      .write
      .format(
        "org.apache.spark.sql.execution.datasources.indexr.IndexRFileFormat")
      .save(s"$TPCH_BASE_DIR/v_lineitem_indexr")
    spark
      .table("v_orders")
      .write
      .format(
        "org.apache.spark.sql.execution.datasources.indexr.IndexRFileFormat")
      .save(s"$TPCH_BASE_DIR/v_orders_indexr")
    spark
      .table("v_partsupp")
      .write
      .format(
        "org.apache.spark.sql.execution.datasources.indexr.IndexRFileFormat")
      .save(s"$TPCH_BASE_DIR/v_partsupp_indexr")
  }
}
