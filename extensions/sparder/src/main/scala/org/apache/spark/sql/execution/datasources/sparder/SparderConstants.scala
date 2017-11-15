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
package org.apache.spark.sql.execution.datasources.sparder

object SparderConstants {
  val KYLIN_SCAN_GTINFO_BYTES = "io.kylin.storage.parquet.scan.gtinfo"
  val KYLIN_SCAN_REQUEST_BYTES = "io.kylin.storage.parquet.scan.gtscanrequest"
  val SPARK_PROJECT_SCHEMA = "org.apache.spark.sql.sparder.row.project"
  val FILTER_PUSH_DOWN = "filter_push_down"
  val BINARY_FILTER_PUSH_DOWN = "binary_filter_push_down"
  val CUBOID_ID = "cuboid_id"
  val TABLE_ALIAS = "table_alias"
  val COLUMN_PREFIX = "col_"
  val DICT = "dict"
  val COLUMN_NAMES_SEPARATOR = "SEPARATOR"
  val COLUMN_NAME_SEPARATOR = "_"
  val DERIVE_TABLE = "DERIVE"
  val DATASCHMEA_TABLE_ORD = 0
  val DATASCHMEA_COL_ORD = 1
  val DATASCHMEA_DATATYPE_ORD = 2
  val DATASTYPESCHMEA_COL_ORD = 0
  val DATASTYPESCHMEA_DATA_ORD = 1
}
