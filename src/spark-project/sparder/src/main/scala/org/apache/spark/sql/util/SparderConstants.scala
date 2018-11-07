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
package org.apache.spark.sql.util

object SparderConstants {
  val KYLIN_SCAN_GTINFO_BYTES = "io.kylin.storage.parquet.scan.gtinfo"
  val KYLIN_SCAN_REQUEST_BYTES = "io.kylin.storage.parquet.scan.gtscanrequest"
  val SPARK_PROJECT_SCHEMA = "org.apache.spark.sql.sparder.row.project"
  val PAGE_FILTER_PUSH_DOWN = "page_filter_push_down"
  val BINARY_FILTER_PUSH_DOWN = "binary_filter_push_down"
  val LATE_DECODE_COLUMN = "late_decode_column"
  val CUBOID_ID = "cuboid_id"
  val TABLE_ALIAS = "table_alias"
  val STORAGE_TYPE = "storage_type"
  val COLUMN_PREFIX = "col_"
  val DICT = "dict"
  val DICT_PATCH = "dict_patch"
  val DIAGNOSIS_WRITER_TYPE = "diagnosisWriterType"
  val SEGMENT_ID = "segmentId"
  val COLUMN_NAMES_SEPARATOR = "SEPARATOR"
  val COLUMN_NAME_SEPARATOR = "__"
  val DERIVE_TABLE = "DERIVE"
  val DATASCHMEA_TABLE_ORD = 0
  val DATASCHMEA_COL_ORD = 1
  val DATASCHMEA_DATATYPE_ORD = 2
  val DATASTYPESCHMEA_COL_ORD = 0
  val DATASTYPESCHMEA_DATA_ORD = 1
  val PARQUET_FILE_FILE_TYPE = "file_type"
  val PARQUET_FILE_RAW_TYPE = "raw"
  val KYLIN_CONF = "kylin_conf"
  val PARQUET_FILE_CUBE_TYPE = "cube"
  val DATE_DICT = "date_dict"
}
