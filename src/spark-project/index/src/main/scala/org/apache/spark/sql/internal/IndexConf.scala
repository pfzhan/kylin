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

package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession

object IndexConf {
  import SQLConf.buildConf

  val METASTORE_LOCATION = buildConf("spark.sql.index.metastore").
    doc("Metastore location or root directory to store index information, will be created " +
      "if path does not exist").
    stringConf.
    createWithDefault("")

  val CREATE_IF_NOT_EXISTS = buildConf("spark.sql.index.createIfNotExists").
    doc("When set to true, creates index if one does not exist in metastore for the table").
    booleanConf.
    createWithDefault(false)

  val NUM_PARTITIONS = buildConf("spark.sql.index.partitions").
    doc("When creating index uses this number of partitions. If value is non-positive or not " +
      "provided then uses `sc.defaultParallelism * 3` or `spark.sql.shuffle.partitions` " +
      "configuration value, whichever is smaller").
    intConf.
    createWithDefault(0)

  val PARQUET_FILTER_STATISTICS_ENABLED =
    buildConf("spark.sql.index.parquet.filter.enabled").
    doc("When set to true, writes filter statistics for indexed columns when creating table " +
      "index, otherwise only min/max statistics are used. Filter statistics are always used " +
      "during filtering stage, if applicable").
    booleanConf.
    createWithDefault(true)

  val PARQUET_FILTER_STATISTICS_TYPE = buildConf("spark.sql.index.parquet.filter.type").
    doc("When filter statistics enabled, selects type of statistics to use when creating index. " +
      "Available options are `bloom`, `dict`").
    stringConf.
    createWithDefault("bloom")

  val PARQUET_FILTER_STATISTICS_EAGER_LOADING =
    buildConf("spark.sql.index.parquet.filter.eagerLoading").
    doc("When set to true, read and load all filter statistics in memory the first time catalog " +
      "is resolved, otherwise load them lazily as needed when evaluating predicate. " +
      "Eager loading removes IO of reading filter data from disk, but requires extra memory").
    booleanConf.
    createWithDefault(false)

  /** Create new configuration from session SQLConf */
  def newConf(sparkSession: SparkSession): IndexConf = {
    new IndexConf(sparkSession.sessionState.conf)
  }
}

class IndexConf private[sql](val sqlConf: SQLConf) {
  import IndexConf._

  /** ************************ Index Params/Hints ******************* */

  def metastoreLocation: String = getConf(METASTORE_LOCATION)

  def parquetFilterEnabled: Boolean = getConf(PARQUET_FILTER_STATISTICS_ENABLED)

  def parquetFilterType: String = getConf(PARQUET_FILTER_STATISTICS_TYPE)

  def parquetFilterEagerLoading: Boolean = getConf(PARQUET_FILTER_STATISTICS_EAGER_LOADING)

  def createIfNotExists: Boolean = getConf(CREATE_IF_NOT_EXISTS)

  def numPartitions: Int = getConf(NUM_PARTITIONS)

  /** ********************** IndexConf functionality methods ************ */

  /** Set configuration for underlying SQLConf */
  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sqlConf.setConf(entry, value)
  }

  /** Set direct configuration key and value in SQLConf */
  def setConfString(key: String, value: String): Unit = {
    sqlConf.setConfString(key, value)
  }

  /** Get configuration from underlying SQLConf */
  def getConf[T](entry: ConfigEntry[T]): T = {
    sqlConf.getConf(entry)
  }

  /** Unset configuration from SQLConf */
  def unsetConf(entry: ConfigEntry[_]): Unit = {
    sqlConf.unsetConf(entry)
  }
}
