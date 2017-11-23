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

package org.apache.spark.sql

import scala.collection.JavaConverters._
import io.kyligence.kap.query.runtime.{
  AggArgc,
  CreateDictPushdownTableArgc,
  OrderArgc,
  SelectArgc
}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.sparder.SparderConstants
import org.apache.spark.sql.manager.{UDTManager, UdfManager}
import org.apache.spark.sql.types.StructType

object SparderFunc {
  var spark: SparkSession = initSpark()

  def getSparkSession(): SparkSession = {
    if (spark == null || spark.sparkContext.isStopped) {
      spark = initSpark()
    }
    spark
  }

  def getSparkConf(key: String): String = {
    getSparkSession().sparkContext.conf.get(key)
  }

  def initSpark(): SparkSession = {
    val conf = initSparkConf()
    val instances = conf.get("spark.executor.instances").toInt
    val cores = conf.get("spark.executor.cores").toInt
    if (conf.get("spark.sql.shuffle.partitions", "").isEmpty) {
      conf.set("spark.sql.shuffle.partitions", (instances * cores).toString)
    }
    conf.set("spark.debug.maxToStringFields", "1000")
    conf.set("spark.scheduler.mode", "FAIR")
    val fairscheduler = KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml"
    conf.set("spark.scheduler.allocation.file", fairscheduler)
    //  conf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=current")
    //  conf.set("spark.executor.extraJavaOptions", "-Dhdp.version=current")
    //  conf.set("spark.yarn.jars","hdfs:///user/spark/spark-lib/2.1.1/*")
    //  conf.set("spark.executor.instances","2")

    val sparkSession = {
      if ("true".equalsIgnoreCase(System.getProperty("spark.local"))) {
        SparkSession.builder
          .master("local")
          .appName("sparder-test-sql-context")
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      } else {
        conf.set("spark.jars", KapConfig.getInstanceFromEnv.sparderJars)
        SparkSession.builder
          .master("yarn-client")
          .appName("sparder-sql-context")
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
    }
    UdfManager.create(sparkSession)
    UDTManager.init()
    spark = sparkSession
    sparkSession
  }

  def initSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    KapConfig.getInstanceFromEnv.getSparkConf.asScala.foreach {
      case (k, v) =>
        sparkConf.set(k, v)

    }
    sparkConf
  }

  def createDictPushDownTable(args: CreateDictPushdownTableArgc): DataFrame = {
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "cube")
    spark.read
      .format(args.fileFormat)
      .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, args.gtinfo)
      .option(SparderConstants.CUBOID_ID, args.cuboid)
      .option(SparderConstants.DICT, args.dict)
      .option(SparderConstants.TABLE_ALIAS, args.tableName)
      .schema(args.schema)
      .option(SparderConstants.FILTER_PUSH_DOWN, args.pushdown)
      .option(SparderConstants.BINARY_FILTER_PUSH_DOWN,
              args.binaryFilterPushdown)
      .load(args.filePath: _*)
      .as(args.tableName)
  }

  def createEmptyDataFrame(structType: StructType): DataFrame = {
    spark.createDataFrame(new java.util.ArrayList[Row], structType)
  }

  def select(selectArgc: SelectArgc): DataFrame = {
    selectArgc.dataFrame.select(selectArgc.select: _*)
  }

  def orderBy(orderArgc: OrderArgc): DataFrame = {
    orderArgc.dataFrame.orderBy(orderArgc.orders: _*).union(orderArgc.dataFrame)
  }

  def agg(aggArgc: AggArgc): DataFrame = {
    if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame
        .groupBy(aggArgc.group: _*)
        .agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else if (aggArgc.agg.isEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame.dropDuplicates(aggArgc.group.map(_.toString()))
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.isEmpty) {
      aggArgc.dataFrame.agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else {
      aggArgc.dataFrame
    }
  }
}
