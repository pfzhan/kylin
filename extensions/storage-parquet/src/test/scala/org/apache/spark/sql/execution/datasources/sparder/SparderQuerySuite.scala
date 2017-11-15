/*
 *
 *  * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *  *
 *  * http://kyligence.io
 *  *
 *  * This software is the confidential and proprietary information of
 *  * Kyligence Inc. ("Confidential Information"). You shall not disclose
 *  * such Confidential Information and shall use it only in accordance
 *  * with the terms of the license agreement you entered into with
 *  * Kyligence Inc.
 *  *
 *  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.execution.datasources.sparder

import java.util.concurrent.{Executors, TimeUnit}

import io.kyligence.kap.storage.parquet.cube.spark.TransformFunction
import io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder.{
  KylinCast,
  RowToBytes
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.TestUdfManager
import org.apache.spark.sql.execution.utils.{RowTearer, SchemaProcessor}
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf.{AggraFuncArgs2, SparderAggFun}

class SparderQuerySuite extends SparderFunSuite {
  var visit: TestSparkSparderVisit = _
  var sparderVisit: SparkSparderVisit = _
  override def afterInit(): Unit = {
    SparkSparderVisit.initSparkSession(spark)
    SparkSparderVisit.spark.conf.set("spark.sql.shuffle.partitions", "1")
    visit =
      TestSparkSparderVisit(sparkJobRequest.getPayload, javaContext, "asd")
    TestUdfManager.create(SparkSparderVisit.spark)
    //    visit2 = SparkSparderVisit(sparkJobRequest.getPayload, "asd")
  }

  def withtask(task: (TestSparkSparderVisit) => Unit): Unit = {}

  test("aggr ") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString = testSparkSparderVisit.register(cleanMap,
                                                       SparkSparderVisit.spark,
                                                       tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val df = SparkSparderVisit.spark.sql(
        s"select kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from $tableName group by col_0")
      checkAnswer(df,
                  Seq(
                    Row(2014, 895974.4400),
                    Row(1942, 895298.4300),
                    Row(1992, 911774.9800),
                    Row(1984, 903623.5400),
                    Row(1962, 904304.6800)
                  ))
      null
    }
    visit.executeTask()
  }

  test("aggr sort ") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString =
        testSparkSparderVisit.register(cleanMap, spark, tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val df = SparkSparderVisit.spark.sql(
        "select  kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")
      checkAnswer(df,
                  Seq(
                    Row(2014, 895974.4400),
                    Row(1942, 895298.4300),
                    Row(1992, 911774.9800),
                    Row(1984, 903623.5400),
                    Row(1962, 904304.6800)
                  ))
      null
    }
    visit.executeTask()
  }
  test("aggr show data 4 ") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString = {
        cleanMap.map {
          case (colId, aggr) =>
            val funcName = TestUdfManager.register(aggr.dataTp, aggr.funcName)
            (colId, funcName)
        }
      }
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val df = SparkSparderVisit.spark
        .sql("select col_0,col_1, bigintCOUNT(col_4), decimal_19_4_SUM(col_6) from Cuboid_114696 group by col_0 ,col_1 order by col_0 , col_1 limit 1 ")
        .javaRDD
        .repartition(1)
        .mapPartitions(
          new TransformFunction(scanRequestStr,
                                sparkJobRequest.getPayload.getKylinProperties))
        .collect()
        .get(0)
        .getData
        .length
      val df1 = SparkSparderVisit.spark
        .sql("select col_0,col_1, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 ,col_1 order by col_0 , col_1 limit 1 ")
        .javaRDD
        .repartition(1)
        .mapPartitions(
          new TransformFunction(scanRequestStr,
                                sparkJobRequest.getPayload.getKylinProperties))
        .collect()
        .get(0)
        .getData
        .length
      println(df)
      null

    }
    visit.executeTask()
  }
  test("aggr data length") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString = {
        cleanMap.map {
          case (colId, aggr) =>
            val name = aggr.dataTp.toString
              .replace("(", "_")
              .replace(")", "_")
              .replace(",", "_") + aggr.funcName

            spark.udf.register(
              name,
              new SparderAggFun(AggraFuncArgs2(aggr.funcName, aggr.dataTp)))
            (colId, name)
        }
      }
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val df = SparkSparderVisit.spark
        .sql("select col_0,col_1, bigintCOUNT(col_4), decimal_19_4_SUM(col_6) from Cuboid_114696 group by col_0 ,col_1 order by col_0 , col_1  ")
        .javaRDD
        .repartition(1)
        .mapPartitions(new RowToBytes)
        .collect()
        .get(0)
        .getData
        .length
      val df1 = SparkSparderVisit.spark
        .sql("select col_0,col_1, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 ,col_1 order by col_0 , col_1  ")
        .javaRDD
        .repartition(1)
        .mapPartitions(
          new TransformFunction(scanRequestStr,
                                sparkJobRequest.getPayload.getKylinProperties))
        .collect()
        .get(0)
        .getData
        .length
      assert(df == 805)
      assert(df1 == 835)
      null
    }
    visit.executeTask()
  }

  test("filter push down") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .option(
          SparderConstants.FILTER_PUSH_DOWN,
          "0300011e144e554c4c2e47545f4d4f434b55505f5441424c450132013107696e7465676572001f01010400ff")
        .schema(SchemaProcessor.buildSchemaV2(testSparkSparderVisit.gtInfo,
                                              tableName))
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val funMap = testSparkSparderVisit.register(cleanMap, spark, tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      //      val df = SparkSparderVisit.spark.sql(SqlGen.genSql(testSparkSparderVisit.scanRequest,funMap))
      val df =
        SparkSparderVisit.spark.sql("select count(*) from Cuboid_114696 ")
      checkAnswer(df,
                  Seq(
                    Row(316)
                  ))
      null
    }
    visit.executeTask()
  }

  test("create table benchmark") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      var total = 0L
      for (i <- Range(1, 200)) {
        val start = System.currentTimeMillis()
        SparkSparderVisit.spark.read
          .format("org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
          .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
          .option(SparderConstants.CUBOID_ID, cuboid)
          .option(SparderConstants.TABLE_ALIAS, alias)
          .option(
            SparderConstants.FILTER_PUSH_DOWN,
            "0300011e144e554c4c2e47545f4d4f434b55505f5441424c450132013107696e7465676572001f01010400ff")
          .schema(SchemaProcessor.buildSchemaV2(testSparkSparderVisit.gtInfo,
                                                tableName))
          .load(testSparkSparderVisit.parquetPathIter: _*)
          .createOrReplaceTempView(tableName + i)
        val duration = System.currentTimeMillis() - start
        println(s"current create time:$duration ")
        total = total + duration
      }
      val avgTime = total / 200.0
      println(s"avg duration:$avgTime")
      null
    }
    visit.executeTask()
  }

  test("mult thread ") {
    SparkSparderVisit.initSparkSession(spark)
    SparkSparderVisit.spark.conf.set("spark.sql.shuffle.partitions", "1")
    val visit =
      TestSparkSparderVisit(sparkJobRequest.getPayload, javaContext, "asd")
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val funMap = testSparkSparderVisit.register(cleanMap, spark, tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val df = SparkSparderVisit.spark
        .sql("select kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0")
        .show(100)
      null
    }
    visit.executeTask()
    val service = Executors.newFixedThreadPool(10)
    for (i <- Range(1, 10)) {
      service.submit(
        new Runnable {
          override def run(): Unit = {
            val visit2 = TestSparkSparderVisit(sparkJobRequest.getPayload,
                                               javaContext,
                                               "asd")
            visit2.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
              val gTinfoStr = testSparkSparderVisit.gTinfoStr
              val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
              val boardcastGTInfo =
                SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
              val cleanMap = testSparkSparderVisit
                .cleanRequest(testSparkSparderVisit.scanRequest, gtInfoStr)
              val funMap =
                testSparkSparderVisit.register(cleanMap, spark, tableName)
              val scanRequestStr: String =
                new String(scanRequestArray, "ISO-8859-1")
              val df = SparkSparderVisit.spark
                .sql("select kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0")
                .show(100)
              null
            }
            visit2.executeTask()
          }
        }
      )

    }
    service.shutdown()
    service.awaitTermination(10000, TimeUnit.DAYS)

  }

  test("aggr sort 2 ") {
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .load(testSparkSparderVisit.parquetPathIter: _*)
        .createOrReplaceTempView(tableName)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString =
        testSparkSparderVisit.register(cleanMap, spark, tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)

      SparkSparderVisit.spark.sql("select col_0 from Cuboid_114696 ").show(100)
      val df = SparkSparderVisit.spark.sql(
        "select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")

      df.show(100)
      //      checkAnswer(
      //        df,
      //        Seq(
      //          Row(2014, 895974.4400),
      //          Row(1942, 895298.4300),
      //          Row(1992, 911774.9800),
      //          Row(1984, 903623.5400),
      //          Row(1962, 904304.6800)
      //        ))
      null
    }
    visit.executeTask()
  }

  test("aggr arrg ") {
    import org.apache.spark.sql.functions._
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      var table = SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .schema(SchemaProcessor.buildSchemaV2(testSparkSparderVisit.gtInfo,
                                              cuboid.toString))
        .load(testSparkSparderVisit.parquetPathIter: _*)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString =
        testSparkSparderVisit.register(cleanMap, spark, cuboid.toString)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val expr1 = expr("kylin_casta(bigintCOUNT(114696_4),4)").alias("t2").expr

      val df = table
        .groupBy("114696_0")
        .agg(expr("kylin_casta(bigintCOUNT(114696_4),4)").alias("t2"),
             expr(" kylin_castb(decimal_19_4_SUM(114696_6),6)").alias("t3"))
        .orderBy("114696_0")
        .select("t2", "t3")
        .limit(5)

      //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")

      df.show(100)
      checkAnswer(df,
                  Seq(
                    Row(2014, 895974.4400),
                    Row(1942, 895298.4300),
                    Row(1992, 911774.9800),
                    Row(1984, 903623.5400),
                    Row(1962, 904304.6800)
                  ))
      null
    }
    visit.executeTask()
  }

  test("aggr optimize ") {
    import org.apache.spark.sql.functions._
    visit.task = (testSparkSparderVisit: TestSparkSparderVisit) => {
      val gTinfoStr = testSparkSparderVisit.gTinfoStr
      val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
      var table = SparkSparderVisit.spark.read
        .format(
          "org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gTinfoStr)
        .option(SparderConstants.CUBOID_ID, cuboid)
        .option(SparderConstants.TABLE_ALIAS, alias)
        .schema(SchemaProcessor.buildSchemaV2(testSparkSparderVisit.gtInfo,
                                              alias))
        .load(testSparkSparderVisit.parquetPathIter: _*)
      val boardcastGTInfo =
        SparkSparderVisit.spark.sparkContext.broadcast(gTinfoStr)
      val cleanMap =
        testSparkSparderVisit.cleanRequest(testSparkSparderVisit.scanRequest,
                                           gtInfoStr)
      val intToString =
        testSparkSparderVisit.register(cleanMap, spark, tableName)
      val scanRequestStr: String = new String(scanRequestArray, "ISO-8859-1")
      SparkSparderVisit.spark.udf
        .register("kylin_castb", new KylinCast(gTinfoStr), DecimalType(19, 4))
      SparkSparderVisit.spark.udf
        .register("kylin_casta", new KylinCast(gTinfoStr), LongType)
      val expr1 = expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2").expr
      println(
        table
          .groupBy("col_0")
          .agg(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
               expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3"))
          .orderBy("col_0")
          .select("t2", "t3")
          .limit(5)
          .queryExecution
          .optimizedPlan)


      //      df.show(100)
      //      checkAnswer(
      //        df,
      //        Seq(
      //          Row(2014, 895974.4400),
      //          Row(1942, 895298.4300),
      //          Row(1992, 911774.9800),
      //          Row(1984, 903623.5400),
      //          Row(1962, 904304.6800)
      //        ))
      null
    }
    visit.executeTask()
  }
}
