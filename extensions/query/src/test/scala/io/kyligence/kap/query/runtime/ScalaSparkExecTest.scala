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

package io.kyligence.kap.query.runtime

import io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder.KylinCast
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expressions, Types}
import org.apache.kylin.common.util.JsonUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, LongType}
import org.apache.spark.sql._

class ScalaSparkExecTest extends SparkQuerySuite {
  val sparkExp = Expressions.parameter(classOf[SparkSession], "spark")
  val argsExp = Expressions.parameter(classOf[CreateTableArgc], "args")
  val sparkruntime = Expressions.parameter(classOf[SparderRuntime], "sparkruntime")
  val aggArcExp = Expressions.parameter(classOf[AggArgc], "agg")
  val selectExp = Expressions.parameter(classOf[SelectArgc], "select")
  val expression = Expressions.call(sparkruntime, SparkMethod.CREATE_TABLE.method, sparkExp, argsExp)
  val createArgs = CreateTableArgc(gtInfoStr, cuboid.toString, cuboid.toString, builSchema(), fileSeq)

  test("test env") {
    val tableName = scanRequest.getInfo.getTableName.replace(" ", "_")
    var table = spark
      .read
      .format("org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
      .option(KYLIN_SCAN_GTINFO_BYTES, gtInfoStr)
      .option(CUBOID_ID, cuboid)
      .schema(builSchema())
      .load(fileSeq: _*)
    val boardcastGTInfo = spark.sparkContext.broadcast(gtInfoStr)
    val cleanMap = cleanRequest(scanRequest, gtInfoStr)
    val intToString = register(cleanMap, spark, tableName)
    val scanRequestStr: String = new String(scanRequest.toByteArray, "ISO-8859-1")
    spark.udf.register("kylin_castb", new KylinCast(gtInfoStr), DecimalType(19, 4))
    spark.udf.register("kylin_casta", new KylinCast(gtInfoStr), LongType)
    val expr1 = expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2").expr

    val df: DataFrame = table
      .groupBy("col_0")
      .agg(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
        expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3"))
      .orderBy("col_0")
      .select(expr("t2"), expr("t3") ,expr("1 as t4"))
      .limit(5)

    //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")

    df.show(100)
    df.schema
    checkAnswer(
      df,
      Seq(
        Row(2014, 895974.4400),
        Row(1942, 895298.4300),
        Row(1992, 911774.9800),
        Row(1984, 903623.5400),
        Row(1962, 904304.6800)
      ))
  }


  test("create tables") {
    val table = Expressions.lambda(expression, sparkruntime, sparkExp, argsExp)
      .compile()
      .dynamicInvoke(new SparderRuntime(), spark, createArgs).asInstanceOf[DataFrame]

    val expr1 = expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2").expr

    val df: DataFrame = table
      .groupBy("col_0")
      .agg(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
        expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3"))
      .orderBy("col_0")
      .select("t2", "t3")
      .limit(5)


    //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")

    df.show(100)
    checkAnswer(
      df,
      Seq(
        Row(2014, 895974.4400),
        Row(1942, 895298.4300),
        Row(1992, 911774.9800),
        Row(1984, 903623.5400),
        Row(1962, 904304.6800)
      ))
  }


  test("aggr") {
    val expression = Expressions.call(sparkruntime, SparkMethod.CREATE_TABLE.method, sparkExp, argsExp)
    val str = Expressions.toString(expression)
    val table = Expressions.lambda(expression, sparkruntime, sparkExp, argsExp)
      .compile()
      .dynamicInvoke(new SparderRuntime(), spark, createArgs).asInstanceOf[DataFrame]

    val argc = AggArgc(table, List(expr("col_0")), List(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
      expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3")))

    val aggred = Expressions.lambda(
      Expressions.call(sparkruntime, SparkMethod.AGG.method, aggArcExp),
      sparkruntime, aggArcExp).compile().dynamicInvoke(new SparderRuntime, argc).asInstanceOf[DataFrame]
    val df = aggred.orderBy("col_0")
      .select("t2", "t3")
      .limit(5)


    //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")

    df.show(100)
    checkAnswer(
      df,
      Seq(
        Row(2014, 895974.4400),
        Row(1942, 895298.4300),
        Row(1992, 911774.9800),
        Row(1984, 903623.5400),
        Row(1962, 904304.6800)
      ))

  }


  test("select ") {
    val str = Expressions.toString(expression)
    val table = Expressions.lambda(expression, sparkruntime, sparkExp, argsExp)
      .compile()
      .dynamicInvoke(new SparderRuntime(), spark, createArgs).asInstanceOf[DataFrame]
    val argc = AggArgc(table,
      List(expr("col_0")),
      List(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
        expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3")))
    val aggred = Expressions.lambda(
      Expressions.call(sparkruntime, SparkMethod.AGG.method, aggArcExp),
      sparkruntime, aggArcExp).compile().dynamicInvoke(new SparderRuntime, argc).asInstanceOf[DataFrame]

    val sekectc = SelectArgc(aggred.orderBy("col_0"), List(expr("t2"), expr("t3")))
    val selected = Expressions.lambda(Expressions.call(sparkruntime, SparkMethod.SELECT.method, selectExp),
      sparkruntime, selectExp).compile().dynamicInvoke(new SparderRuntime, sekectc).asInstanceOf[DataFrame]
    val df = selected.queryExecution.sparkPlan

    println(df)

    //      .limit(5)
    //    //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")
    //
    //    df.show(100)
    //    checkAnswer(
    //      df,
    //      Seq(
    //        Row(2014, 895974.4400),
    //        Row(1942, 895298.4300),
    //        Row(1992, 911774.9800),
    //        Row(1984, 903623.5400),
    //        Row(1962, 904304.6800)
    //      ))

  }
  test("all expo ") {
    val list = new BlockBuilder
    val sparkExp = list.append("spark", Expressions.constant(spark))
    val createExp = Expressions.new_(classOf[CreateTableArgc],
      Expressions.constant(gtInfoStr),
      Expressions.constant(cuboid.toString),
      Expressions.constant(builSchema()),
      Expressions.constant(fileSeq))

    val create = Expressions.call(sparkruntime,
      SparkMethod.CREATE_TABLE.method,
      Expressions.constant(spark), createExp)
    val aggArgc = Expressions.new_(classOf[AggArgc],
      create,
      Expressions.constant(List(expr("col_0"))),
      Expressions.constant(List(expr("kylin_casta(bigintCOUNT(col_4),4)").alias("t2"),
        expr(" kylin_castb(decimal_19_4_SUM(col_6),6)").alias("t3"))))

    val aggedExp = Expressions.call(sparkruntime, SparkMethod.AGG.method, aggArgc)


    val orderExp = Expressions.new_(classOf[OrderArgc], aggedExp, Expressions.constant(List(exp("col_0"))))

    val orderedExp = Expressions.call(sparkruntime, SparkMethod.ORDER.method, orderExp)
    val selectExp = Expressions.new_(classOf[SelectArgc], orderedExp, Expressions.constant(List(expr("t2"), expr("t3"))))
    val selectedExp = Expressions.call(sparkruntime, SparkMethod.SELECT.method, selectExp)

    //    Expressions.lambda(selectedExp,sparkruntime).compile().dynamicInvoke(new SparkRuntime())
    println(selectedExp.toString)

    //      .limit(5)
    //    //      val df = SparkSparderVisit.spark.sql("select col_0, kylin_casta(bigintCOUNT(col_4),4), kylin_castb(decimal_19_4_SUM(col_6),6) from Cuboid_114696 group by col_0 order by col_0 ")
    //
    //    df.show(100)
    //    checkAnswer(
    //      df,
    //      Seq(
    //        Row(2014, 895974.4400),
    //        Row(1942, 895298.4300),
    //        Row(1992, 911774.9800),
    //        Row(1984, 903623.5400),
    //        Row(1962, 904304.6800)
    //      ))

  }


  test("new exp") {
    println(builSchema())
    val exp = Expressions.new_(classOf[CreateTableArgc],
      Expressions.constant(gtInfoStr),
      Expressions.constant(cuboid.toString),
      Expressions.constant(builSchema()),
      Expressions.constant(fileSeq))
    val argc = Expressions.lambda(exp).compile().dynamicInvoke().asInstanceOf[CreateTableArgc]
    println(argc)
  }


  test("scala map to jason") {
    import scala.collection.JavaConverters._
    val map = Map(1 -> 2)
    val java = map.asJava
    println(java.getClass)
    val str = JsonUtil.writeValueAsString(java)
    val stringToString = JsonUtil.readValueAsMap(str)
    println(stringToString)

  }
}
